import json
import logging
import time
import os

from datetime import datetime, timedelta
from prometheus_client import Summary
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from aliyunsdkcore.client import AcsClient
from aliyun_exporter.desc import Desc
# from aliyunsdkcms.request.v20190101 import QueryMetricLastRequest
from aliyunsdkcms.request.v20190101 import DescribeMetricLastRequest
from aliyunsdkrds.request.v20140815 import DescribeDBInstancePerformanceRequest
from ratelimiter import RateLimiter

from aliyun_exporter.info_provider import InfoProvider
from aliyun_exporter.utils import try_or_else

rds_performance = 'rds_performance'
special_projects = {
    rds_performance: lambda collector: RDSPerformanceCollector(collector),
}

requestSummary = Summary('cloudmonitor_request_latency_seconds', 'CloudMonitor request latency', ['project'])
requestFailedSummary = Summary('cloudmonitor_failed_request_latency_seconds', 'CloudMonitor failed request latency',
                               ['project'])


class CollectorConfig(object):
    def __init__(self,
                 pool_size=10,
                 rate_limit=10,
                 credential=None,
                 metrics=None,
                 info_metrics=None,
                 do_info_region=None,
                 ):
        # if metrics is None:
        # raise Exception('Metrics config must be set.')

        self.credential = credential
        self.metrics = metrics
        self.rate_limit = rate_limit
        self.info_metrics = info_metrics
        self.do_info_region = do_info_region

        # ENV
        access_id = os.environ.get('ALIYUN_ACCESS_ID')
        access_secret = os.environ.get('ALIYUN_ACCESS_SECRET')
        region = os.environ.get('ALIYUN_REGION')

        if self.credential is None:
            self.credential = {}
        if access_id is not None and len(access_id) > 0:
            self.credential['access_key_id'] = access_id
        if access_secret is not None and len(access_secret) > 0:
            self.credential['access_key_secret'] = access_secret
        if region is not None and len(region) > 0:
            self.credential['region_id'] = region
        if self.credential['access_key_id'] is None or \
                self.credential['access_key_secret'] is None:
            raise Exception('Credential is not fully configured.')


class AliyunCollector(object):
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.metrics = config.metrics
        self.info_metrics = config.info_metrics
        self.client = AcsClient(
            ak=config.credential['access_key_id'],
            secret=config.credential['access_key_secret'],
       
            max_retry_time=2
            # region_id=config.credential['region_id'] #在获取监控指标metrics时貌似不需要region
        )
        self.rateLimiter = RateLimiter(max_calls=config.rate_limit)
        self.info_provider = InfoProvider(ak=config.credential['access_key_id'],
                                          secret=config.credential['access_key_secret'],
                                          region_id=config.credential['region_id'])

        self.desc = Desc(config.credential['access_key_id'], 
                         config.credential['access_key_secret'],
                         config.credential['region_id'])

        self.special_collectors = dict()
        for k, v in special_projects.items():
            if k in self.metrics:
                self.special_collectors[k] = v(self)

# Query metrics data 
    def query_metric(self, project: str, metric: str, period: int):
        with self.rateLimiter:
            # req = QueryMetricLastRequest.QueryMetricLastRequest()
            # req.set_Project(project)
            # req.set_Metric(metric)
            # req.set_Period(period)
            req = DescribeMetricLastRequest.DescribeMetricLastRequest()
            req.set_Namespace(project)
            req.set_MetricName(metric)
            req.set_Period(period)

            start_time = time.time()

            resp_result = True
            resp_count = 1
            resp = {}
            while resp_result:
                if resp_count > 20:
                    logging.error("进行了{}次请求，终止".format(resp_count))
                    requestFailedSummary.labels(project).observe(time.time() - start_time)
                    return []
                try:
                    if resp_count > 1:
                        logging.error("上次请求失败，正在进行第{}次请求".format(resp_count))
                        time.sleep(5)
                    resp = self.client.do_action_with_exception(req)
                except Exception as e:
                    logging.error('Error request cloud monitor api', exc_info=e)
                    resp_count += 1
                else:
                    resp_result = False
                    requestSummary.labels(project).observe(time.time() - start_time)


            # try:
            #     logging.info("开始请求{project} {metric}".format(project=project, metric=metric))
            #     resp = self.client.do_action_with_exception(req)
            # except Exception as e:
            #     try:
            #         logging.error('Error request cloud monitor api', exc_info=e)
            #         resp = self.client.do_action_with_exception(req)
            #     except Exception as e:
            #         logging.error('Error request cloud monitor api', exc_info=e)
            #         requestFailedSummary.labels(project).observe(time.time() - start_time)
            #         return []
            # else:
            #     requestSummary.labels(project).observe(time.time() - start_time)
        data = json.loads(resp)
        # print(data)
        if 'Datapoints' in data:
            points = json.loads(data['Datapoints'])
            return points
        else:
            logging.error(
                'Error query metrics for {}_{}, the response body don not have Datapoints field, please check you permission or workload'.format(
                    project, metric))
            return None

    def parse_label_keys(self, point):
        return [k for k in point if k not in ['timestamp', 'Maximum', 'Minimum', 'Average']]

    def format_metric_name(self, project, name):
        return 'aliyun_{}_{}'.format(project, name)

    def metric_generator(self, project, metric):

        if 'name' not in metric:
            raise Exception('name must be set in metric item.')
        name = metric['name']
        metric_name = metric['name']
        period = 60
        measure = 'Average'
        if 'rename' in metric:
            name = metric['rename']
        if 'period' in metric:
            period = metric['period']
        if 'measure' in metric:
            measure = metric['measure']

        try:
            points = self.query_metric(project, metric_name, period)
        except Exception as e:
            logging.error('Error query metrics for {}_{}'.format(project, metric_name), exc_info=e)
            yield metric_up_gauge(self.format_metric_name(project, name), False)
            return
        if points is None:
            yield metric_up_gauge(self.format_metric_name(project, name), False)
            return
        if len(points) < 1:
            yield metric_up_gauge(self.format_metric_name(project, name), False)
            return


            '''
            points[
            {
                'timestamp': 1637056440000,
                'userId': '1907690484245347',
                'instanceId': 'r-bp10x4kim0yrb1j5vw',
                'nodeId': 'r-bp10x4kim0yrb1j5vw-proxy-3',
                'Maximum': 0.0,
                'Average': 0.0
            }
            ]
            '''
        label_keys = self.parse_label_keys(points[0])
        # add label key : name
        if(project == 'acs_kvstore'):
            label_keys.append('name')
            for p in points:
                p['name'] = self.desc.get_desc(p['instanceId'])

        gauge = GaugeMetricFamily(self.format_metric_name(project, name), '', labels=label_keys)
        for point in points:
            try:
               gauge.add_metric([try_or_else(lambda: str(point[k]), '') for k in label_keys], point[measure])
            except:
               logging.error("error happened when measure:{}, label_keys:{}, point: {}".format(measure, label_keys, point))

        yield gauge
        yield metric_up_gauge(self.format_metric_name(project, name), True)

# main
    def collect(self):
        for project in self.metrics:
            if project in special_projects:
                continue
            for metric in self.metrics[project]:
                yield from self.metric_generator(project, metric)

        ####### info as metrics  ####### 
        if self.info_metrics != None:
            for resource in self.info_metrics:
                if self.config.do_info_region == None:
                    client = AcsClient(
                        ak=self.config.credential['access_key_id'],
                        secret=self.config.credential['access_key_secret'],
                        region_id=self.config.credential['region_id'],
                        timeout=10,
                        connect_timeout=10,
                        max_retry_time=2,
                    )
                    t_metrice = self.info_provider.get_metrics(resource, client)
                    if t_metrice == None:
                        continue
                    else:
                        yield t_metrice
                else:
                    for a_region in self.config.do_info_region:
                        client = AcsClient(
                            ak=self.config.credential['access_key_id'],
                            secret=self.config.credential['access_key_secret'],
                            region_id=a_region,
                            max_retry_time=2,
                        )
                        t_metrice = self.info_provider.get_metrics(resource, client)
                        if t_metrice == None:
                            continue
                        else:
                            yield t_metrice
        ####### info as metrics  ####### 

        for v in self.special_collectors.values():
            yield from v.collect()


def metric_up_gauge(resource: str, succeeded=True):
    metric_name = resource + '_up'
    description = 'Did the {} fetch succeed.'.format(resource)
    return GaugeMetricFamily(metric_name, description, value=int(succeeded))


class RDSPerformanceCollector:

    def __init__(self, delegate: AliyunCollector):
        self.parent = delegate

    def collect(self):
        if self.parent.config.do_info_region == None:
            client = AcsClient(
                ak=self.parent.config.credential['access_key_id'],
                secret=self.parent.config.credential['access_key_secret'],
                region_id=self.parent.config.credential['region_id'],
                timeout=10,
                connect_timeout=10,
                max_retry_time=2,
            )
            for id in [s.labels['DBInstanceId'] for s in self.parent.info_provider.get_metrics('rds', client).samples]:
                metrics = self.query_rds_performance_metrics(id)
                for metric in metrics:
                    yield from self.parse_rds_performance(id, metric)

        else:
            for a_region in self.parent.config.do_info_region:
                client = AcsClient(
                    ak=self.parent.config.credential['access_key_id'],
                    secret=self.parent.config.credential['access_key_secret'],
                    region_id=a_region,
                    timeout=10,
                    connect_timeout=10,
                    max_retry_time=2,
                )
                for id in [s.labels['DBInstanceId'] for s in
                           self.parent.info_provider.get_metrics('rds', client).samples]:
                    metrics = self.query_rds_performance_metrics(id)
                    for metric in metrics:
                        yield from self.parse_rds_performance(id, metric)

    def parse_rds_performance(self, id, value):
        value_format: str = value['ValueFormat']
        metric_name = value['Key']
        keys = ['value']
        if value_format is not None and '&' in value_format:
            keys = value_format.split('&')
        metric = value['Values']['PerformanceValue']
        if len(metric) < 1:
            return
        values = metric[0]['Value'].split('&')
        for k, v in zip(keys, values):
            gauge = GaugeMetricFamily(
                self.parent.format_metric_name(rds_performance, metric_name + '_' + k),
                '', labels=['instanceId'])
            gauge.add_metric([id], float(v))
            yield gauge

    def query_rds_performance_metrics(self, id):
        req = DescribeDBInstancePerformanceRequest.DescribeDBInstancePerformanceRequest()
        req.set_DBInstanceId(id)
        req.set_Key(','.join([metric['name'] for metric in self.parent.metrics[rds_performance]]))
        now = datetime.utcnow()
        now_str = now.replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%MZ")
        one_minute_ago_str = (now - timedelta(minutes=1)).replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%MZ")
        req.set_StartTime(one_minute_ago_str)
        req.set_EndTime(now_str)
        try:
            resp = self.parent.client.do_action_with_exception(req)
        except Exception as e:
            logging.error('Error request rds performance api', exc_info=e)
            return []
        data = json.loads(resp)
        return data['PerformanceKeys']['PerformanceKey']
