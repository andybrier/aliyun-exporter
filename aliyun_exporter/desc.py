# redis
from alibabacloud_r_kvstore20150101.client import Client as R_kvstore20150101Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_r_kvstore20150101 import models as r_kvstore_20150101_models

from expiringdict import ExpiringDict
import logging

class Desc():
    def __init__(self, ak, secret, region_id):
        self.client = None
        self.ak = ak
        self.secret = secret
        self.region_id = region_id
        self.cache = ExpiringDict(max_len=10000, max_age_seconds=10)
        
    
    def get_desc(self, id):
        if id in self.cache:
            return self.cache[id]
        else:
              logging.info("start to query id info at {}".format(id))
              # redis
              config = open_api_models.Config(self.ak, self.secret)        
              config.endpoint = 'r-kvstore.aliyuncs.com'
              client = R_kvstore20150101Client(config)
              describe_instances_request = r_kvstore_20150101_models.DescribeInstancesRequest()
              rsp = client.describe_instances(describe_instances_request).to_map()
              if rsp['body'] is not None:
                if rsp['body']['Instances'] is not None:
                    if rsp['body']['Instances']['KVStoreInstance'] is not None:
                        for kv in rsp['body']['Instances']['KVStoreInstance']:
                          self.cache[kv['InstanceId']] = kv['InstanceName']
            

              return self.cache[id]
            
                


            


