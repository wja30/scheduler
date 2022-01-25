import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
import json
import time
from os.path import abspath, dirname, join
from base64 import b64encode, b64decode
import random
import requests
import json
import logging
from redis import Redis
import uuid
import math
import redis
from rpq.RpqQueue import RpqQueue
from urllib2 import urlopen
#from urllib.request import urlopen
#from urllib import request
#from urllib.request import Request, urlopen
import numpy as np
import re

logging.basicConfig(filename='logs/metric.log', level=logging.INFO,format='%(asctime)s: %(message)s')

headers = {"content-type": "application/json"}
headers_binary = {"content-type": "application/octet-stream"}
timeout = 60
header_data = ""
reqtype = ["R", "B", "G", "Y", "S"]
nums = []

def redis_connection():
    r = Redis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    # Redis instance
    rq = redis.StrictRedis(host='23.23.220.207', port=6379, db=0, password='redisscheduler')
    # RpqQueue
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue


def metric(r, queue):
    try:
        for req in reqtype:
            get_json = str(r.get(req+"_all_reqs"))
            logging.info(req+"_all_reqs : extract raw data : " + get_json + str(type(get_json)))
            nums = get_json.split(',')
            logging.info("numbers:" + str(nums))
            nums_f = map(float, nums)
            logging.info("numbers_f:" + str(nums_f))
            a = np.array(nums_f, dtype=np.float32)
            p = np.percentile(a, 50, interpolation = 'lower')
            logging.info(req+"50 percentil : " + str(p))
            p_gu = np.percentile(a, 90, interpolation = 'lower')
            logging.info(req+"90 percentil : " + str(p_gu))
            p_guo = np.percentile(a, 95, interpolation = 'lower')
            logging.info(req+"95 percentil : " + str(p_guo))
            p_gugu = np.percentile(a, 99, interpolation ='lower')
            logging.info(req+"99 percentil : " + str(p_gugu))
         
            r.set(req+"_90p_latency",  str(p_gu))
            r.set(req+"_95p_latency",  str(p_guo))
            r.set(req+"_99p_latency",  str(p_gugu))
    except Exception as e:
        logging.info(e)
 
'''
    get_dict = json.loads(get_json)
    try:
        endpoint = get_dict["endpoint"]
        metric_check = get_dict["metric_check"]
        data = get_dict["reqdata"]
        reqtype = get_dict["reqtype"]
        reqtime = get_dict["time"]
        logging.info("dispatch endpoint : " + endpoint)
        logging.info("dispatch data : " + data)
    except Exception as e:
        logging.info(e)

    try :
        req_json = {
                "progress" : 1, # 0 : before dispatch, 1 : after dispatch
                "time" : reqtime,
                "reqtype" : reqtype,
                "reqdata" : data,
                "respdata" : resp, # 0 : before dispatch, value : response data
                "latency" : elapsed, # 0 : before dispatch, value : latency
                "endpoint" : endpoint, # 0 : before dispatch, value : after endpoint decision
                "metric_check" : metric_check, # 0 : before metric check, 1 : after check
                }
    except Exception as e:
        logging.info(e)

    try :
        req_json = json.dumps(req_json)
        logging.info("after dispatch : " + req_json)
        req_uuid = item
        r.set(req_uuid, req_json) # after 60 seconds expire
        #r.expire(req_uuid, 10)
        #logging.info("ttl : " + str(r.ttl(req_uuid)))
    except Exception as e:
        logging.info(e)
'''

def metric_main():
    pool = ThreadPoolExecutor(1000)
    r, queue = redis_connection()
    while True:
        pool.submit(metric, r, queue)
        time.sleep(30)

if __name__ == "__main__":
	metric_main()
