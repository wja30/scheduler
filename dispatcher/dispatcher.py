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

logging.basicConfig(filename='logs/dispatch.log', level=logging.WARNING,format='%(asctime)s: %(message)s')

url = "https://wja300-cortex.s3.amazonaws.com/sound-classifier/mia.wav"
headers = {"content-type": "application/json"}
headers_binary = {"content-type": "application/octet-stream"}
timeout = 60
header_data = ""

def redis_connection():
    r = Redis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    # Redis instance
    rq = redis.StrictRedis(host='23.23.220.207', port=6379, db=0, password='redisscheduler')
    # RpqQueue
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue

def dispatch(r, queue):
    # dispatcher code
    # pop request priority queue
    logging.info('* pop:')
    item = queue.popOne()
    if item:
        logging.info("popped uuid item :" + item)
    else:
        logging.info('Queue is empty')
    #logging.info("Key {} was set at {} and has {} seconds until expired".format(keyName, keyValue, keyTTL))
    get_json = r.get(item)
    logging.info("before dispatch : " + get_json)
    # delete req_uuid
    #r.delete(item)
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

    try:
        if reqtype == "Y":
            header_data = headers_binary
            data = urlopen(url).read() 
        else:
            header_data = headers
    except Exception as e:
        logging.info(e)
    
    start = time.time()
    try:
        resp = requests.post(
                endpoint,
                data = data,
                headers = header_data,
                timeout = timeout,
            )
    except Exception as e:
        print(e)
    end = time.time()
    elapsed = end - start
    logging.info(endpoint + " latency: " + str(round(elapsed, 4)) + "seconds")
   
    r.append(reqtype+"_all_reqs", ","+str(round(elapsed, 3))) #for all latency dumps -> 90,95,99 percentile
     
    logging.warning("resp : " + str(resp))

    if str(resp) == "<Response [503]>": # 5xx error latency = 60 sec set (time out)
 #       logging.warning("wja30 503")
        elapsed = 60
    try : 

        if reqtype == "R":
            if str(resp) == "<Response [200]>":
                resp = json.dumps(resp.json())
            else:
                resp = "503"
        elif reqtype == "B":
            resp = resp.text
        elif reqtype == "G":
            resp = resp.text
        elif reqtype == "Y":
            resp = resp.text
            data = ""
        elif reqtype == "S":
            resp = resp.text
    except Exception as e:
        logging.warning(e)

    # todo "G","Y","S"
    
    # request value
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
        logging.warning(e)

    try :
        req_json = json.dumps(req_json)
        logging.info("after dispatch : " + req_json)
        req_uuid = item
        r.set(req_uuid, req_json) # after 60 seconds expire
        #r.expire(req_uuid, 10)
        #logging.info("ttl : " + str(r.ttl(req_uuid)))
    except Exception as e:
        logging.warning(e)

def dispatch_main():
    pool = ThreadPoolExecutor(100000)
    r, queue = redis_connection()
    while True:
        if queue.count() > 0:
            pool.submit(dispatch, r, queue)
            time.sleep(1/1000.0)

if __name__ == "__main__":
	dispatch_main()
