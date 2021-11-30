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

logging.basicConfig(filename='logs/on_meta.log', level=logging.INFO,format='%(asctime)s: %(message)s')

headers = {"content-type": "application/json"}
headers_binary = {"content-type": "application/octet-stream"}
timeout = 60
windown = 60 # 60 sec
instype = ["i1", "p2", "p3", "c5"]
reqtype = ["R", "B", "G", "Y", "S"]

def redis_connection():
    r = redis.StrictRedis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    # Redis instance
    rq = redis.StrictRedis(host='23.23.220.207', port=6379, db=0, password='redisscheduler')
    # RpqQueue
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue

def get_meta(r, key):
    get_json = r.get(key)
    logging.info("get_meta : " + key + " " + get_json)


def set_meta(r, instype, reqtype, inflight, avglatency, req_sec):
    try:
        meta_json = {
                "inflight" : inflight,
                "avglatency" : avglatency,
                "req_sec" : req_sec,
                }
        meta_json = json.dumps(meta_json)
        meta_key = instype + reqtype + "_on" # instype(i1) + reqtype(R) = i1R_on
        r.set(meta_key, meta_json)
        get_meta(r, meta_key)
    except Exception as e:
        logging,info(e)

def on_meta(r, queue):
    # on_meta information update (for window recent 60 seconds)
    inflight = [[0]*5 for j in range(5)]
    for key in r.scan_iter("*-*"):
        #logging.info("keys : " + key)
        get_json = r.get(key)
        #logging.info("get uuid : " + get_json)
        get_dict = json.loads(get_json)

        ins_index = instype.index(get_dict["endpoint"][-2:])
        req_index = reqtype.index(get_dict["reqtype"])

        # to do : each i1R, i1B, i1G summation is needed
        inflight[ins_index][req_index] = inflight[ins_index][req_index]+1
        req_sec = 0
        if get_dict["progress"] == 1:
            set_meta(r, get_dict["endpoint"][-2:], get_dict["reqtype"], inflight[ins_index][req_index], get_dict["latency"], req_sec) 


    # all instype + reqtype data initiation
#    for ins in instype:
 #       for req in reqtype:


def on_meta_main():
    pool = ThreadPoolExecutor(1)
    r, queue = redis_connection()
    #scan_value = r.keys("*c5*")
    #logging.info(scan_value)
    if r.keys("*-*"):
        logging.info("scan_iter return value is true")
    else :
        logging.info("scan_iter return value is FALSE")
    while True:
        #time.sleep(30)
        if r.keys("*-*"):
            pool.submit(on_meta, r, queue)
            time.sleep(0.1)
            
if __name__ == "__main__":
	on_meta_main()
