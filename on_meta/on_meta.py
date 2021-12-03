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
    #if get_json:
    #    logging.info("get_meta : " + key + " " + get_json)
    return get_json

def get_trace(r, key):
    get_json = r.get(key)
    if get_json:
        logging.info("get_trace : " + key + " " + get_json)
    return get_json

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
        #get_meta(r, meta_key)
    except Exception as e:
        logging.info(e)
    return meta_key

def on_meta(r, queue):
    # on_meta information update (for window recent 60 seconds)
    inflight = [[0]*5 for j in range(5)]
    req_sec = [[0]*5 for j in range(5)]
    latency = [[0.0]*5 for j in range(5)]
    cnt = [[1]*5 for j in range(5)]

    base_time = time.time()
    for key in r.scan_iter("*-*"):
        get_json = r.get(key)
        get_dict = json.loads(get_json)

        ins_index = instype.index(get_dict["endpoint"][-2:])
        req_index = reqtype.index(get_dict["reqtype"])
        
        # inflight calculation
        if get_dict["progress"] == 0:
            inflight[ins_index][req_index] = inflight[ins_index][req_index]+1
        
        # reqs calculation
        if (base_time - float(get_dict["time"]) < float(60)):
            req_sec[ins_index][req_index] = req_sec[ins_index][req_index]+1
       
        # avg latency calculation
        if get_dict["progress"] == 1:
            latency[ins_index][req_index] += get_dict["latency"]
            cnt[ins_index][req_index] += 1
    for ins in instype:
        for req in reqtype:
            ins_index = instype.index(ins)
            req_index = reqtype.index(req)
            set_meta(r, ins, req, inflight[ins_index][req_index], latency[ins_index][req_index]/(cnt[ins_index][req_index]), req_sec[ins_index][req_index]) 

def trace(r):
    for ins in instype:
        for req in reqtype:
            key = ins + req + "_on"
            get_json = get_meta(r, key)
            get_dict = json.loads(get_json)

            current_time = time.time()
            now = time.gmtime(current_time)
            reqs = get_dict["req_sec"]

            trace_json = {
                    str(now.tm_year)+"-"+str(now.tm_mon)+"-"+str(now.tm_mday)+"/"+str(now.tm_hour)+":"+str(now.tm_min)+":"+str(now.tm_sec) : reqs
                    }

            trace_key = ins + req + "_trace"
            get_json = get_trace(r, trace_key)
            if get_json:
                get_dict = json.loads(get_json)
                get_dict.update(trace_json)
                r.set(trace_key, json.dumps(get_dict))
            else:
                trace_json = json.dumps(trace_json)
                r.set(trace_key, trace_json)
    return "trace"

def on_meta_report(r):
    for ins in instype:
        for req in reqtype:
            #logging.info(ins + req)
            key = ins + req + "_on"
    #        logging.info(key)
            get_json = get_meta(r, key)
            if get_json:
                logging.info("get_meta : " + key + " " + get_json)
 
    return "on_meta_report"

def on_meta_main():
    #pool = ThreadPoolExecutor(1)
    r, queue = redis_connection()
    #scan_value = r.keys("*c5*")
    #logging.info(scan_value)
    cnt = 0
    if r.keys("*-*"):
        logging.info("scan_iter return value is true")
    else :
        logging.info("scan_iter return value is FALSE")
    while True:
        time.sleep(10) # every 10 seconds, meta data is updated (# of reqs in 60secons / avglatency / inflight request)
        on_meta(r, queue)
        on_meta_report(r)
        cnt+=10
        if(cnt == 60): # every 60 seconds, # of reqs in 60 seconds is written to trace file (redis)
            trace(r)
            cnt=0 
if __name__ == "__main__":
	on_meta_main()
