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
import threading
import os
import datetime as dt
from datetime import timedelta as td


logging.basicConfig(filename='logs/on_meta.log', level=logging.WARNING, format='%(message)s')


isStart = 0
start = dt.datetime.now()
headers = {"content-type": "application/json"}
headers_binary = {"content-type": "application/octet-stream"}
timeout = 60
windown = 60 # 60 sec
instype = ["i1", "p2", "p3", "c5"]
reqtype = ["R", "B", "G", "Y", "S"]
total_reqs = 0
lock = threading.Lock()

gappluscount = [0, 0, 0, 0] # i1, p2, p3, c5
gapminuscount = [0, 0, 0, 0]

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
    cnt = [[0]*5 for j in range(5)] # 0 -> 1 for prevent divided by zero : latency / cnt

    
    slo_violate_cnt = [[0]*5 for j in range(5)]
    slo2_violate_cnt = [[0]*5 for j in range(5)]
    slo5_violate_cnt = [[0]*5 for j in range(5)]
    slo8_violate_cnt = [[0]*5 for j in range(5)]

    first_check = [[0]*5 for j in range(5)] 

    base_time = time.time()

    
    global isStart
    global start

    if isStart == 0:
        start = dt.datetime.now()
        isStart = 1

    loopout_cnt = 0
    #try:
    #    lock.acquire()
    #except Exception as e:
    #    logging.warning(e)

    for key in r.scan_iter("*-*", count=10000):
        #loopout_cnt += 1
        #if loopout_cnt > 100: #for on_meta sync performance
        #    break

        get_json = r.get(key)
        if get_json is None: # if expired item is occured
            break
        get_dict = json.loads(get_json)

        #ins_index = instype.index(get_dict["endpoint"][-2:])
        if get_dict["reqtype"] == "R": # for :     "i1Rtail" : "image-classifier-resnet50-v2-i1/v1/models/resnet50:predict", case
            ins_index = instype.index(get_dict["endpoint"][-29:-27])
        if get_dict["reqtype"] == "B": # for :     "i1Btail" : "sentiment-analyzer-v2-i1/v1/models/bert:predict", case
            ins_index = instype.index(get_dict["endpoint"][-25:-23])
        if get_dict["reqtype"] == "G": # for :     "i1Gtail" : "text-generator-v2-i1", case
            ins_index = instype.index(get_dict["endpoint"][-2:])
        if get_dict["reqtype"] == "Y": # for :     "i1Ytail" : "sound-classifier-v2-i1/v1/models/yamnet:predict", case
            ins_index = instype.index(get_dict["endpoint"][-27:-25])
        if get_dict["reqtype"] == "S": # for :     "i1Stail" : "image-classifier-inception-v2-i1/v1/models/inception:predict", case
            ins_index = instype.index(get_dict["endpoint"][-30:-28])
 
        req_index = reqtype.index(get_dict["reqtype"])
        
        # inflight calculation
        if get_dict["progress"] == 0:
            inflight[ins_index][req_index] = inflight[ins_index][req_index]+1
        
        # reqs calculation
        if (base_time - float(get_dict["time"]) < float(60)):
            req_sec[ins_index][req_index] = req_sec[ins_index][req_index]+1
       
        # avg latency calculation
        if (get_dict["progress"] == 1) and (get_dict["metric_check"] == 0): # check metric if not metric checked and progress is 1
            latency[ins_index][req_index] += get_dict["latency"]
            slo_key = get_dict["reqtype"] + "_SLO_ms"

            slo = float(r.get(slo_key))/1000
            slo2 = slo*0.2
            slo5 = slo*0.5
            slo8 = slo*0.8
            
            if get_dict["latency"] > slo : # if slo violation
                slo_violate_cnt[ins_index][req_index] += 1
            if get_dict["latency"] > slo2 : # if slo*0.2 violation
                slo2_violate_cnt[ins_index][req_index] += 1
            if get_dict["latency"] > slo5 : # if slo*0.5 violation
                slo5_violate_cnt[ins_index][req_index] += 1
            if get_dict["latency"] > slo8 : # if slo*0.8 violation
                slo8_violate_cnt[ins_index][req_index] += 1




            cnt[ins_index][req_index] += 1 # e.g. R_total_reqs : means measure the request count except timeout (60seconds) -> because progress value can not be "1"

            progress = get_dict["progress"]
            reqtime = get_dict["time"]
            reqreqtype = get_dict["reqtype"]
            reqdata = get_dict["reqdata"]
            respdata = get_dict["respdata"]
            avglatency = get_dict["latency"]
            smpl_latency = get_dict["smpl_latency"]
            endpoint = get_dict["endpoint"]
            metric_check = 1

            # request value
            try :
                req_json = {
                    "progress" : progress, # 0 : before dispatch, 1 : after dispatch
                    "time" : reqtime,
                    "reqtype" : reqreqtype,
                    "reqdata" : reqdata,
                    "respdata" : respdata, # 0 : before dispatch, value : response data
                    "latency" : avglatency, # 0 : before dispatch, value : latency
                    "smpl_latency" : smpl_latency, 
                    "endpoint" : endpoint, # 0 : before dispatch, value : after endpoint decision
                    "metric_check" : metric_check, # 0 : before metric check, 1 : after check
                    }
            except Exception as e:
                logging.info(e)

            now = dt.datetime.now()
            try :
                req_json = json.dumps(req_json)
                #logging.info("after dispatch : " + req_json)
                req_uuid = key
                #r.set(req_uuid, req_json, 1) # after 60 seconds expire
                # before erasing logging the smpl - mrlg latency!!

                if reqreqtype == "R": # for :     "i1Rtail" : "image-classifier-resnet50-v2-i1/v1/models/resnet50:predict", case
                    ins = endpoint[-29:-27]
                if reqreqtype == "B": # for :     "i1Rtail" : "image-classifier-resnet50-v2-i1/v1/models/resnet50:predict", case
                    ins = endpoint[-25:-23]
                if reqreqtype == "G": # for :     "i1Rtail" : "image-classifier-resnet50-v2-i1/v1/models/resnet50:predict", case
                    ins = endpoint[-2:]
                if reqreqtype == "Y": # for :     "i1Rtail" : "image-classifier-resnet50-v2-i1/v1/models/resnet50:predict", case
                    ins = endpoint[-27:-25]
                if reqreqtype == "S": # for :     "i1Rtail" : "image-classifier-resnet50-v2-i1/v1/models/resnet50:predict", case
                    ins = endpoint[-30:-28]

                ins_index = instype.index(ins)
                if ((smpl_latency/1000 - avglatency) > 0):
                    gappluscount[ins_index] = gappluscount[ins_index] + 1
                if ((smpl_latency/1000 - avglatency) < 0):
                    gapminuscount[ins_index] = gapminuscount[ins_index] + 1

                logging.warning(str(os.getpid()) + " " + str(now-start) + str(" ins ") + str(ins) + str(" avglatency ") + str(avglatency) + str(" smpl_latency ") + str(smpl_latency/1000) + str(" (smpl-mrlg)/smpl*100 ") + str((smpl_latency/1000 - avglatency)/smpl_latency*100) + " pluscount/minuscount i1 " + str(gappluscount[0]) + "/" + str(gapminuscount[0]) + " p2 " + str(gappluscount[1]) + "/" + str(gapminuscount[1]) + " p3 " + str(gappluscount[2]) + "/" + str(gapminuscount[2]) + " c5 " + str(gappluscount[3]) + "/" + str(gapminuscount[3]))

                r.delete(req_uuid) # erase reqs if metric_check => 1
            except Exception as e:
                logging.info(e)


    for ins in instype:
        for req in reqtype:
            ins_index = instype.index(ins)
            req_index = reqtype.index(req)
            if cnt[ins_index][req_index] > 0:
                set_meta(r, ins, req, inflight[ins_index][req_index], latency[ins_index][req_index]/(cnt[ins_index][req_index]), req_sec[ins_index][req_index]) 
            #if(window == 59): # every 60 seconds : 0 -> 10 -> 20 -> 30 -> 40 -> 50
                on_meta_summation(r, ins, req, cnt[ins_index][req_index], latency[ins_index][req_index], slo_violate_cnt[ins_index][req_index], slo2_violate_cnt[ins_index][req_index], slo5_violate_cnt[ins_index][req_index], slo8_violate_cnt[ins_index][req_index])
            elif cnt[ins_index][req_index] == 0:
                set_meta(r, ins, req, inflight[ins_index][req_index], latency[ins_index][req_index], req_sec[ins_index][req_index]) 

    #try : 
    #    lock.release()
    #except Exception as e:
    #    logging.warning(e)

# (60sec window) summation : total avglatency, total slo violation rate
def on_meta_summation(r, ins, req, reqs, latencies, slo_violate_cnt, slo2_violate_cnt, slo5_violate_cnt, slo8_violate_cnt):
    
    logging.info("summation starts")
    req_key = req + "_total_reqs"
    latency_key = req + "_avg_latency"

    slo_key = req + "_slo_vio_rate"
    slo2_key = req + "_slo2_vio_rate"
    slo5_key = req + "_slo5_vio_rate"
    slo8_key = req + "_slo8_vio_rate"

    
    vio_cnt_key = req + "_slo_vio_cnt"
    ins_vio_cnt_key = ins + req + "_slo_vio_cnt"

    now_reqs = int(r.get(req_key))
    now_latencies = float(r.get(latency_key)) * float(now_reqs)
 
    now_slo_vio_rate = float(r.get(slo_key)) * float(now_reqs)
    now_slo2_vio_rate = float(r.get(slo2_key)) * float(now_reqs)
    now_slo5_vio_rate = float(r.get(slo5_key)) * float(now_reqs)
    now_slo8_vio_rate = float(r.get(slo8_key)) * float(now_reqs)


    now_vio_cnt = int(r.get(vio_cnt_key))
    now_ins_vio_cnt = int(r.get(ins_vio_cnt_key))

    now_reqs += int(reqs)
    now_latencies += float(latencies)

    now_slo_vio_rate += float(slo_violate_cnt)
    now_slo2_vio_rate += float(slo2_violate_cnt)
    now_slo5_vio_rate += float(slo5_violate_cnt)
    now_slo8_vio_rate += float(slo8_violate_cnt)
 
    
    
    now_vio_cnt += int(slo_violate_cnt) 
    now_ins_vio_cnt += int(slo_violate_cnt)

    if(now_reqs != 0):
        r.set(req_key, now_reqs)
        r.set(latency_key, now_latencies / float(now_reqs))
        
        
        r.set(slo_key, now_slo_vio_rate / float(now_reqs))
        r.set(slo2_key, now_slo2_vio_rate / float(now_reqs))
        r.set(slo5_key, now_slo5_vio_rate / float(now_reqs))
        r.set(slo8_key, now_slo8_vio_rate / float(now_reqs))
 
        
        r.set(vio_cnt_key, now_vio_cnt)
        r.set(ins_vio_cnt_key, now_ins_vio_cnt)

    return "on_meta_summation"


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

# for 10 seconds, inflight, avglatency, # of reqs in 60 seconds
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
    loopcnt = 0.0
    if r.keys("*-*"):
        logging.info("scan_iter return value is true")
    else :
        logging.info("scan_iter return value is FALSE")
    while True:
        #time.sleep(0.01) # every 1 seconds, meta data is updated (# of reqs in 60secons / avglatency / inflight request)
        on_meta(r, queue) # collect metrics (inflight, avg_latency, res)
        loopcnt += (0.1)
        logging.info("loopcnt : "+str(loopcnt))
        if(loopcnt > 0.5): # every 60 seconds, # of reqs in 60 seconds is written to trace file (redis)
            on_meta_report(r)
            #trace(r)
            loopcnt= 0.0 
if __name__ == "__main__":
	on_meta_main()
