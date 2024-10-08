from flask import Flask, render_template, request, current_app
import boto3
#from redis import Redis
import json
import requests
import time
import datetime
from rpq.RpqQueue import RpqQueue
import redis
import math
import uuid
from redis import Redis
import random
import os
import datetime as dt
from datetime import timedelta as td

#########################################
app = Flask(__name__)

if not app.debug:
    # 즉 debug=true면 이는 false로서 아래 함수가 돌아간다.
    # 실제 상용화단계에서 로깅을 진행해라는 의미이다
    import logging
    from logging.handlers import RotatingFileHandler
    # logging 핸들러에서 사용할 핸들러를 불러온다.
    file_handler = RotatingFileHandler(
        'logs/scheduler_server.log', maxBytes=2000, backupCount=10)
    file_handler.setLevel(logging.WARNING)
    # 어느 단계까지 로깅을 할지를 적어줌
    # app.logger.addHandler() 에 등록시켜줘야 app.logger 로 사용 가능
    app.logger.addHandler(file_handler)
    #logging.basicConfig(filename='logs/info.log', level=logging.WARNING,format='%(asctime)s: %(message)s')
    logging.basicConfig(filename='logs/info.log', level=logging.WARNING,format='%(message)s')

data = json.dumps({'url': 'https://i.imgur.com/213xcvs.jpg'})
headers = {"content-type": "application/json"}# for R, B, G, S
headers_binary = {"content-type": "application/octet-stream"}# for Y
timeout = 60
instype = ["i1", "p2", "p3", "c5"]
reqtype = ["R", "B", "G", "Y", "S"]
inscount = [0, 0, 0, 0]
isStart = 0
start = dt.datetime.now()
gappluscount = [0, 0, 0, 0] # i1, p2, p3, c5
gapminuscount = [0, 0, 0, 0]
isAuto = 0
autoStart = 0
dynamicStart = 0
AutoW = 0.3 # autoscaling aware weight
WindowMax = 0.01 # base max value : 0.01 no action window max (prevent oscilliation)
WindowMin = 0.001 # base min value : 0.0001 no action window min (prevent oscillation)


##########################################



def redis_connection():

    #redis pool patch code
    redis_pool = redis.ConnectionPool(host='23.23.220.207', port=6379, db=0, max_connections=100 , decode_responses=True, password='redisscheduler')# pool로 connection을 취득
    r =  Redis(connection_pool=redis_pool)
    if r.ping():
        logging.info("Connected to Redis")
    rq = redis.StrictRedis(connection_pool=redis_pool)
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue
    ''' 
    r = Redis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    # Redis instance
    rq = redis.StrictRedis(host='23.23.220.207', port=6379, db=0, password='redisscheduler')
    # RpqQueue
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue
    '''
##########################################

@app.route('/')
def hello_world():
	return 'Hello aWorld!'

@app.route("/off/endpoint",methods=['GET', 'POST'])
def offend_post():
    if(request.method == 'GET'):
        return "GET"
    elif(request.method == 'POST'):
        # cluster endpoint insert
        r, queue = redis_connection()
        #endpoints = json.dumps(request.get_json())
        #logging.info(endpoints)
        endpoints = request.get_json()
        for key, value in endpoints.items():
            r.set(key, value)
        return "off_endpoint"
 
@app.route("/off/meta",methods=['GET', 'POST'])
def offmeta_post():
    if(request.method == 'GET'):
        return "GET"
    elif(request.method == 'POST'):
        # insert cluster offline information
        r, queue = redis_connection()
        #endpoints = json.dumps(request.get_json())
        #logging.info(endpoints)
        metas = request.get_json()
        for key, value in metas.items():
            r.set(key, value)
        return "off_meta"


@app.route("/check",methods=['POST'])
def check_get():
    if(request.method == 'POST'):
        data = request.get_json()
        #logging.info("check_data : " + data["request_uuid"])
        r, queue = redis_connection()
        
        check_result = r.get(data["request_uuid"])

        #logging.info("check_result : " + str(r.get(data["request_uuid"])))
        
        if (json.loads(check_result))["respdata"]:
            #r.delete(data["request_uuid"])
            return (json.loads(check_result))["respdata"]
        else:
            return "0"

################################################################
'''
# RR endpoint_policy
def endpoint_policy(r, reqtype, auto="off"): # default auto off, if on : new policy is calculated

    ins_index = random.randrange(0, 4)
    
    # make endpoint
    endpoint = "http://"+r.get(instype[ins_index]+"api")+"/"+r.get(instype[ins_index]+reqtype+"tail")
    logging.info("endpoint :"+endpoint)
    return endpoint
'''
'''
# BGS(Best Greedy Selection) endpoint_policy
def endpoint_policy(r, reqtype, auto="off"): # default auto off, if on : new policy is calculated

    ins_index = 3 # 0:i1, 1:p2, 2:p3, 3:c5 
    
    # make endpoint
    endpoint = "http://"+r.get(instype[ins_index]+"api")+"/"+r.get(instype[ins_index]+reqtype+"tail")
    logging.info("endpoint :"+endpoint)
    return endpoint, 0.5 # 0.5 is dummy value
'''
'''
# Weight endpoint_policy
def endpoint_policy(r, reqtype, auto="off"): # default auto off, if on : new policy is calculated

    if reqtype == "R":
        reqratio = [5, 26, 60, 9] #R
    elif reqtype == "B":
        reqratio = [16, 24, 43, 17] #B
    elif reqtype == "G":
        reqratio = [8, 31, 53, 8] #G
    elif reqtype == "S":
        reqratio = [1, 18, 78, 3] #S
    elif reqtype == "Y":
        reqratio = [22, 20, 34, 24] #Y

    x = random.randrange(0, 100)
    logging.info("beforex : " + str(x))
    if(x >=0 and x < reqratio[0]):
        x = 0
    if(x >= reqratio[0] and x < reqratio[0] + reqratio[1]):
        x = 1
    if(x >= reqratio[0] + reqratio[1] and x < reqratio[0] + reqratio[1] + reqratio[2]):
        x = 2
    if(x >= reqratio[0] + reqratio[1] + reqratio[2] and x < reqratio[0] + reqratio[1] + reqratio[2]+ reqratio[3]):
        x = 3

    ins_index = x
    # make endpoint
    endpoint = "http://"+r.get(instype[ins_index]+"api")+"/"+r.get(instype[ins_index]+reqtype+"tail")
    logging.info("endpoint :"+endpoint)
    return endpoint
'''
'''
# SMPL endpoint_policy
def endpoint_policy(r, reqtype, auto="off"): # default auto off, if on : new policy is calculated
    # endpoint_policy algorithm
    score = [0.0 for j in range(4)]
    inf_latency = [0.0 for j in range(4)]
    wait_time = [0.0 for j in range(4)]
    scaling_ins_sum = 0
    score_slo = [0.0 for j in range(4)]

    global isStart
    global start
    
    global isAuto
    global autoStart


    global dynamicStart # use for dynamic autoscaling (every 30 seconds)
    global AutoW #default valule : 0.3
    global WindowMax # base max value : 0.01 no action window max (prevent oscilliation)
    global WindowMin # base min value : 0.0001 no action window min (prevent oscillation)
    WindowMax = 0.01
    WindowMin = 0.001



    if isStart == 0:
        start = dt.datetime.now()
        isStart = 1
 

    # extract inf_latency and wait_time
    for ins in instype:
        ins_index = instype.index(ins)
        key = instype[ins_index] + reqtype
        inf_latency[ins_index] = float(r.get(key+"_inf_latency"))
        get_dict = json.loads(r.get(key+"_on"))
        scaling_ins_sum += int(r.get(ins+reqtype+"_scaler")) # if autoscaling trigger total number of ins 
        scaling_ins = int(r.get(ins+reqtype+"_scaler")) # if autoscaling triggered, number of ins
        ##### scaling_ins enforce test
        #if ins == "c5":
        #    scaling_ins = 20
        ##### scaling_ins enforce test
        wait_time[ins_index] = inf_latency[ins_index] * float(get_dict["inflight"])
        if ins == "i1" and (reqtype == "R" or reqtype == "B" or reqtype == "Y" or reqtype == "S"): # for fixing cortex memeory bug ,decresing i1 scores
            wait_time[ins_index] = wait_time[ins_index] * wait_time[ins_index]
        if (auto == "on" and scaling_ins > 1): # autoscaling on and scaling instance is larger than 2, wait time is recalculated
            # wait 5 minutes for autoscaling is stabled
            if isAuto == 0:
                autoStart = time.time()
                isAuto = 1
            nowAuto = time.time()
            if (nowAuto > autoStart + 300): # digit means 0 ; 0 seconds, 300 means 5 mins
                # if 5 minutes is passed
                for i in range(scaling_ins-1):
                    
                    # every 30 seconds logic is executed
                    if(time.time() > dynamicStart + 30):
                        #check inf variation
                        var = float(r.get(key+"_var"))
                        if(var < WindowMin):
                            AutoW = AutoW * 0.95 # reduce weight by -5%
                        elif(var >= WindowMin and var < WindowMax):
                            AutoW = AutoW
                        elif(var >= WindowMax):
                            AutoW = AutoW * 1.05 # improve weight by +5%
                        dynamicStart = time.time()
                        logging.warning("AutoW : " + str(AutoW) + " " + key + "_var : " + str(var))

                    wait_time[ins_index] = wait_time[ins_index] * AutoW
                    #wait_time[ins_index] = wait_time[ins_index] * (0.7)

    
    # evaluate score each instype
    #now = time.localtime()
    #now = datetime.now()
    now = dt.datetime.now()
    for ins in instype:
        ins_index = instype.index(ins)
        slo = float(r.get(reqtype+"_SLO_ms"))
        score[ins_index] += slo - ((float(inf_latency[ins_index]) + float(wait_time[ins_index])))
   
        #now = time.localtime()
        #print(time.strftime('%X', now))
        # for detail log analysis
        logging.info(str(os.getpid()) + " " +str(now-start) + " " + str(ins) + " " + str(slo) + " " + str(wait_time[ins_index])
                + " inscount i1 " + str(inscount[0]) + " p2 " + str(inscount[1]) + " p3 " + str(inscount[2]) + " c5 " + str(inscount[3])
                + " sum " + str(inscount[0]+inscount[1]+inscount[2]+inscount[3]))

    for ins in instype:
        ins_index = instype.index(ins)
        logging.info("score :" + str(score[ins_index]))
 
    # select max score
    ins_index = score.index(max(score))
    inscount[ins_index] = inscount[ins_index] + 1
    
    # for testing autoscaling
    #ins_index = 0 # TOBE DELETED

    # make endpoint
    endpoint = "http://"+r.get(instype[ins_index]+"api")+"/"+r.get(instype[ins_index]+reqtype+"tail")
    logging.info("endpoint :"+endpoint)
    return endpoint, 0.5
'''

# MRLG endpoint_policy
def endpoint_policy(r, reqtype, auto="off"): # default auto off, if on : new policy is calculated
    # endpoint_policy algorithm
    score = [0.0 for j in range(4)]
    inf_latency = [0.0 for j in range(4)]
    wait_time = [0.0 for j in range(4)]
    scaling_ins_sum = 0
    score_slo = [0.0 for j in range(4)]
    avg_latency = [0.0 for j in range(4)]
    
    global isStart # use for logging starting 00:00:00
    global start

    global isAuto # use for first autoscaling time
    global autoStart

    global dynamicStart # use for dynamic autoscaling (every 30 seconds)
    global AutoW #default valule : 0.3
    global WindowMax # base max value : 0.01 no action window max (prevent oscilliation)
    global WindowMin # base min value : 0.0001 no action window min (prevent oscillation)
    WindowMax = 0.01
    WindowMin = 0.001

    if isStart == 0:
        start = dt.datetime.now()
        isStart = 1
    
    # extract inf_latency and wait_time
    for ins in instype:
        ins_index = instype.index(ins)
        key = instype[ins_index] + reqtype
        inf_latency[ins_index] = float(r.get(key+"_inf_latency"))
        get_dict = json.loads(r.get(key+"_on"))
        scaling_ins_sum += int(r.get(ins+reqtype+"_scaler")) # if autoscaling trigger total number of ins 
        scaling_ins = int(r.get(ins+reqtype+"_scaler")) # if autoscaling triggered, number of ins
        ##### scaling_ins enforce test
        #if ins == "c5":
        #    scaling_ins = 20
        ##### scaling_ins enforce test
        wait_time[ins_index] = inf_latency[ins_index] * float(get_dict["inflight"])
        avg_latency[ins_index] = float(get_dict["avglatency"])*1000.0
        if ins == "i1" and (reqtype == "R" or reqtype == "B" or reqtype == "Y" or reqtype == "S"): # for fixing cortex memeory bug ,decresing i1 scores
            wait_time[ins_index] = wait_time[ins_index] * wait_time[ins_index]
            avg_latency[ins_index] = avg_latency[ins_index] * avg_latency[ins_index]
        if (auto == "on" and scaling_ins > 1): # autoscaling on and scaling instance is larger than 2, wait time is recalculated
            # wait 5 minutes for autoscaling is stabled
            if isAuto == 0:
                autoStart = time.time()
                isAuto = 1
            nowAuto = time.time()
            if (nowAuto > autoStart + 300): # digit means 0 ; 0 seconds, 300 means 5 mins
                # if 5 minutes is passed
                for i in range(scaling_ins-1):
                    
                        
                    # every 30 seconds logic is executed
                    if(time.time() > dynamicStart + 30):
                        #check inf variation
                        var = float(r.get(key+"_var"))
                        if(var < WindowMin):
                            AutoW = AutoW * 0.95 # reduce weight by -5%
                        elif(var >= WindowMin and var < WindowMax):
                            AutoW = AutoW
                        elif(var >= WindowMax):
                            AutoW = AutoW * 1.05 # improve weight by +5%
                        dynamicStart = time.time()
                        logging.warning("AutoW : " + str(AutoW) + " " + key + "_var : " + str(var))


                    wait_time[ins_index] = wait_time[ins_index] * AutoW
                    avg_latency[ins_index] = avg_latency[ins_index] * AutoW 
                    #wait_time[ins_index] = wait_time[ins_index] * (0.7)
                    #avg_latency[ins_index] = avg_latency[ins_index] * (0.7) 

    # evaluate score each instype
    #now = time.localtime()
    now = dt.datetime.now()
    for ins in instype:
        ins_index = instype.index(ins)
        slo = float(r.get(reqtype+"_SLO_ms"))
        if (avg_latency[ins_index] > 0.0): # if real-time avglatency is higher than 0.0
            exp_l = avg_latency[ins_index]
        else: 
            exp_l = (float(inf_latency[ins_index]) + float(wait_time[ins_index]))
        if slo >= exp_l:
            score[ins_index] += (slo - exp_l)
        else:
            score[ins_index] += (slo - exp_l)
    
        smpl_exp_l = (float(inf_latency[ins_index]) + float(wait_time[ins_index]))
        
        if ((smpl_exp_l - exp_l) > 0):
            gappluscount[ins_index] = gappluscount[ins_index] + 1
        
        if ((smpl_exp_l - exp_l) < 0):
            gapminuscount[ins_index] = gapminuscount[ins_index] + 1

        #logging.warning(str(time.strftime('%X', now)) + " " + str(ins) + " " + str(slo) + " " + str(avg_latency[ins_index]))

        # for info.log -> accumulative req for each cloud instances
        logging.info(str(os.getpid()) + " " + str(now-start) + " " + str(ins) + " " + str(slo) + " " + str(round(avg_latency[ins_index],2))
                + " inscount i1 " + str(inscount[0]) + " p2 " + str(inscount[1]) + " p3 " + str(inscount[2]) + " c5 " + str(inscount[3])
                + " sum " + str(inscount[0]+inscount[1]+inscount[2]+inscount[3]) + " MRLG " + str(exp_l) + " SMPL " + str(smpl_exp_l) + " SMPL-MRLG " + str(smpl_exp_l - exp_l) + " (SMPL-MRLG)/SMPL*100 " + str((smpl_exp_l - exp_l)/smpl_exp_l*100) + " pluscount/minuscount i1 " + str(gappluscount[0]) + 
            "/" + str(gapminuscount[0]) + " p2 " + str(gappluscount[1]) + "/" + str(gapminuscount[1]) + " p3 " + str(gappluscount[2]) + "/" + str(gapminuscount[2]) + " c5 " + str(gappluscount[3]) + "/" + str(gapminuscount[3]))
 

    for ins in instype:
        ins_index = instype.index(ins)
        logging.info("score :" + str(score[ins_index]))
 
    # select max score
    ins_index = score.index(max(score))
    inscount[ins_index] = inscount[ins_index] + 1
 

    # for testing autoscaling
    #ins_index = 0 # TOBE DELETED

    # make endpoint
    endpoint = "http://"+r.get(instype[ins_index]+"api")+"/"+r.get(instype[ins_index]+reqtype+"tail")
    logging.info("endpoint :"+endpoint)
    smpl_exp_l = (float(inf_latency[ins_index]) + float(wait_time[ins_index])) 
    return endpoint, smpl_exp_l 
'''
# MAEL endpoint_policy
def endpoint_policy(r, reqtype, auto="off"): # default auto off, if on : new policy is calculated
    # endpoint_policy algorithm
    score = [0.0 for j in range(4)]
    inf_latency = [0.0 for j in range(4)]
    wait_time = [0.0 for j in range(4)]
    scaling_ins_sum = 0

    # extract inf_latency and wait_time
    for ins in instype:
        ins_index = instype.index(ins)
        key = instype[ins_index] + reqtype
        inf_latency[ins_index] = float(r.get(key+"_inf_latency"))
        get_dict = json.loads(r.get(key+"_on"))
        scaling_ins_sum += int(r.get(ins+reqtype+"_scaler")) # if autoscaling trigger total number of ins 
        scaling_ins = int(r.get(ins+reqtype+"_scaler")) # if autoscaling triggered, number of ins
        wait_time[ins_index] = inf_latency[ins_index] * float(get_dict["inflight"])
        if ins == "i1" and (reqtype == "R" or reqtype == "B" or reqtype == "Y" or reqtype == "S"): # for fixing cortex memeory bug ,decresing i1 scores
            wait_time[ins_index] = wait_time[ins_index] * wait_time[ins_index]
        if (auto == "on" and scaling_ins > 1): # autoscaling on and scaling instance is larger than 2, wait time is recalculated
            for i in range(scaling_ins-1):
                wait_time[ins_index] = wait_time[ins_index] * (0.7)

    # evaluate score each instype
    for ins in instype:
        ins_index = instype.index(ins)
        score[ins_index] += 1/(float(inf_latency[ins_index]) + float(wait_time[ins_index]))

    for ins in instype:
        ins_index = instype.index(ins)
        logging.info("score :" + str(score[ins_index]))
 
    # select max score
    ins_index = score.index(max(score))

    # for testing autoscaling
    #ins_index = 0 # TOBE DELETED

    # make endpoint
    endpoint = "http://"+r.get(instype[ins_index]+"api")+"/"+r.get(instype[ins_index]+reqtype+"tail")
    logging.info("endpoint :"+endpoint)
    return endpoint
# SLO-MAEL endpoint_policy
def endpoint_policy(r, reqtype, auto="off"):
    # endpoint_policy algorithm
    score = [0.0 for j in range(4)]
    inf_latency = [0.0 for j in range(4)]
    wait_time = [0.0 for j in range(4)]
    score_slo = [0.0 for j in range(4)]

    # extract inf_latency and wait_time
    for ins in instype:
        ins_index = instype.index(ins)
        key = instype[ins_index] + reqtype
        inf_latency[ins_index] = float(r.get(key+"_inf_latency"))
        get_dict = json.loads(r.get(key+"_on"))
        wait_time[ins_index] = inf_latency[ins_index] * float(get_dict["inflight"])

    # evaluate score each instype
    for ins in instype:
        ins_index = instype.index(ins)
        slo = float(r.get(reqtype+"_SLO_ms"))
        exp_l = (float(inf_latency[ins_index]) + float(wait_time[ins_index]))
        if exp_l > slo:
            score_slo[ins_index] -= exp_l/slo
        else:
            score[ins_index] += 1/exp_l
        if score_slo[ins_index] < 0:
            score[ins_index] = score_slo[ins_index]

    # for debugging
    for ins in instype:
        ins_index = instype.index(ins)
        logging.info("score :" + str(score[ins_index]))
 
    # select max score
    ins_index = score.index(max(score))

    # make endpoint
    endpoint = "http://"+r.get(instype[ins_index]+"api")+"/"+r.get(instype[ins_index]+reqtype+"tail")
    logging.info("endpoint :"+endpoint)
    return endpoint
# latencyGAP-SLO-MAEL endpoint_policy
def endpoint_policy(r, reqtype, auto="off"):
    # endpoint_policy algorithm
    score = [0.0 for j in range(4)]
    inf_latency = [0.0 for j in range(4)]
    wait_time = [0.0 for j in range(4)]
    score_slo = [0.0 for j in range(4)]
    avg_latency = [0.0 for j in range(4)]

    # extract inf_latency and wait_time
    for ins in instype:
        ins_index = instype.index(ins)
        key = instype[ins_index] + reqtype
        inf_latency[ins_index] = float(r.get(key+"_inf_latency"))
        get_dict = json.loads(r.get(key+"_on"))
        wait_time[ins_index] = inf_latency[ins_index] * float(get_dict["inflight"])
        avg_latency[ins_index] = float(get_dict["avglatency"])*1000.0

    # evaluate score each instype
    for ins in instype:
        ins_index = instype.index(ins)
        slo = float(r.get(reqtype+"_SLO_ms"))
        if (avg_latency[ins_index] > 0.0): # if real-time avglatency is higher than 0.0
            exp_l = avg_latency[ins_index]
        else: 
            exp_l = (float(inf_latency[ins_index]) + float(wait_time[ins_index]))
        if slo >= exp_l:
            score[ins_index] += (slo - exp_l)
        else:
            score[ins_index] += (slo - exp_l)


    # for debugging
    for ins in instype:
        ins_index = instype.index(ins)
        logging.info("score :" + str(score[ins_index]))
 
    # select max score
    ins_index = score.index(max(score))

    # make endpoint
    endpoint = "http://"+r.get(instype[ins_index]+"api")+"/"+r.get(instype[ins_index]+reqtype+"tail")
    logging.info("endpoint :"+endpoint)
    return endpoint
'''
#################################################################

#maintaine R_start_time, R_trace_cursor, [1,2,3...]_R_trace
def trace_request(r, reqtype):
    # count number of requests in 60 mins
    trace_starttime = r.get(reqtype+"_start_time")
    if trace_starttime == "0":
        r.set(reqtype+"_start_time", time.time())
        trace_starttime = r.get(reqtype+"_start_time")
    passed_time = (int(float(time.time())) - int(float(trace_starttime)))/60 # for minutes base
    passed_time = str(int(passed_time))
    count_key = passed_time + "_" + reqtype + "_trace"
    # cursor value setting
    r.set(reqtype+"_trace_cursor", passed_time)
    
    r.incr(count_key)
    r.expire(count_key, 360)

'''
    count_key_value = r.get(count_key)
    if not count_key_value:
        r.set(count_key, 0)
        count_key_value = r.get(count_key)
    count_key_value = int(count_key_value) + 1
    r.set(count_key, count_key_value, 360) # expire 360 seconds
'''

#################################################################

@app.route("/call/R",methods=['GET', 'POST'])
def R_post():
    if(request.method == 'GET'):
        return "GET"
    elif(request.method == 'POST'):
        data = json.dumps(request.get_json())
        #logging.info(data)
        # evaluation & select endpoint
        r, queue = redis_connection()
        trace_request(r, "R") # maintain R_Start_time, R_trace_cursor, tracing request
        # request key
        req_uuid = str(uuid.uuid4())
        #req_uuidrq = str(req_uuid) +"rq"
        # endpoint selection policy
        endpoint, smpl_exp_l = endpoint_policy(r, "R", "on") # autoscaling on : "on", autoscaling off : "off"
        # request value
        req_json = {
                "progress" : 0, # 0 : before dispatch, 1 : after dispatch
                "time" : time.time(), # set current time (request insert time)
                "reqtype" : "R",
                "reqdata" : data,
                "respdata" : 0, # 0 : before dispatch, value : response data
                "latency" : 0, # 0 : before dispatch, value : latency
                "smpl_latency" : smpl_exp_l, # smpl_latency : expected latency based on offline latency
                "endpoint" : endpoint, # 0 : before dispatch, value : after endpoint decision
                "metric_check" : 0, # 0 : before metric (e.g. reqs) check, 1 : after metric check
                }
        req_json = json.dumps(req_json)
        r.set(req_uuid, req_json, 60) #if 60 : 503 reqs can not detected, so prolong the value from 60 to 120
        #// expire after 60 seconds (if expire 60s -> inflight count is correct, if no expire 60s=no expires -> inflight count is not correct, but time out reqs count value is remained in infklight value)
        # insert request priority queue
        #logging.info('* push:')
        res = queue.push(req_uuid)
        #logging.info(res)
        # reqtype tracing
        #trace_request(r, "R")
        return req_uuid

@app.route("/call/B",methods=['GET', 'POST'])
def B_post():
    if(request.method == 'GET'):
        return "GET"
    elif(request.method == 'POST'):
        data = json.dumps(request.get_json())
        #logging.info(data)
        # evaluation & select endpoint
        r, queue = redis_connection()
        trace_request(r, "B") # maintain R_Start_time, R_trace_cursor, tracing request
        # request key
        req_uuid = str(uuid.uuid4())
        #req_uuidrq = str(req_uuid) +"rq"
        # endpoint selection policy
        endpoint = endpoint_policy(r, "B", "off")
        # request value
        req_json = {
                "progress" : 0, # 0 : before dispatch, 1 : after dispatch
                "time" : time.time(), # set current time (request insert time)
                "reqtype" : "B",
                "reqdata" : data,
                "respdata" : 0, # 0 : before dispatch, value : response data
                "latency" : 0, # 0 : before dispatch, value : latency
                "endpoint" : endpoint, # 0 : before dispatch, value : after endpoint decision
                "metric_check" : 0, # 0 : before metric (e.g. reqs) check, 1 : after metric check
                }
        req_json = json.dumps(req_json)
        r.set(req_uuid, req_json, 60) #// expire after 60 seconds
        # insert request priority queue
        #logging.info('* push:')
        res = queue.push(req_uuid)
        #logging.info(res)
        return req_uuid

@app.route("/call/G",methods=['GET', 'POST'])
def G_post():
    if(request.method == 'GET'):
        return "GET"
    elif(request.method == 'POST'):
        data = json.dumps(request.get_json())
        #logging.info(data)
        # evaluation & select endpoint
        r, queue = redis_connection()
        trace_request(r, "G") # maintain R_Start_time, R_trace_cursor, tracing request
        # request key
        req_uuid = str(uuid.uuid4())
        #req_uuidrq = str(req_uuid) +"rq"
        # endpoint selection policy
        endpoint = endpoint_policy(r, "G", "off")
        # request value
        req_json = {
                "progress" : 0, # 0 : before dispatch, 1 : after dispatch
                "time" : time.time(), # set current time (request insert time)
                "reqtype" : "G",
                "reqdata" : data,
                "respdata" : 0, # 0 : before dispatch, value : response data
                "latency" : 0, # 0 : before dispatch, value : latency
                "endpoint" : endpoint, # 0 : before dispatch, value : after endpoint decision
                "metric_check" : 0, # 0 : before metric (e.g. reqs) check, 1 : after metric check
                }
        req_json = json.dumps(req_json)
        r.set(req_uuid, req_json, 60) #// expire after 60 seconds
        # insert request priority queue
        #logging.info('* push:')
        res = queue.push(req_uuid)
        #logging.info(res)
        return req_uuid

@app.route("/call/Y",methods=['GET', 'POST'])
def Y_post():
    if(request.method == 'GET'):
        return "GET"
    elif(request.method == 'POST'):
        #data = request.get_data() # binary data is extracted from get_data()
        data = ""
        #logging.info(data)
        # evaluation & select endpoint
        r, queue = redis_connection()
        trace_request(r, "Y") # maintain R_Start_time, R_trace_cursor, tracing request
        # request key
        req_uuid = str(uuid.uuid4())
        #req_uuidrq = str(req_uuid) +"rq"
        # endpoint selection policy
        endpoint = endpoint_policy(r, "Y", "off")
        # request value
        req_json = {
                "progress" : 0, # 0 : before dispatch, 1 : after dispatch
                "time" : time.time(), # set current time (request insert time)
                "reqtype" : "Y",
                "reqdata" : data,
                "respdata" : 0, # 0 : before dispatch, value : response data
                "latency" : 0, # 0 : before dispatch, value : latency
                "endpoint" : endpoint, # 0 : before dispatch, value : after endpoint decision
                "metric_check" : 0, # 0 : before metric (e.g. reqs) check, 1 : after metric check
                }
        req_json = json.dumps(req_json)
        r.set(req_uuid, req_json, 60) #// expire after 60 seconds
        # insert request priority queue
        #logging.info('* push:')
        res = queue.push(req_uuid)
        #logging.info(res)
        return req_uuid

@app.route("/call/S",methods=['GET', 'POST'])
def S_post():
    if(request.method == 'GET'):
        return "GET"
    elif(request.method == 'POST'):
        data = json.dumps(request.get_json())
        #logging.info(data)
        # evaluation & select endpoint
        r, queue = redis_connection()
        trace_request(r, "S") # maintain R_Start_time, R_trace_cursor, tracing request
        # request key
        req_uuid = str(uuid.uuid4())
        #req_uuidrq = str(req_uuid) +"rq"
        # endpoint selection policy
        endpoint = endpoint_policy(r, "S", "off")
        # request value
        req_json = {
                "progress" : 0, # 0 : before dispatch, 1 : after dispatch
                "time" : time.time(), # set current time (request insert time)
                "reqtype" : "S",
                "reqdata" : data,
                "respdata" : 0, # 0 : before dispatch, value : response data
                "latency" : 0, # 0 : before dispatch, value : latency
                "endpoint" : endpoint, # 0 : before dispatch, value : after endpoint decision
                "metric_check" : 0, # 0 : before metric (e.g. reqs) check, 1 : after metric check
                }
        req_json = json.dumps(req_json)
        r.set(req_uuid, req_json, 60) #// expire after 60 seconds
        # insert request priority queue
        #logging.info('* push:')
        res = queue.push(req_uuid)
        #logging.info(res)
        return req_uuid



####################################################################
if __name__ == "__main__":
	app.run()
