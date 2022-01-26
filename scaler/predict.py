#!/usr/bin/env python3

import sys
from time import time
import csv
from pandas import DataFrame
from pandas import Series
from pandas import concat
from pandas import read_csv
from pandas import datetime
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
#from sklearn.externals import joblib
import joblib
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import LSTM
# from keras.callbacks import TensorBoard
from math import sqrt
import math
import matplotlib
matplotlib.use('agg')
from matplotlib import pyplot
from numpy import array
import numpy as np
import tensorflow.keras as ks
import pandas as pd
import logging
import time
import redis
from redis import Redis

logging.basicConfig(filename='logs/scaler.log', level=logging.INFO,format='%(asctime)s: %(message)s')

def forecast_lstm(model, X):
        X = X.reshape(1, 1, len(X))
        forecast = model.predict(X, batch_size=1)
        # return an array
        return [x for x in forecast[0, :]]

def inverse_difference(last_ob, forecast):
    inverted = list()
    inverted.append(forecast[0] + last_ob)
    for i in range(1, len(forecast)):
        inverted.append(forecast[i] + inverted[i-1])
    return inverted

def inverse_transform(scaler, forecast, current_load):
    forecast = array(forecast)
    forecast = forecast.reshape(1, len(forecast))
    # invert scaling
    inv_scale = scaler.inverse_transform(forecast)
    inv_scale = inv_scale[0, :]
    inv_diff = inverse_difference(current_load, inv_scale)
    return inv_diff

##############################################################################################################
model = ks.models.load_model("./scaler/52_my_model_32.h5")
scaler = joblib.load("./scaler/my_scaler.save")
#############################################################################################################
buf = [0]
timeout = 2 # two elements prediction (e.g 60sec + 60sec)
modelfile = "./scaler/52_my_model_32.h5"
original_file = "./test_trace.csv"
result_file = "./predict_result.csv"
future_min = 15 # predict after furture_min minutes
instype = ["i1", "p2", "p3", "c5"]
reqtype = ["R", "B", "G", "Y", "S"]

#############################################################################################################

def redis_connection():
    r = Redis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    return r 

def lstm_predict(last_step, current_load, future_min, reqtype):
    buf2 = [0]
    x = [[(current_load - last_step)]]
    #last_step = current_load
    x=np.asarray(x)
    x.reshape(-1, 1)
    y = scaler.transform(x)
    forecast = forecast_lstm(model, y)
    forecast_real = inverse_transform(scaler, forecast, current_load)

    for index, value in enumerate(forecast_real):
        if index > future_min:
            break
        buf2.append(int(value))
    for index, val in enumerate(buf2):
        print(f'future_min[after {index}mins] : {val}')
    
    if reqtype == "G": # for sensitive G
        result_max = buf2[buf2.index(max(buf2[1:2]))]
    else:
        result_max = buf2[buf2.index(max(buf2[1:]))]
    print(f'result_max : {result_max}')
    result_min = buf2[buf2.index(min(buf2[1:]))]
    print(f'result_min : {result_min}')
    result_delta = (result_max - result_min)
    print(f'result_delta : {result_delta}')
    if (current_load > last_step): # if traffic is increasing, result_delta plusing
        if reqtype == "R":
            result = result_max + result_delta*5
        elif reqtype == "B":
            result = result_max + result_delta*2
        elif reqtype == "G":
            result = result_max + result_delta*0
        elif reqtype == "Y":
            result = result_max + result_delta*3
        elif reqtype == "S":
            result = result_max + result_delta*3
    else:
        result = result_max
    print(f'lstm_result : {result}')
    return result     
    #return buf2[future_min] 

def scaling_policy(r, result, reqType): # based on predicted reqType's # of reqs in 60 mins determine policy of auto scaling

    total_capacity = [0,0,0,0,0] # for each reqtype, total capacity is needed [R, B, G, Y, S]
    scaler = [0,0,0,0] # for each reqtype, store the predicted the number of instance  [i1, p2, p3, c5]
    cost = [0,0,0,0]
    performance = [0,0,0,0]
    score = [0.0 for j in range(4)] # for each reqtype, scoring for each instance [i1, p2, p3, c5]
    req_index = reqtype.index(reqType)
    

    # to determine autoscaling poloicy, extract offline information
    for ins in instype:
        ins_index = instype.index(ins)
        total_capacity[req_index] += int(r.get(ins+reqType+"_reqs_satiSLO_60sec"))
        cost[ins_index] = float(r.get(ins+"_cost_$_hour"))
        performance[ins_index] = float(r.get(ins+reqType+"_reqs_satiSLO_60sec"))
        exp_l = (float(cost[ins_index]) / float(performance[ins_index]))
        score[ins_index] += 1/exp_l # 성능/비용

    # for debugging
    for ins in instype:
        ins_index = instype.index(ins)
        logging.info("scaling score :" + str(score[ins_index]))
        print(f'scaling sorce :{ins} : {score[ins_index]}') 
    # select max score
    max_ins_index = score.index(max(score))

    # determine scaling_policy
    if total_capacity[req_index] > result:
        # if total_capacity is sufficient (assumption : i1*1, p2*1, p3*1, c5*1)
        for ins in instype:
            scaler_value = r.get(ins+reqType+"_scaler")
            if int(scaler_value) > 1:
                scaler_value = int(scaler_value) - 1
            #scaler_value = 1
            # nothing done (depends on cortex autoscalnig stabilization)
            r.set(ins+reqType+"_scaler", scaler_value)
            print(f'scaler value :{ins}: {scaler_value}')
    elif total_capacity[req_index] < result:
        # if total_capacity is not sufficeint -> autoscaling triggered
        for ins in instype:
            scaler_value = r.get(ins+reqType+"_scaler")
            scaler_update_value = 1 + math.ceil( (result - total_capacity[req_index]) / performance[max_ins_index] )
            if instype.index(ins) != max_ins_index:
                r.set(ins+reqType+"_scaler", scaler_value)
                print(f'scaler update value : {ins} : {scaler_value}')
            elif instype.index(ins) == max_ins_index: # only performance/cost value max is selected
                r.set(ins+reqType+"_scaler", scaler_update_value)
                print(f'scaler update value : {ins} : {scaler_update_value}')
       
        
      
if __name__ == "__main__":
    r = redis_connection()
#    last_step = 100
#    current_load = 1000
    #future_min = 5
#    result = lstm_predict(last_step, current_load, future_min)
#    print(f'last : {last_step}, current : {current_load}, result : {result}')

    for reqType in reqtype:
        trace_starttime = r.get(reqType+"_start_time")
        if trace_starttime != "0": # reqtype request is ocurred
            count_key_cursor = int(r.get(reqType+"_trace_cursor"))
            if count_key_cursor >= 2: # at least greater than 2 : last_step : 0_R_trace, current : 1_R_trace
                last_step = r.get(str(count_key_cursor-2)+"_"+reqType+"_trace")
                current_load = r.get(str(count_key_cursor-1)+"_"+reqType+"_trace")
                if last_step is None:
                    last_step = 0
                    current_load = 0
                #if current_load is None:
                #    current_load = 0
                result = lstm_predict(int(last_step), int(current_load), future_min, reqType)
                delta = int(current_load) - int(last_step)
                print(f'reqtype : {reqType} last : {last_step}, current : {current_load}, result : {result}, delta {delta}')
                scaling_policy(r, result, reqType)


