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
model = ks.models.load_model("52_my_model_32.h5")
scaler = joblib.load("my_scaler.save")
#############################################################################################################
buf = [0]
timeout = 2 # two elements prediction (e.g 60sec + 60sec)
modelfile = "52_my_model_32.h5"
original_file = "./test_trace.csv"
result_file = "./predict_result.csv"
future_min = 5 # predict after furture_min minutes
instype = ["i1", "p2", "p3", "c5"]
reqtype = ["R", "B", "G", "Y", "S"]

#############################################################################################################

def redis_connection():
    r = Redis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    return r 

def lstm_predict(last_step, current_load, future_min):
    buf2 = ['']
    x = [[(current_load - last_step)]]
    last_step = current_load
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
    return buf2[future_min] 

if __name__ == "__main__":
    r = redis_connection()
    last_step = 233
    current_load = 270
    #future_min = 5
    result = lstm_predict(last_step, current_load, future_min)
#    print(f'last : {last_step}, current : {current_load}, result : {result}')

    for reqType in reqtype:
        trace_starttime = r.get(reqType+"_start_time")
        if trace_starttime != "0": # reqtype request is ocurred
            count_key_cursor = int(r.get(reqType+"_trace_cursor"))
            if count_key_cursor >= 2: # at least greater than 2 : last_step : 0_R_trace, current : 1_R_trace
                last_step = r.get(str(count_key_cursor-2)+"_"+reqType+"_trace")
                current_load = r.get(str(count_key_cursor-1)+"_"+reqType+"_trace")
                result = lstm_predict(int(last_step), int(current_load), future_min)
                print(f'reqtype : {reqType} last : {last_step}, current : {current_load}, result : {result}')


