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
#############################################################################################################


with open(original_file, 'r') as f:
    timeout_buf = timeout + 2
    reader = csv.DictReader(f)
    for row in reader:
        if reader.line_num > timeout_buf:
            break
        buf.append(int(row['tweets']))
        last_step = int(row['tweets'])
        current_load = int(row['tweets'])
        print(f'original_trace[{reader.line_num-1}mins] : {buf[reader.line_num-1]}')

with open(original_file, 'r') as fr:
    timeout_real = timeout + 1
    reader = csv.DictReader(fr)
    buf2 = ['']
    with open(result_file, 'w') as fw:
        fw.write("\"time\",\"tweets_predict\"\n") # real write to file
    for row in reader:
        if reader.line_num > timeout_real:
            break
        print(f'reader.line_num : {reader.line_num}')
        logging.info(f'reader.line_num : {reader.line_num}')
        with open(result_file, 'a') as fw:
                buf2 = ['']
                last_step = buf[reader.line_num-1]
                current_load = buf[reader.line_num]
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
                fw.write("\"" + row['time'] +"\"" + "," + str(buf2[future_min]) + "\n") # real write to file
