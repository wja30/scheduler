import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
import json
import time
from os.path import abspath, dirname, join
from base64 import b64encode, b64decode
import random
import requests
import boto3, json
import numpy as np
from urllib import request
import logging
from redis import Redis
import uuid
import math
import redis
from rpq.RpqQueue import RpqQueue

logging.basicConfig(filename='logs/sender.log', level=logging.INFO,format='%(asctime)s: %(message)s')

global number_reqs
number_reqs = 0
url = "https://wja300-cortex.s3.amazonaws.com/sound-classifier/mia.wav"
endpoint = "http://34.233.80.127/call/"
reqtype = ["R", "B", "G", "Y", "S"]
reqratio = [100, 0, 0, 0, 0] # req ratio (R : B : G : Y : S)
#endpoint_check = "http://34.233.80.127/check"
headers = {"content-type": "application/json"}
headers_binary = {"content-type": "application/octet-stream"}
timeout = 60
payload = ""
payloadR = json.dumps({'url': 'https://i.imgur.com/213xcvs.jpg'})
payloadB = json.dumps({'review': 'the movie was amazing!'})
payloadG = json.dumps({'text': 'machine learning is'})
payloadY = request.urlopen(url).read()
payloadS = json.dumps({'url': 'https://i.imgur.com/PzXprwl.jpg'})
def sender(data):
    # x : R, B, G, Y, S
    logging.info("sender")
    x = random.randrange(0, 100)
    logging.info("beforex : " + str(x))
    try:
        if(x >=0 and x < reqratio[0]):
            x = 0
        if(x >= reqratio[0] and x < reqratio[0] + reqratio[1]):
            x = 1
        if(x >= reqratio[0] + reqratio[1] and x < reqratio[0] + reqratio[1] + reqratio[2]):
            x = 2
        if(x >= reqratio[0] + reqratio[1] + reqratio[2] and x < reqratio[0] + reqratio[1] + reqratio[2]+ reqratio[3]):
            x = 3
        if(x >= reqratio[0] + reqratio[1] + reqratio[2]+ reqratio[3] and x < reqratio[0] + reqratio[1] + reqratio[2]+ reqratio[3] + reqratio[4]):
            x = 4
    except Exception as e:
        logging.info(e)
    logging.info("afterx : " + str(x))
    newendpoint =  endpoint + str(reqtype[x]) # when R(image-resnet50 calls)
    logging.info("newendpoint :" + newendpoint)
    
    if str(reqtype[x]) == "R":
        payload = payloadR
    elif str(reqtype[x]) == "B":
        payload = payloadB
    elif str(reqtype[x]) == "G":
        payload = payloadG
    elif str(reqtype[x]) == "Y":
        payload = payloadY
    elif str(reqtype[x]) == "S":
        payload = payloadS
    
    try:
        resp = requests.post(
                newendpoint,
                data = payload,
                headers = headers,
                timeout = timeout,
                )
    except Exception as e:
        print(e)
    try :
        req_uuid = resp.text
        logging.info("req_uuid : " + req_uuid)
    except Exception as e:
        print(e)
    request_uuid = json.dumps({'request_uuid' : req_uuid})
    return "0"

def send_data(timeout, reader):
    pool = ThreadPoolExecutor(5000)
    data = ""
    global number_reqs
    
    
    for row in reader:
        if reader.line_num > timeout:
            break

        number_reqs = 0
        # resnet tps : 169/s 
        # 169*60 = 10140
        # tweet avg : 3312.91
        # tweet min : 1
        # tweet max : 91113
        # 1/3 정도 수준으로 감소 시키면 적정함 
        num = int(int(row['tweets']) * 1)
        num1 = int(row['tweets'])
        print(f'row[tweets] : {num1}')
        print(f'num : {num}')
        lam = (60 * 1000.0) / num
        samples = np.random.poisson(lam, num)
        print(f'line: {reader.line_num}; sample_number: {num}')
        print(f'lam : {lam}')
        print(f'samples : {samples}')
        print(f'bug : {len(samples)}')
        logging.info("test")
        try:
            for s in samples:
                number_reqs += 1
                if number_reqs % 100 == 0:
                    print(f'number_reqs : {number_reqs}')
                logging.info("bofore pool")
                pool.submit(sender, data)
                logging.info("after pool")
                time.sleep(s/1000.0)
        except Exception as e:
            logging.info(e)

with open(f'./tweet_load_10-16_test.csv', 'r') as f:
    reader = csv.DictReader(f)
    send_data(10,reader)

