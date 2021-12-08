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

import logging
from redis import Redis
import uuid
import math
import redis
from rpq.RpqQueue import RpqQueue

logging.basicConfig(filename='logs/sender.log', level=logging.INFO,format='%(asctime)s: %(message)s')

global number_reqs
number_reqs = 0
endpoint = "http://34.233.80.127/call/R"
endpoint_check = "http://34.233.80.127/check"
headers = {"content-type": "application/json"}
headers_binary = {"content-type": "application/octet-stream"}
timeout = 60
payloadR = json.dumps({'url': 'https://i.imgur.com/213xcvs.jpg'})

def sender(data):
    try:
        resp = requests.post(
                endpoint,
                data = payloadR,
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
    #time.sleep(1)
'''    
    while True:
        try :
            time.sleep(0.1)
            resp = requests.post(
                    endpoint_check,
                    data = request_uuid,
                    headers = headers,
                    timeout = timeout,
                    )
            if (resp.text != "0"):
                logging.info("req_result : " + resp.text)
        except Exception as e:
            print(e)
        if resp.text == "0":
            continue
        break
'''
def send_data(timeout, reader):
    pool = ThreadPoolExecutor(100000)
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
        print(len(samples))
        for s in samples:
            number_reqs += 1
            if number_reqs % 100 == 0:
                print(f'number_reqs : {number_reqs}')
            pool.submit(sender, data)
            time.sleep(s/1000.0)

with open(f'./tweet_load_10-16_test.csv', 'r') as f:
    reader = csv.DictReader(f)
    send_data(2,reader)
