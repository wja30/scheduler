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
import subprocess

logging.basicConfig(filename='logs/scaler.log', level=logging.INFO,format='%(asctime)s: %(message)s')


def redis_connection():
    r = redis.StrictRedis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    # Redis instance
    rq = redis.StrictRedis(host='23.23.220.207', port=6379, db=0, password='redisscheduler')
    # RpqQueue
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue

def scaler(r):
    #train execution
    subprocess.call("./scaler/predict.py", shell=True)
    return "scaler"


def scaler_main():
    r, queue = redis_connection()
    cnt = 0
    while True:
        scaler(r)
        time.sleep(50)

if __name__ == "__main__":
    scaler_main()
