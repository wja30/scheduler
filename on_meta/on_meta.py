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


def redis_connection():
    r = redis.StrictRedis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    # Redis instance
    rq = redis.StrictRedis(host='23.23.220.207', port=6379, db=0, password='redisscheduler')
    # RpqQueue
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue

def on_meta(r, queue):
    # on_meta information update (for window recent 60 seconds)
    for key in r.scan_iter("*-*"):
        logging.info("keys : " + key)

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
        if r.keys("*-*"):
            pool.submit(on_meta, r, queue)
            time.sleep(1)
if __name__ == "__main__":
	on_meta_main()
