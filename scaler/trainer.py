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

logging.basicConfig(filename='logs/trainer.log', level=logging.INFO,format='%(asctime)s: %(message)s')


def redis_connection():
    r = redis.StrictRedis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    # Redis instance
    rq = redis.StrictRedis(host='23.23.220.207', port=6379, db=0, password='redisscheduler')
    # RpqQueue
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue

def trainer(r):
    subprocess.call("./train.py", shell=True)
    return "trainer"
 
def trainer_main():
    r, queue = redis_connection()
    cnt = 0
    while True:
        trainer(r)
        time.sleep(30)

if __name__ == "__main__":
   trainer_main()
