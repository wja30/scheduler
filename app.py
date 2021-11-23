from flask import Flask, render_template, request, current_app
import boto3
#from redis import Redis
import json
import requests
import time
from rpq.RpqQueue import RpqQueue
import redis
from redis import Redis

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
    logging.basicConfig(filename='logs/info.log', level=logging.INFO,format='%(asctime)s: %(message)s')



endpoint = "http://a53eba14af27b4920a2505c87169e06a-deef86faded1927d.elb.us-east-1.amazonaws.com/image-classifier-resnet50-i1"
#data = json.dumps({'url': 'https://i.imgur.com/213xcvs.jpg'})
headers = {"content-type": "application/json"}
headers_binary = {"content-type": "application/octet-stream"}
timeout = 60
##########################################

def test():
    logging.info("test")


def redis_connection():
    r = Redis(host='23.23.220.207', port=6379, decode_responses=True, password='redisscheduler')
    if r.ping():
        logging.info("Connected to Redis")
    # Redis instance
    rq = redis.StrictRedis(host='23.23.220.207', port=6379, db=0, password='redisscheduler')
    # RpqQueue
    queue = RpqQueue(rq, 'simple_queue')
    return r, queue

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
        return "off_meta"
##########################################

@app.route("/call/R",methods=['GET', 'POST'])
def R_post():
    if(request.method == 'GET'):
        return "GET"
    elif(request.method == 'POST'):
      
        data = json.dumps(request.get_json())
        logging.info(data)

        # evaluation & select endpoint

        r, queue = redis_connection()

        # scheduler manager

        logging.info('* push:')
        res = queue.push('test_item')
        logging.info(res)

        logging.info('* pop:')
        item = queue.popOne()
        if item:
            logging.info(item)
        else:
            logging.info('Queue is empty')
        #logging.info("Key {} was set at {} and has {} seconds until expired".format(keyName, keyValue, keyTTL))

        try:
            resp = requests.post(
                    endpoint,
                    data = data,
                    headers = headers,
                    timeout = timeout,
             )
        except Exception as e:
            print(e)

        # cortex metadata management (inflight req, avg latnecy, etc)
        # cortex endpoint management
        return json.dumps(resp.json())


if __name__ == "__main__":
	app.run()
