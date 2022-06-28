# scheduler
test2022.06.28
# operation check
ps -ef | grep -e dispatcher -e on_meta -e gunicorn -e scaler

# execution all process
./exe.sh

# deleting all process
./exedel.sh

gunicorn -b 0.0.0.0:8000 app:app -w 30 --threads 30
gunicorn -b 0.0.0.0:8000 app:app -w 100 --threads 100
$ sudo systemctl restart nginx
$ sudo systemctl restart helloworld


1. offline endpoint manager 
* post [scheduler endpoint]/off/endpoint [json body] 
* e.g {
    "i1operator" : "a8e2352d8432b40aa93e908b9bc952a5-c8d053a2526cdc9e.elb.us-east-1.amazonaws.com",
}

2. offline meta mananger
* post [scheduler endpoint]/off/meta [json body] 
* e.g {
    "i1operator" : "a8e2352d8432b40aa93e908b9bc952a5-c8d053a2526cdc9e.elb.us-east-1.amazonaws.com",
}
