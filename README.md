# scheduler

# operation check

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
