# dispatcher
* execution method

nohup python dispatcher.py &

[1] 4400

* process monitor

ps -ef | grep dispatcher

root       935     1  0 01:27 ?        00:00:00 /usr/bin/python3 /usr/bin/networkd-dispatcher --run-startup-triggers

ubuntu    4400  2318 15 04:06 pts/2    00:00:25 python dispatcher.py

ubuntu    4426  2318  0 04:09 pts/2    00:00:00 grep --color=auto dispatcher

* kill process

ubuntu@ip-172-31-29-223:~/scheduler/dispatcher$ kill -9 4400

[1]+  Killed                  nohup python dispatcher.py
