# on_meta
* execution method

nohup python scaler.py &
nohup python trainer.py &

[1] 4400

* process monitor

ps -ef | grep -e dispatcher -e on_meta -e gunicorn -e scaler -e trainer

* train

$python3 train.py (default 1 epoch)
output : 0_my_model_32.h5

* predict

$pythone3 predict.py (using 52_my_model_32.h5)
input : trace file (*.csv)
output : predicion file (predict_result.csv)

* kill process

ubuntu@ip-172-31-29-223:~/scheduler/dispatcher$ kill -9 4400

[1]+  Killed                  nohup python dispatcher.py
