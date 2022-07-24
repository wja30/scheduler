for ((i=0;i<15;i++)); do
	nohup python dispatcher/dispatcher.py &
done
for ((i=0;i<1;i++)); do # 2022.7.24:10, change to 1 (if 10, total reqs count be overlapped)
	nohup python on_meta/on_meta.py &
done

nohup python scaler/scaler.py &
nohup python dispatcher/metric.py &
