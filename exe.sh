for ((i=0;i<5;i++)); do
	nohup python dispatcher/dispatcher.py &
done
nohup python on_meta/on_meta.py &
nohup python scaler/scaler.py &
nohup python dispatcher/metric.py &
