for ((i=0;i<15;i++)); do
	nohup python dispatcher/dispatcher.py &
done
for ((i=0;i<10;i++)); do
	nohup python on_meta/on_meta.py &
done

nohup python scaler/scaler.py &
nohup python dispatcher/metric.py &
