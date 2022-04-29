#!/bin/sh
for d in */; do
	if [ $d = "plots/" ]; then
		continue
	fi
	echo $d
	cd $d
	python ../graph_resource_logs.py resources.log
	cd ..
	mv $d*.pdf plots
done

python graph_violations.py bad_penalty_300 combo oracle
python graph_violations.py bad_penalty_30 combo oracle
python graph_violations.py global_kill combo oracle
python graph_violations.py bad combo oracle
python graph_violations.py pegasus combo oracle
mv *.pdf plots
