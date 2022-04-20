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

python graph_violations.py bad combo
mv wfs_started.pdf plots
