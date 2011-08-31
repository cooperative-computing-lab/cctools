#!/bin/bash
ulimit -c 400000000


# Benchmark Parameters

# Name of this benchmark. The generated makeflow would be $proj.makeflow.  The
# project name returned by work_queue_status would be $user-$proj-$worker.
# $user is the unix login username and $worker is the number of workers are
# provided for the corresponding run.
proj="master.location"

# number of workloads to run in each experiment iteration
workloads=(1 2 4 6 8 10 12 14 15 16 18 20)
#workloads=(1 2 3 4 5 6 7 8 9 10)
#workloads=(2 4 6 8 10)
workloads_max=`echo ${workloads[@]} | tr ' ' '\n' | sort -nr | head -1 `

# Workload format: "input_size:execution_time:output_size:num_of_tasks". Note
# that the execution time is in seconds. And the unit for input/output size is
# set by the "unit" variable.
workload="200:100:1:10"
unit="M"

# The directory to store input/output files
dir_input="input"
dir_output="output"
dir_makeflows="makeflows"

# If yes, all the tasks would share the same input, otherwise every task has
# its independing input. 
shared_input="yes"

# Number of workers for each workload
num_of_workers=10

# Max retry times when makeflow jobs fail
retry_max=30

# yrange max in the work queue log plot
yrange_max=$((num_of_workers + 30))
if [ "$yrange_max" -lt "60" ] ; then
	yrange_max=60
fi

settings="settings.txt"
rm -rf $settings
echo "Project: $proj
Workloads: ${workloads[@]}
Workload specs: $workload
Shared input: $shared_input
Number of workers for each workload: $num_of_workers
Retry max: $retry_max
Plot y range max: $yrange_max" >> $settings

# Functions
getfield () {
	echo $1 | awk -F':' '{print $'$2'}'
}

validate_input () {
	# Create input files if necessary
	for ((j=1;j<=$workloads_max;j++)); do
		inputdir=$dir_input.$j
		mkdir -p $inputdir
		cd $inputdir
		if [ "$shared_input" = "yes" ]; then
			if [ -e "0.in" ]; then
				size=`stat -c%s 0.in`
				if [ "$size" -ne "$input_size" ]; then
					dd if=/dev/zero of=0.in bs=1$unit count=$in
				fi
			else
				dd if=/dev/zero of=0.in bs=1$unit count=$in
			fi
		else 
			for ((i=1;i<=$num;i++));do
				if [ -e $i.in ]; then
					size=`stat -c%s $i.in`
					if [ "$size" -ne "$input_size" ]; then
						dd if=/dev/zero of=$i.in bs=1$unit count=$in
					fi
				else
					dd if=/dev/zero of=$i.in bs=1$unit count=$in
				fi
			done
		fi
		cd ../
	done
}

gen_sub_makeflows () {
	for ((j=1;j<=$workloads_max;j++)); do
		# Generate makeflow
		inputdir=$dir_input.$j
		outputdir=$dir_output.$j
		submakeflow=workload.$j.makeflow
		rm -rf $submakeflow
		echo "# This is an auto-generated makeflow script" >> $submakeflow
		for ((i=1;i<=$num;i++));do
			## Pipelined workflow that contains intermediate data
			#echo -e "$outputdir/$i.tmp:$inputdir/$i.in\n\t mkdir -p $outputdir; dd if=/dev/zero of=$outputdir/$i.tmp bs=1$unit count=$out; sleep $exe \n" >> $submakeflow
			#echo -e "$outputdir/$i.out:$outputdir/$i.tmp\n\t mkdir -p $outputdir; dd if=/dev/zero of=$outputdir/$i.out bs=1$unit count=$out; sleep $exe \n" >> $submakeflow
			if [ "$shared_input" = "yes" ]; then
				echo -e "$outputdir/$i.out:$inputdir/0.in\n\t mkdir -p $outputdir; dd if=/dev/zero of=$outputdir/$i.out bs=1$unit count=$out; sleep $exe \n" >> $submakeflow
			else 
				echo -e "$outputdir/$i.out:$inputdir/$i.in\n\t mkdir -p $outputdir; dd if=/dev/zero of=$outputdir/$i.out bs=1$unit count=$out; sleep $exe \n" >> $submakeflow
			fi
		done
	done
}

gen_grand_makeflows () {
	if [ "$1" = "local" ]; then
		expr="local"
		location="LOCAL"
	elif [ "$1" = "remote" ]; then 
		expr="remote"
		location=""
	else 
		echo "Failed to generate makeflow scripts: argument to gen_grand_makeflows() should be either \"local\" or \"remote\"!"
		exit 1
	fi

	for i in "${workloads[@]}"; do
		makeflowscript=$expr.$i.makeflow
		rm -rf $makeflowscript
		echo "# This is an auto-generated makeflow script" >> $makeflowscript
		for ((j=1;j<=$i;j++)); do
			inputdir=$dir_input.$j
			outputdir=$dir_output.$j
			submakeflow=workload.$j.makeflow

			makeflowlog=$submakeflow.makeflowlog
			wqlog=$submakeflow.wqlog
			makeflowoutput=$submakeflow.stdout.stderr
			projname=lyu2.$j

			echo -e "$outputdir $makeflowoutput $makeflowlog $wqlog : $inputdir $submakeflow makeflow \n\t $location makeflow -T wq -d all -a -e -N $projname -p -1 -r $retry_max $makeflowargs $submakeflow &> $makeflowoutput\n" >> $makeflowscript
		done
	done
}

gen_makeflows () {
	experiments=( local remote )
	rm -rf $dir_makeflows
	mkdir -p $dir_makeflows

	gen_sub_makeflows

	for expr in ${experiments[@]} ; do
		gen_grand_makeflows $expr
	done

	for expr in ${experiments[@]} ; do
		for i in "${workloads[@]}"; do
			mkdir -p $dir_makeflows/$i
			for ((j=1;j<=$i;j++)); do
				cp workload.$j.makeflow $dir_makeflows/$i/
			done
			mv $expr.$i.makeflow $dir_makeflows/$i/
		done
	done

	for ((j=1;j<=$workloads_max;j++)); do
		rm workload.$j.makeflow
	done
}

start_workers () {
	echo Starting $workloads_max \* $num_of_workers workers for executing the workloads ...
	for ((j=1;j<=$workloads_max;j++)); do
		projname=lyu2.$j
		./work_queue_pool -T condor -f -a -N $projname -t 86400 $num_of_workers 
		sleep 5
	done
	echo $((workloads_max * num_of_workers)) are started successfully.

	echo Starting workers for hosting multiple masters ...
	./work_queue_pool -T condor -f -a -N lyu2.0 -t 86400 $((workloads_max)) 
	echo $((workloads_max + 10)) are started successfully.
}


run_experiment () {
	expr=$1
	if [ "$expr" != "local" ] && [ "$expr" != "remote" ] ; then
		echo "Failed to run experiment: argument to run_experiment() should be either \"local\" or \"remote\"."
		exit 1
	fi

	exprresult=$expr
	rm -rf $exprresult
	mkdir -p $exprresult

	times=$expr.times
	rm -rf $times

	for i in "${workloads[@]}"; do
		cp $dir_makeflows/$i/* .

		makeflowscript=$expr.$i.makeflow

		makeflowlog="$makeflowscript.makeflowlog"
		wqlog="$makeflowscript.wqlog"
		makeflowoutput="$makeflowscript.stdout.stderr"


		echo Start running $expr masters experiment \($i workloads\) ...
		if [ "$expr" = "local" ] ; then
			./makeflow -d all -j $i -r $retry_max $makeflowargs $makeflowscript &> $makeflowoutput 
		fi

		if [ "$expr" = "remote" ] ; then
			./makeflow -T wq -d all -a -e -N lyu2.0 -r $retry_max $makeflowargs $makeflowscript &> $makeflowoutput 
		fi

		# Get turnaround time
		started=`grep 'STARTED' $makeflowlog | awk '{print $3}'`
		completed=`grep 'COMPLETED' $makeflowlog | awk '{print $3}'`
		duration=`bc <<< "$completed - $started"`

		# Do some statistics
		echo $in $exe $out $num $num_of_workers $i $duration >> $times

		dir=$expr.$i
		rm -rf $dir
		mkdir -p $dir
		
		if [ "$expr" = "remote" ] ; then
			mv $wqlog $dir/
		fi
		mv $makeflowoutput $makeflowlog $dir/

		for ((j=1;j<=$i;j++)); do
			submakeflow=workload.$j.makeflow

			makeflowlog="$submakeflow.makeflowlog"
			wqlog="$submakeflow.wqlog"
			makeflowoutput="$submakeflow.stdout.stderr"

			grep QUEUE $wqlog > $j.queue
			mv $makeflowlog $wqlog $makeflowoutput $dir/
		done

		gen_wqlog_gnuplot $expr $num_of_workers "capacity" $i
		gnuplot < $expr.capacity.gnuplot
		gen_wqlog_gnuplot $expr $num_of_workers "busyworkers" $i
		gnuplot < $expr.busyworkers.gnuplot

		mv *.queue *.gnuplot *.png $dir/

		mv $dir $exprresult/
		makeflow -c $makeflowscript
	done

	mv $times $exprresult/
	mv $exprresult $projresult/
}

gen_wqlog_gnuplot () {
	expr=$1
	workers_provided=$2
	curve=$3
	num_of_workloads=$4
	wqloggnuplot=$expr.$curve.gnuplot

	if [ "$curve" = "capacity" ] ; then
		column=17
	elif [ "$curve" = "busyworkers" ]; then
		column=6
	else
		column=0
	fi

	# Generate gnuplot script
	echo "set terminal png

set output \"$expr.$curve.png\"

set title \"In: $in $unit.B. Exe: $exe sec Out: $out $unit.B. Tasks: $num Workers $workers_provided \"

set xdata time
set timefmt \"%s\"
set format x \"%M:%S\"

set xlabel \"Time\"
set ylabel \"Count\"

set yrange [0:${yrange_max}]

plot \"1.queue\" using (\$2 / 1000000):$column title \"workload 1\" with lines lt 1 lw 3, \\" >> $wqloggnuplot

	for ((i=2;i<$num_of_workloads;i++)); do
		echo "    \"$i.queue\" using (\$2 / 1000000):$column title \"workload $i\" with lines lt $i lw 3, \\" >> $wqloggnuplot
	done

	echo "    \"$num_of_workloads.queue\" using (\$2 / 1000000):$column title \"workload $num_of_workloads\" with lines lt $num_of_workloads lw 3 " >> $wqloggnuplot
}




# Main Program	

in=`getfield $workload 1`
exe=`getfield $workload 2`
out=`getfield $workload 3`
num=`getfield $workload 4`

if [ $unit == "M" ]; then
	unit_size=$((1024*1024))
elif [ $unit == "K" ]; then
	unit_size=$((1024))
else 
	unit_size=1
fi

input_size=$(($in * $unit_size))

projresult="$proj.result"
rm -rf $projresult
mkdir -p $projresult

mv $settings $projresult/

validate_input

gen_makeflows

#start_workers

run_experiment "local"

run_experiment "remote"

mv *.makeflow $projresult/

