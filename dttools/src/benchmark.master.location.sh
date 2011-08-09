#!/bin/bash
ulimit -c 400000000


# Benchmark Parameters

# Name of this benchmark. The generated makeflow would be $proj.makeflow.  The
# project name returned by work_queue_status would be $user-$proj-$worker.
# $user is the unix login username and $worker is the number of workers are
# provided for the corresponding run.
proj="master.location"

num_of_workloads=2

# Workload format: "input_size:execution_time:output_size:num_of_tasks". Note
# that the execution time is in seconds. And the unit for input/output files
# can be set by the "unit" global variable below.
workload="5:10:5:3"
unit="M"

# The directory to store input/output files
dir_input="input"
dir_output="output"

# If yes, all the tasks would share the same input, otherwise every task has
# its independing input. This parameter can be used to test the worker caching
# behavior.
shared_input="no"

# Additional arguments added to makeflow.  Format: "symbolic_name:real
# arguments". All results related to one "real argument" would be stored under
# the directory that contains the correspoinding symbolic name
#makeflow_args=( "fcfs:-z fcfs"
#				"fd:-z fd" )
makeflow_args=( "ad:-z adaptive" )

# Max retry times when makeflow jobs fail
retry_max=30

# Number of workers for each run
workers=( 20 )

# yrange max in the work queue log plot
yrange_max=$((`echo ${workers[@]} | tr ' ' '\n' | sort -nr | head -1 ` + 50))
if [ "$yrange_max" -lt "120" ] ; then
	yrange_max=120
fi

# Functions
getfield () {
	echo $1 | awk -F':' '{print $'$2'}'
}

validate_input () {
	# Create input files if necessary
	for ((j=1;j<=$num_of_workloads;j++)); do
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

gen_sub_makeflow () {
	for ((j=1;j<=$num_of_workloads;j++)); do
		# Generate makeflow
		inputdir=$dir_input.$j
		outputdir=$dir_output.$j
		submakeflow=$makeflow.$j
		rm -rf $submakeflow
		echo "# This is an auto-generated makeflow script" >> $submakeflow
		for ((i=1;i<=$num;i++));do
			echo -e "$outputdir/$i.tmp:$inputdir/$i.in\n\t mkdir -p $outputdir; dd if=/dev/zero of=$outputdir/$i.tmp bs=1$unit count=$out; sleep $exe \n" >> $submakeflow
			echo -e "$outputdir/$i.out:$outputdir/$i.tmp\n\t mkdir -p $outputdir; dd if=/dev/zero of=$outputdir/$i.out bs=1$unit count=$out; sleep $exe \n" >> $submakeflow
		done
	done
}

gen_core_makeflow () {
	if [ "$1" = "local" ]; then
		expr="local"
		location="LOCAL"
	elif [ "$1" = "remote" ]; then 
		expr="remote"
		location=""
	else 
		echo "Argument to gen_core_makeflow() should be either \"local\" or \"remote\"!"
		exit 1
	fi
	makeflowscript=$makeflow.$expr
	rm -rf $makeflowscript
	echo "# This is an auto-generated makeflow script" >> $makeflowscript
	for ((j=1;j<=$num_of_workloads;j++)); do
		inputdir=$dir_input.$j
		outputdir=$dir_output.$j
		submakeflow=$makeflow.$j

		makeflowlog="$submakeflow.makeflowlog"
		wqlog="$submakeflow.wqlog"
		makeflowoutput="$submakeflow.stdout.stderr"
		projname=lyu2.$j

		echo -e "$outputdir $makeflowoutput $makeflowlog $wqlog : $inputdir $submakeflow makeflow \n\t $location makeflow -T wq -d all -a -e -N $projname -p -1 -r $retry_max $makeflowargs $submakeflow &> $makeflowoutput\n" >> $makeflowscript
	done

}

gen_makeflows () {
	gen_sub_makeflow
	gen_core_makeflow "local"
	gen_core_makeflow "remote"
}

start_workers () {
	echo Starting $i workers for each workload ...
	for ((j=1;j<=$num_of_workloads;j++)); do
		projname=lyu2.$j
		work_queue_pool -T condor -f -a -N $projname -t 86400 $1
	done
	echo $((i*num_of_workloads)) are started successfully.
}


run_experiment () {
	expr=$1
	if [ "$expr" != "local" ] && [ "$expr" != "remote" ] ; then
		echo "Argument to run_experiment() should be either \"local\" or \"remote\"!"
		exit 1
	fi
	exprresult="$expr"
	rm -rf $exprresult
	mkdir -p $exprresult

	statistics="$proj.$in.$exe.$out.$num.$expr.statistics"
	rm -rf $statistics

	for i in "${workers[@]}"; do
		makeflowscript=$makeflow.$expr

		makeflowlog="$makeflowscript.makeflowlog"
		wqlog="$makeflowscript.wqlog"
		makeflowoutput="$makeflowscript.stdout.stderr"


		if [ "$expr" = "local" ] ; then
			#start_workers $i
			echo Start running the local masters experiment ...
			makeflow -d all -j $num_of_workloads -r $retry_max $makeflowargs $makeflowscript &> $makeflowoutput 
		fi

		if [ "$expr" = "remote" ] ; then
			echo Starting workers to host multiple masters ...
			#work_queue_pool -T condor -f -a -N lyu2.0 -t 86400 $((num_of_workers+5))
			echo $((num_of_workers + 5)) are started successfully.
			#start_workers $i
			echo Start running the remote masters experiment ...
			makeflow -T wq -d all -a -e -N lyu2.0 -r $retry_max $makeflowargs $makeflowscript &> $makeflowoutput 
		fi

		# Get turnaround time
		started=`grep 'STARTED' $makeflowlog | awk '{print $3}'`
		completed=`grep 'COMPLETED' $makeflowlog | awk '{print $3}'`
		duration=`bc <<< "$completed - $started"`

		# Do some statistics
		echo $in $exe $out $num $i $duration >> $statistics

		dir=$in.$exe.$out.$num.$expr.w$i
		rm -rf $dir
		mkdir -p $dir
		
		if [ "$expr" = "remote" ] ; then
			mv $wqlog $dir/
		fi
		mv $makeflowoutput $makeflowlog $dir/

		for ((j=1;j<=$num_of_workloads;j++)); do
			submakeflow=$makeflow.$j

			makeflowlog="$submakeflow.makeflowlog"
			wqlog="$submakeflow.wqlog"
			makeflowoutput="$submakeflow.stdout.stderr"

			mv $makeflowlog $wqlog $makeflowoutput $dir/
		done

		#makeflow -c $makeflow
		#condor_rm -all
		mv $dir $exprresult/
	done

	cp $statistics $makeflow.* $exprresult/
	mv $exprresult $projresult/
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

makeflow="$proj.$in.$exe.$out.$num.makeflow"
projresult="$proj.result"
rm -rf $projresult
mkdir -p $projresult

validate_input

gen_makeflows

#run_experiment "local"

run_experiment "remote"


#gen_plots

# Clean Up
rm -rf $makeflow.* *.statistics

