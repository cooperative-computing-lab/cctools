#!/bin/bash
ulimit -c 400000000

# Benchmark Parameters

# Name of this benchmark. The generated makeflow would be $proj.makeflow.  The
# project name returned by work_queue_status would be $user-$proj-$worker.
# $user is the unix login username and $worker is the number of workers are
# provided for the corresponding run.
proj="load1"

# Workload format: "input_size:execution_time:output_size:num_of_tasks". Note
# that the execution time is in seconds. And the unit for input/output files
# can be set by the "unit" global variable below.
workload="5:10:5:800"
unit="M"

# The directory to store input files
dir_input="input"

# If yes, all the tasks would share the same input, otherwise every task has
# its independing input. This parameter can be used to test the worker caching
# behavior.
shared_input="no"

# Additional arguments added to makeflow.  Format: "symbolic_name:real
# arguments". All results related to one "real argument" would be stored under
# the directory that contains the correspoinding symbolic name
#makeflow_args=( "fcfs:-z fcfs"
#				"fd:-z fd" )
makeflow_args=( "ad:-Z adaptive" )

# Max retry times when makeflow jobs fail
retry_max=30

# Number of workers for each run
workers=( 100 )

# Capacity tolerance for work queue master
capacity_tolerance=5
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
	mkdir -p $dir_input
	cd $dir_input
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
}

gen_makeflow () {
	# Generate makeflow
	rm -rf $makeflow
	echo "# This is an auto-generated makeflow script" >> $makeflow
	if [ "$shared_input" = "yes" ]; then
		for ((i=1;i<=$num;i++));do
			echo -e "$i.out:$dir_input/0.in\n\t dd if=/dev/zero of=$i.out bs=1$unit count=$out; sleep $exe \n" >> $makeflow
		done
	else
		for ((i=1;i<=$num;i++));do
			echo -e "$i.out:$dir_input/$i.in\n\t dd if=/dev/zero of=$i.out bs=1$unit count=$out; sleep $exe \n" >> $makeflow
		done
	fi
}

gen_wqlog_gnuplot () {
	# Generate gnuplot script
	echo "set terminal png
	set output \"$wqlogplot\"

	set title \"In: $in $unit.B. Exe: $exe sec Out: $out $unit.B. Tasks: $num Workers $1 \"

	set xdata time
	set timefmt \"%s\"
	set format x \"%M:%S\"

	set xlabel \"Time\"
	set ylabel \"Count\"

	set yrange [0:${yrange_max}]

	plot \"$makeflow.wqlog.queue\" using (\$2 / 1000000):(\$3+\$4+\$5) title \"workers connected\" with lines lt 1 lw 3 , \\
		 \"$makeflow.wqlog.queue\" using (\$2 / 1000000):6 title \"task running\" with lines lt 2 lw 3, \\
		 \"$makeflow.wqlog.queue\" using (\$2 / 1000000):(\$15 * 100) title \"calculated efficiency\" with lines lt 3 lw 3, \\
		 \"$makeflow.wqlog.queue\" using (\$2 / 1000000):(\$16 * 100) title \"idle percentage\" with lines lt 4 lw 3, \\
		 \"$makeflow.wqlog.queue\" using (\$2 / 1000000):17 title \"estimated capacity\" with lines lt 5 lw 3, \\
		 \"$makeflow.wqlog.queue\" using (\$2 / 1000000):18 title \"reliable capacity\" with lines lt 6 lw 3, \\
		 \"$makeflow.wqlog.queue\" using (\$2 / 1000000):19 title \"workers joined\" with lines lt 7 lw 3, \\
		 \"$makeflow.wqlog.queue\" using (\$2 / 1000000):20 title \"workers removed\" with lines lt 8 lw 3
	" > $wqloggnuplot
}

gen_runtime_gnuplot () {
	rm -rf $runtimegnuplot $runtimeplot
	echo "set terminal png
	set output \"$runtimeplot\"

	set title \"In: $in $unit.B. Exe: $exe sec Out: $out $unit.B. Tasks: $num\"

	set xlabel \"Workers\"
	set ylabel \"Time (sec)\"

	" > $runtimegnuplot

	echo -n "plot " >> $runtimegnuplot

	i=1
	for item in "${makeflow_args[@]}"; do
		arg_name=${item%%:*}
		arg=${item#*:}

		resultdir=$in.$exe.$out.$num.$arg_name
		statistics="$proj.$in.$exe.$out.$num.$arg_name.statistics"

		if [ "$i" -ne "${#makeflow_args[@]}" ]; then
			echo -e "\t\"$resultdir/$statistics\" using 5: (\$6 / 1000000) title \"$arg_name\" with lines lt $i lw 3, \\" >> $runtimegnuplot
		else 
			echo -e "\t\"$resultdir/$statistics\" using 5: (\$6 / 1000000) title \"$arg_name\" with lines lt $i lw 3 " >> $runtimegnuplot
		fi
		i=$((i+1))
	done
}

run_experiments () {
	mkdir -p $projresult

	# In bash 4, you can use associative arry to handle hash tables,
	# But in our systems, we only have bash 3.
	for item in "${makeflow_args[@]}"; do
		# Note that ":" could appear in the makeflow arguments.
		arg_name=${item%%:*}
		arg=${item#*:}

		resultdir="$in.$exe.$out.$num.$arg_name"
		plotdir="$resultdir/plots"
		rm -rf $resultdir
		mkdir -p $resultdir

		statistics="$proj.$in.$exe.$out.$num.$arg_name.statistics"
		rm -rf $statistics

		for i in "${workers[@]}"; do
			name=lyu2-$proj-$i
			#work_queue_pool -T condor -f -a -C cclweb01.cse.nd.edu:9097 -N $name $i
			#makeflow -T wq -d all -a -e -C cclweb01.cse.nd.edu:9097 -N $name -r $retry_max $arg $makeflow &> $makeflow.stdout.stderr
			work_queue_pool -T condor -f -a -N $name $i
			makeflow -T wq -d all -a -e -N $name -r $retry_max $arg -t $capacity_tolerance $makeflow &> $makeflow.stdout.stderr

			# Get turnaround time
			started=`grep 'STARTED' $makeflowlog | awk '{print $3}'`
			completed=`grep 'COMPLETED' $makeflowlog | awk '{print $3}'`
			duration=`bc <<< "$completed - $started"`
			# Get observed efficiendy
			efficiency=`tail -1 $wqlog.queue | awk '{print $15}'`
			# Do some statistics
			echo $in $exe $out $num $i $duration $efficiency >> $statistics

			dir=$in.$exe.$out.$num.$arg_name.w$i
			rm -rf $dir
			mkdir -p ${dir}
			
			mv $makeflow.stdout.stderr $makeflowlog $wqlog $dir/
			cp $makeflow $dir/

			makeflow -c $makeflow
			condor_rm -all
			mv $dir $resultdir/
			sleep 3
		done

		mv $statistics $resultdir/
		mv $resultdir $projresult/
	done
}

gen_plots () {
	# Generate graphs
	cd $projresult
	gen_runtime_gnuplot
	gnuplot < $runtimegnuplot

	for item in "${makeflow_args[@]}"; do
		arg_name=${item%%:*}
		arg=${item#*:}

		resultdir="$in.$exe.$out.$num.$arg_name"
		cd $resultdir
		plotdir="plots"
		rm -rf $plotdir
		mkdir -p $plotdir

		for i in "${workers[@]}"; do
			dir=$in.$exe.$out.$num.$arg_name.w$i
			cd $dir

			cat $wqlog | grep QUEUE > $wqlog.queue
			gen_wqlog_gnuplot $i
			gnuplot < $wqloggnuplot

			mv $wqlogplot ../$plotdir/$i.png
			cd ..
		done
		cd ..
	done
	cd ..
}

# Main Program	

in=`getfield $workload 1`
exe=`getfield $workload 2`
out=`getfield $workload 3`
num=`getfield $workload 4`

projresult="$proj.result"

makeflow="$proj.$in.$exe.$out.$num.makeflow"
makeflowlog="$makeflow.makeflowlog"

wqlog="$makeflow.wqlog"
wqloggnuplot="$wqlog.gnuplot"
wqlogplot="$wqlog.png"

runtimegnuplot="$proj.$in.$exe.$out.$num.runtimes.gnuplot"
runtimeplot="$proj.$in.$exe.$out.$num.runtimes.png"

if [ $unit == "M" ]; then
	unit_size=$((1024*1024))
elif [ $unit == "K" ]; then
	unit_size=$((1024))
else 
	unit_size=1
fi

input_size=$(($in * $unit_size))

validate_input

gen_makeflow

run_experiments

gen_plots

# Clean Up
rm -rf $wqloggnuplot $makeflow

