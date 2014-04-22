#!/bin/sh

show_help() 
{
	echo "Use: torque_submit_workers [options] <servername> <port> <num-workers>"
	echo "where options are:"
	echo "  -a               Enable auto mode."
	echo "  -s               Run as a shared worker."
	echo "  -N <name>        Preferred project name for worker."
	echo "  -C <catalog>     Set catalog server to <catalog>. <catalog> format: HOSTNAME:PORT."
	echo "  -t <time>        Abort after this amount of idle time. (default=900s)"
	echo "  -j               Use job array to submit workers."	
	echo "  -p <parameters>  torque qsub parameters."
	echo "  -h               Show this help message."
	exit 1
}

arguments=""
use_auto=0
use_jobarray=0
parameters=""

while getopts aC:hjN:p:st: opt 
do
	case "$opt" in
		a)  arguments="$arguments -a"; use_auto=1;;
		C)  arguments="$arguments -C $OPTARG";;
		h)  show_help;;
		j)  use_jobarray=1;;
		N)  arguments="$arguments -N $OPTARG";;
		p)  parameters="$parameters $OPTARG";;
		s)  arguments="$arguments -s";;
		t)  arguments="$arguments -t $OPTARG";;
		\?) show_help;;
	esac
done

shift $(expr $OPTIND - 1)

if [ $use_auto = 0 ]; then
    if [ X$3 = X ]
    then
	show_help	
    fi
    host=$1
    port=$2
    count=$3
else
    if [ X$1 = X ]
    then
	show_help	
    fi
    host=
    port=
    count=$1
fi

worker=`which work_queue_worker 2>/dev/null`
if [ $? != 0 ]
then
	echo "$0: please add 'work_queue_worker' to your PATH."
	exit 1
fi

qsub=`which qsub 2>/dev/null`
if [ $? != 0 ]
then
	echo "$0: please add 'qsub' to your PATH."
	exit 1
fi

mkdir -p ${USER}-workers
cd ${USER}-workers
cp $worker .

#PBS -l nodes=1:ppn=1

cat >worker.sh <<EOF
#!/bin/sh
#PBS -N worker.sh
#PBS -q GPU
#PBS -l walltime=72:00:00

#export _CONDOR_SCRATCH_DIR=/hsgs/projects/pande/leeping/scratch/\$HOSTNAME
export _CONDOR_SCRATCH_DIR=/tmp/leeping/\$HOSTNAME
mkdir -p \$_CONDOR_SCRATCH_DIR

./work_queue_worker -d all $arguments $host $port
EOF

chmod 755 worker.sh

if [ $use_jobarray = 1 ]
then
	qsub -t 1-$count:1 -d `pwd` $parameters worker.sh	
else 
	for n in `seq 1 $count`
	do
		qsub -d `pwd` $parameters worker.sh
	done
fi
return_status=$?

exit $return_status
