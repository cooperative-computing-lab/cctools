#!/bin/sh

show_help() 
{
    echo "Use: torque_submit_workers [options] <servername> <port> <num-workers>"
    echo "where options are:"
    echo "  -M <name>        Name of the preferred master for worker."
    echo "  -N <name>        Same as -M (backwards compatibility)."
    echo "  -c <num>         Set the number of cores each worker should use (0=auto). (default=1)"
    echo "  -C <catalog>     Set catalog server to <catalog>. <catalog> format: HOSTNAME:PORT."
    echo "  -t <time>        Abort after this amount of idle time. (default=900s)."
    echo "  -d <subsystem>   Enable debugging on worker for this subsystem (try -d all to start)."
    echo "  -w <size>        Set TCP window size."
    echo "  -i <time>        Set initial value for backoff interval when worker fails to connect to a master. (default=1s)"
    echo "  -b <time>        Set maxmimum value for backoff interval when worker fails to connect to a master. (default=60s)"
    echo "  -z <size>        Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)"
    echo "  -A <arch>        Set architecture string for the worker to report to master instead of the value in uname."
    echo "  -O <os>          Set operating system string for the worker to report to master instead of the value in uname."
    echo "  -s <path>        Set the location for creating the working directory of the worker."
    echo "  -j               Use job array to submit workers."
    echo "  -f               SSH port forwarding enabled."
    echo "  -n <nodes>       Number of compute nodes that this job will reserve."
    echo "  -W <numworkers>  Number of worker processes to start on each compute node."    
    echo "  -p <parameters>  Torque qsub parameters."
    echo "  -h               Show this help message."
    exit 1
}

# An tutorial for submitting workers with SSH port forwarding to work around firewalls, 
# compute nodes without Internet connectivity, and similar situations.
# --> denotes a command that you need to run.
# The master is workstation.university.edu at port 9999.  
# The head node is headnode.cluster.com.
# 
# Consider a case where workstation.university.edu can connect to headnode.cluster.com, but not vice versa.
# On workstation.university.edu, I run the command:
# --> ssh -o ServerAliveInterval=180 -N -f -R9998:workstation.university.edu:9999 headnode.cluster.com
# This redirects host "localhost" port 9998 on headnode.cluster.com to host 
# "workstation.university.edu" port 9999 (numbers chosen to be different for clarity).
# 
# Now I submit a worker with the command:
# --> [pbs|torque|sge]_submit_workers -f headnode.cluster.com 9998
# The worker process will run the following command before starting the worker:
# ssh -o ServerAliveInterval=180 -N -f -L9998:localhost:9998 headnode.cluster.com
# This redirects host "localhost" port 9998 on the compute node to "localhost" port 9998 on headnode.cluster.com,
# which *then* gets redirected to host "workstation.univeresity.edu" port 9999.
# The worker should then connect to the master successfully..
# 
# Now consider where headnode.cluster.com can connect to workstation.university.edu but not vice versa.
# Repeat the above command, but on headnode.cluster.com I run the command:
# --> ssh -o ServerAliveInterval=180 -N -f -L9998:workstation.university.edu:9999 workstation.university.edu
# This redirects host "localhost" port 9998 on headnode.cluster.com to host "workstation.university.edu" port 9999.
# Note now -L creates a "localhost" port LOCALLY while -R creates a "localhost" port REMOTELY.

arguments=""
use_auto=0
use_jobarray=0
parameters=""
port_forward=0
nodes=1
numworkers=1

while getopts aM:N:c:C:t:d:w:i:b:z:A:O:s:fn:W:jp:h opt 
do
    case "$opt" in
        a)  arguments="$arguments -a"; use_auto=1;; #left here for backwards compatibility
        M)  arguments="$arguments -M $OPTARG"; use_auto=1;;
        N)  arguments="$arguments -M $OPTARG"; use_auto=1;;
        c)  cores=$OPTARG;;
        C)  arguments="$arguments -C $OPTARG";;
        t)  arguments="$arguments -t $OPTARG";;
        d)  arguments="$arguments -d $OPTARG";;
        w)  arguments="$arguments -w $OPTARG";;
        i)  arguments="$arguments -i $OPTARG";;
        b)  arguments="$arguments -b $OPTARG";;
        z)  arguments="$arguments -z $OPTARG";;
        A)  arguments="$arguments -A $OPTARG";;
        O)  arguments="$arguments -O $OPTARG";;
        s)  arguments="$arguments -s $OPTARG";;
        j)  use_jobarray=1;;
        f)  port_forward=1;;
        n)  nodes=$OPTARG;;
        W)  numworkers=$OPTARG;;
        p)  parameters="$parameters $OPTARG";;
        h)  show_help;;
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

# For SSH port forwarding, establish a connection from the head node (i.e. where this script is run) to the master
if [ $port_forward == 1 ]; then
    if [ `ps aux | grep $USER | grep ServerAlive | grep $port | grep -v grep | awk '{print \$2}' | wc -l` -eq 0 ] ; then
        ssh -x -o ServerAliveInterval=180 -N -f -L$port:$host:$port $host
    fi
fi

#================================#
#| Lee-Ping's Modifications for |#
#| Best Performance on Clusters |#
#|  using Torque: vsp-compute,  |#
#|      Certainty, biox3        |#
#================================#
# Host-specific parameters to qsub.
# Note that these variables
# are evaluated on the head node!

if [[ $HOSTNAME =~ "biox3" ]] ; then
    QSUB_EXTRAS="#PBS -q MP
#PBS -V
#PBS -l walltime=168:00:00"
    SELF_SSH="y"
    SCRATCH_DIR=/tmp/$USER
    if [ "x$cores" == "x" ] ; then cores=$(( 16 / $numworkers )) ; fi
elif [[ $HOSTNAME =~ "vsp-compute" ]] ; then
    SCRATCH_DIR=/opt/scratch/$USER
    if [ "x$cores" == "x" ] ; then cores=$(( 24 / $numworkers )) ; fi
elif [[ $HOSTNAME =~ "certainty" ]] ; then
    QSUB_EXTRAS="-l walltime=24:00:00"
    SCRATCH_DIR=/state/partition1/$USER/\$HOSTNAME
    NODEFILE_DIR=/scratch/$USER
    if [ "x$cores" == "x" ] ; then cores=$(( 24 / $numworkers )) ; fi
fi
arguments="$arguments --cores $cores"; 

# This variable will prevent leeping-workers 
# folders from being strewn everywhere.
WORKER_DIR=$HOME/temp

mkdir -p $WORKER_DIR/${USER}-workers/$$
cd $WORKER_DIR/${USER}-workers/$$
cp $worker .

#====================================#
#| This script is the only one that |#
#|      goes three layers deep.     |#
#====================================#
# First create the script that will
# be submitted to the scheduler.
cat >worker.sh <<EOF
#!/bin/bash
#PBS -l nodes=$nodes:ppn=$(( cores * numworkers ))
$QSUB_EXTRAS

export PATH=$(dirname $(which qsub)):\$PATH

#===========================================#
#| If submitting a multiple-node job, then |#
#| the second-layer script is broadcast to |#
#| each allocated node using "pbsdsh".     |#
#===========================================#
if [ $nodes -gt 1 ] ; then
    pbsdsh -u -v $PWD/worker1.sh
else
    $PWD/worker1.sh
fi
EOF

#====================================================#
#| Create the second layer script.  This gives us   |#
#| the option of whether we should SSH back into    |#
#| the local node before running the worker, which  |#
#| allows us to surpass some buggy resource limits. |#
#====================================================#
cat <<EOF > worker1.sh
#!/bin/bash

# Execute the third layer with an optional self-SSH.
if [ "x$SELF_SSH" == "x" ] ; then
    $PWD/worker2.sh
else
    ssh \$HOSTNAME "$PWD/worker2.sh \$\$"
fi
EOF

#=======================================#
#|    Create the third layer script.   |#
#| This actually launches the workers. |#
#=======================================#
cat <<EOF > worker2.sh
#!/bin/bash

# Load environment variables.
. /etc/profile
. /etc/bashrc
. $HOME/.bashrc

# Limit core dump size.
ulimit -c 0

# This function makes the script kill itself if:
# 1) the second layer stops running (i.e. job deleted by scheduler)
# 2) there are no more workers (i.e. idle timeout)
waitkill(){
    while sleep 1 ; do 
        kill -0 \$1 2> /dev/null || break
        [ \$( ps xjf | grep work_queue_worker | grep -v grep | wc -l ) -gt 0 ] || break
    done
    kill -TERM -\$\$
};

# Go into the directory where the worker program is.
cd $PWD

# Set environment variables.
export OMP_NUM_THREADS=$cores
export MKL_NUM_THREADS=$cores
export CORES_PER_WORKER=$cores
export _CONDOR_SCRATCH_DIR=$SCRATCH_DIR
mkdir -p \$_CONDOR_SCRATCH_DIR

# Create the PBS Node File
if [ "x$NODEFILE_DIR" != "x" ] ; then
    export PBS_NODEFILE=$NODEFILE_DIR/pbs_nodefile.\$HOSTNAME
    rm -f \$PBS_NODEFILE
    for i in \`seq $(( cores * numworkers ))\` ; do
        echo \$HOSTNAME >> \$PBS_NODEFILE
    done
fi

# Optional SSH port forwarding
if [ $port_forward == 1 ]; then
    if [ \`ps aux | grep $USER | grep ServerAlive | grep $port | grep -v grep | awk '{print \$2}' | wc -l\` -eq 0 ] ; then
        ssh -x -o ServerAliveInterval=180 -N -f -L$port:localhost:$port $HOSTNAME
    fi
fi

waitkill \$1 &

# Actually execute the workers.
for i in \`seq $numworkers\`; do
    if [ $port_forward == 1 ]; then
        ./work_queue_worker -d all $arguments localhost $port &
    else
        ./work_queue_worker -d all $arguments $host $port &
    fi
done

wait
EOF

chmod 755 worker.sh worker1.sh worker2.sh

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
