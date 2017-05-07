#!/bin/bash

pod_id=$1
job_id=$2
inps=$3
cmd=$4
oups=$5

# update the log file from inside the container
update_log()
{
    kubectl exec $pod_id -- ${job_id}_update_log.sh $1
}

create_pod()
{
    kubectl create $pod_id -f $pod_id.json
    update_log "$job_id pod_created" 
}

transfer_inps()
{
    for i in $(echo $inps | sed "s/,/ /g")
    do
        kubectl cp $i $pod_id:/
    done
    update_log "$job_id inps_transferred"
}

exec_cmd()
{
    echo "$cmd" > ${job_id}_cmd.sh 
    chmod +x ${job_id}_cmd.sh
    kubectl cp ${job_id}_cmd.sh $pod_id:/
    kubectl rm ${job_id}_cmd.sh

    kubectl exec $pod_id -- sh -c ${job_id}_cmd.sh

    if [ $? -eq 0 ]
    then 
        update_log "$job_id exec_success"
    else
        update_log "$job_id exec_failed"
    fi
}

transfer_oups() 
{
    for i in $(echo $oups | sed "s/,/ /g")
    do
        kubectl cp $pod_id:$i $i
    done
    update_log "$job_id oups_transferred"
}

main()
{
    # generate script for updating the log from inside the pod 
    echo -e "echo \"\$1\" > $pod_id.log " > ${job_id}_update_log.sh
    chmod +x ${job_id}_update_log.sh
    kubectl cp ${job_id}_update_log.sh $pod_id:/
    rm ${job_id}_update_log.sh

    create_pod
    transfer_inps
    exec_cmd
    transfer_oups
    update_log "$job_id job_done" 
}

main
# vim: set noexpandtab tabstop=4:
