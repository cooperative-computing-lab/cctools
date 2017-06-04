"\
#!/bin/bash\n\
\n\
pod_id=$1\n\
job_id=$2\n\
inps=$3\n\
cmd=$4\n\
oups=$5\n\
\n\
# update the log file from inside the container\n\
update_log()\n\
{\n\
    kubectl exec $pod_id -- bash ${job_id}_update_log.sh $1\n\
}\n\
\n\
create_pod()\n\
{\n\
    kubectl create -f $pod_id.json\n\
\n\
	# generate script for updating the log from inside the pod \n\
    echo -e \"echo \\\"\\$1\\\" >> $pod_id.log \" > ${job_id}_update_log.sh\n\
    chmod +x ${job_id}_update_log.sh\n\
	status=$(kubectl get pods $pod_id | awk \'{if (NR != 1) {print $3}}\')\n\
	while [ \"Running\" != \"$status\" ]	\n\
	do\n\
		echo \"The status of container is $status\"\n\
		sleep 1	\n\
		status=$(kubectl get pods $pod_id | awk \'{if (NR != 1) {print $3}}\')\n\
	done\n\
\n\
    kubectl cp ${job_id}_update_log.sh $pod_id:/\n\
    rm ${job_id}_update_log.sh\n\
\n\
    update_log \"$job_id,pod_created\" \n\
}\n\
\n\
transfer_inps()\n\
{\n\
    for i in $(echo $inps | sed \"s/,/ /g\")\n\
    do\n\
		echo \"kubectl cp $i $pod_id:/\"\n\
        kubectl cp $i $pod_id:/\n\
    done\n\
    update_log \"$job_id,inps_transferred\"\n\
}\n\
\n\
exec_cmd()\n\
{\n\
	echo \"Execut $cmd in $pod_id\"\n\
    echo \"$cmd\" > ${job_id}_cmd.sh \n\
    chmod +x ${job_id}_cmd.sh\n\
    kubectl cp ${job_id}_cmd.sh $pod_id:/\n\
    rm ${job_id}_cmd.sh\n\
\n\
    kubectl exec $pod_id -- sh -c \"./${job_id}_cmd.sh\"\n\
\n\
    if [ $? -eq 0 ]\n\
    then \n\
        update_log \"$job_id,exec_success\"\n\
    else\n\
        update_log \"$job_id,exec_failed\"\n\
    fi\n\
}\n\
\n\
transfer_oups() \n\
{\n\
    for i in $(echo $oups | sed \"s/,/ /g\")\n\
    do\n\
        kubectl cp $pod_id:$i $i\n\
    done\n\
    update_log \"$job_id,oups_transferred\"\n\
}\n\
\n\
main()\n\
{\n\
    create_pod\n\
    transfer_inps\n\
    exec_cmd\n\
    transfer_oups\n\
    update_log \"$job_id,job_done\" \n\
}\n\
\n\
main\n\
# vim: set noexpandtab tabstop=4:\n\
";
