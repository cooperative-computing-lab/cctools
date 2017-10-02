"\
#!/bin/bash\n\
\n\
action=$1\n\
pod_id=$2\n\
job_id=$3\n\
inps=$4\n\
cmd=$5\n\
oups=$6\n\
\n\
# update the log file from inside the container\n\
update_log()\n\
{\n\
    kubectl exec $pod_id -- bash ${job_id}_update_log.sh $1\n\
}\n\
\n\
create_pod()\n\
{\n\
    kubectl create -f $pod_id.json --validate=false\n\
}\n\
\n\
transfer_inps()\n\
{\n\
    # generate script for updating the log from inside the pod \n\
    echo -e \"echo \\\"\\$1\\\" >> $pod_id.log \" > ${job_id}_update_log.sh\n\
    chmod +x ${job_id}_update_log.sh\n\
	kubectl cp ${job_id}_update_log.sh $pod_id:/\n\
    rm ${job_id}_update_log.sh\n\
\n\
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
    # echo \"$cmd\" > ${job_id}_cmd.sh \n\
    # chmod +x ${job_id}_cmd.sh\n\
    # kubectl cp ${job_id}_cmd.sh $pod_id:/\n\
    # rm ${job_id}_cmd.sh\n\
\n\
    # kubectl exec $pod_id -- sh -c \"./${job_id}_cmd.sh\"\n\
\n\
    kubectl exec $pod_id -- sh -c \"$cmd\"\n\
\n\
    if [ $? -eq 0 ]\n\
    then \n\
        update_log \"$job_id,exec_success\"\n\
    else\n\
        update_log \"$job_id,job_failed\"\n\
		exit 1\n\
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
run_task()\n\
{\n\
	transfer_inps\n\
    exec_cmd\n\
    transfer_oups\n\
    update_log \"$job_id,job_done\"\n\
}\n\
\n\
main()\n\
{\n\
	if [ \"$action\" == \"create\" ]\n\
	then\n\
		create_pod\n\
	else \n\
		run_task\n\
	fi\n\
}\n\
\n\
main\n\
# vim: set noexpandtab tabstop=4:\n\
";
