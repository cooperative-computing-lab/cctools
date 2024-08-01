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
cmd_retry()\n\
{\n\
	retry=0\n\
	/bin/bash -c \"$1\"\n\
	while [[ \"$?\" -ne 0 && \"$retry\" -lt 5 ]]\n\
	do \n\
		echo \"$1 failed $retry times, will retry in 10 seconds\"\n\
		retry=$((retry+1))\n\
		sleep 10\n\
		/bin/bash -c \"$1\"\n\
	done \n\
\n\
	if [ \"$retry\" -eq 5 ]\n\
	then\n\
		echo \"$1 failed too many times, $job_id failed\"\n\
		echo $job_id, failed_${2}, $? >> kubectl_failed.log\n\
	fi\n\
}\n\
\n\
# update the log file from inside the container\n\
update_log()\n\
{\n\
    cmd_retry \"kubectl exec $pod_id -- sh -c \\\" echo $1 >> ${pod_id}.log\\\"\" \"exec\"\n\
}\n\
\n\
create_pod()\n\
{\n\
    cmd_retry \"kubectl create -f $pod_id.json --validate=false\" \"create\"\n\
}\n\
\n\
transfer_inps()\n\
{\n\
	if [ ! -f throughput.log ]\n\
	then\n\
		cmd_retry \"touch throughput.log\" \"touch\"\n\
	fi\n\
	for i in $(echo $inps | sed \"s/,/ /g\")\n\
    do\n\
		echo \"kubectl cp $i $pod_id:/$i\"\n\
		size=$(wc -c $i | awk '{print $1}')\n\
		start_time=$(date +%s)\n\
       		cmd_retry \"kubectl cp $i $pod_id:/$i\" \"cp\"\n\
		end_time=$(date +%s)\n\
	    	dur=$((end_time-start_time))\n\
		echo \"The size of $i is $size byte, and it is transferred in $dur milliseconds\" >> throughput.log\n\
    done\n\
	update_log \"$job_id,inps_transferred,$(TZ=UTC date +\\\"%H%M%S\\\")\"\n\
}\n\
\n\
exec_cmd()\n\
{\n\
	echo \"Execut $cmd in $pod_id\"\n\
\n\
    cmd_retry \"kubectl exec $pod_id -- sh -c \\\"$cmd\\\"\"\n\
\n\
    if [ \"$?\" -eq 0 ]\n\
    then \n\
		update_log \"$job_id,exec_success,$(TZ=UTC date +\\\"%H%M%S\\\")\"\n\
    else\n\
		update_log \"$job_id,job_failed,$(TZ=UTC date +\\\"%H%M%S\\\")\"\n\
		exit 1\n\
    fi\n\
}\n\
\n\
transfer_oups() \n\
{\n\
    for i in $(echo $oups | sed \"s/,/ /g\")\n\
    do\n\
        cmd_retry \"kubectl cp $pod_id:$i $i\" \"cp\"\n\
    done\n\
	update_log \"$job_id,oups_transferred,$(TZ=UTC date +\\\"%H%M%S\\\")\"\n\
}\n\
\n\
run_task()\n\
{\n\
	transfer_inps\n\
    exec_cmd\n\
    transfer_oups\n\
	update_log \"$job_id,job_done,$(TZ=UTC date +\\\"%H%M%S\\\")\"\n\
	cmd_retry \"kubectl cp $pod_id:${pod_id}.log ${job_id}.log.k8s\" \"cp\"\n\
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
# vim: set noexpandtab tabstop=8:\n\
";
