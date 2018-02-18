#!/bin/sh
#
# File: amazon_batch_cleanup_script.sh
# Author: Kyle Sweeney
#
# Created on Jul 17, 2017
#

configFile=$1


if [ ! -e $configFile ]; then
	echo "Config file not present! Please provide it to run this script"
	exit 1
fi

load_field()
{
	grep $1 $configFile | cut -d ":" -f 2 | tr -d '"' | tr -d ','
}

#ec2_vpc=$(python -c "import json; print json.load(open('$configFile','r'))['vpc'];")
#ec2_subnet=$(python -c "import json; print json.load(open('$configFile','r'))['subnet'];")
#ec2_security_group_id=$(python -c "import json; print json.load(open('$configFile','r'))['sec_group'];")
#env_name=$(python -c "import json; print json.load(open('$configFile','r'))['env_name'];")
#queue_name=$(python -c "import json; print json.load(open('$configFile','r'))['queue_name'];")
#bucket_name=$(python -c "import json; print json.load(open('$configFile','r'))['bucket'];")
#gateway=$(python -c "import json; print json.load(open('$configFile','r'))['gateway'];")

ec2_vpc=$(load_field vpc)
ec2_subnet=$(load_field subnet)
ec2_security_group_id=$(load_field sec_group)
env_name=$(load_field env_name)
queue_name=$(load_field queue_name)
bucket_name=$(load_field bucket)
gateway=$(load_field gateway)


#disabling jobqueues
echo "Deleting queue"
aws batch update-job-queue --job-queue $queue_name --state DISABLED

#queue_done_check_output="$(aws --output text --query 'jobQueues[0].status' batch describe-job-queues --job-queues $queue_name)"
#queue_done=$(python -c "import json; k=json.loads('''$queue_done_check_output'''); print k['jobQueues'][0]['state'] != 'ENABLED' and k['jobQueues'][0]['status'] == 'VALID';")
#queue_done=$queue_done_check_output
#while [ "$queue_done" != "VALID" ]
#do
#queue_done_check_output="$(aws --output text --query 'jobQueues[0].status' batch describe-job-queues --job-queues $queue_name)"
#queue_done=$(python -c "import json; k=json.loads('''$queue_done_check_output'''); print k['jobQueues'][0]['state'] != 'ENABLED' and k['jobQueues'][0]['status'] == 'VALID';")
#queue_done=$queue_done_check_output
#done

queue_status="$(aws --output text --query 'jobQueues[0].status' batch describe-job-queues --job-queues $queue_name)"
queue_state="$(aws --output text --query 'jobQueues[0].state' batch describe-job-queues --job-queues $queue_name)"

while ! [ "$queue_status" == "VALID" -a "$queue_state" != "ENABLED"  ]
do
	queue_status="$(aws --output text --query 'jobQueues[0].status' batch describe-job-queues --job-queues $queue_name)"
	queue_state="$(aws --output text --query 'jobQueues[0].state' batch describe-job-queues --job-queues $queue_name)"
done

#deleting all jobqueues
aws batch delete-job-queue --job-queue $queue_name

queue_done_check_output="$(aws --output text batch describe-job-queues --job-queues $queue_name)"
#queue_done=$(python -c "import json; k=json.loads('''$queue_done_check_output'''); print len(k['jobQueues']) == 0;")
queue_done=$queue_done_check_output
while [ ! -z "$queue_done" ]
do
queue_done_check_output="$(aws --output text batch describe-job-queues --job-queues $queue_name)"
#queue_done=$(python -c "import json; k=json.loads('''$queue_done_check_output'''); print len(k['jobQueues']) == 0;")
queue_done=$queue_done_check_output
done



#disabling the environment
echo "Deleting environments"
aws batch update-compute-environment --compute-environment $env_name --state DISABLED

env_status="$(aws --output text --query 'computeEnvironments[0].status' batch describe-compute-environments --compute-environments $env_name)"
env_state="$(aws --output text --query 'computeEnvironments[0].state' batch describe-compute-environments --compute-environments $env_name)"

while ! [ "$env_status" == "VALID" -a "$env_state" != "ENABLED"  ]
do
#env_done_check_output="$(aws --output text batch describe-compute-environments --compute-environments $env_name)"
#env_done=$(python -c "import json; k=json.loads('''$env_done_check_output'''); print k['computeEnvironments'][0]['state'] != 'ENABLED' and k['computeEnvironments'][0]['status'] == 'VALID';")
#env_done=$env_done_check_output
env_status="$(aws --output text --query 'computeEnvironments[0].status' batch describe-compute-environments --compute-environments $env_name)"
env_state="$(aws --output text --query 'computeEnvironments[0].state' batch describe-compute-environments --compute-environments $env_name)"


done


#deleting the environment
aws batch delete-compute-environment --compute-environment $env_name

env_done_check_output="$(aws --output text batch describe-compute-environments --compute-environments $env_name)"
#env_done=$(python -c "import json; k=json.loads('''$env_done_check_output'''); print len(k['computeEnvironments']) == 0;")
env_done=$env_done_check_output
while [ ! -z "$env_done" ]
do
env_done_check_output="$(aws --output text batch describe-compute-environments --compute-environments $env_name)"
#env_done=$(python -c "import json; k=json.loads('''$env_done_check_output'''); print len(k['computeEnvironments']) == 0;")
env_done=$env_done_check_output
done

#delete the internet gateway
echo "Deleting gateway"
aws ec2 detach-internet-gateway --vpc-id $ec2_vpc --internet-gateway-id $gateway
aws ec2 delete-internet-gateway --internet-gateway-id $gateway

#delete the security group
#gets handled by the deletion of the vpc and subnets
#aws ec2 delete-security-group --group-id $ec2_security_group_id

#delete the subnet
#echo "THis is the subnet variable value: $ec2_subnet"
echo "Deleting subnet"
aws ec2 delete-subnet --subnet-id $ec2_subnet

#delete the vpc
echo "deleting vpc"
aws ec2 delete-vpc --vpc-id $ec2_vpc

#delete the bucket
echo "deleting bucket"
aws s3 rb s3://$bucket_name --force
