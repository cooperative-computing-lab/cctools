#!/bin/sh
# 
# File:   amazon_batch_script.sh
# Author: Kyle Sweeney
#
# Created on Jul 7, 2017, 1:48:40 PM
#

cpus=$1
min_cpus=$2
max_cpus=$3
time=$(date +%s)

outputfile="makeflow_amazon_batch.config"
echo "{" >> $outputfile

#Grabbing the necessary keys
aws_id=$(python -c "import ConfigParser,os; config = ConfigParser.RawConfigParser(); config.readfp(open(os.path.expanduser('~/.aws/credentials'))); print config.get('default','aws_access_key_id')")
aws_key=$(python -c "import ConfigParser,os; config = ConfigParser.RawConfigParser(); config.readfp(open(os.path.expanduser('~/.aws/credentials'))); print config.get('default','aws_secret_access_key')")
aws_reg=$(python -c "import ConfigParser,os; config = ConfigParser.RawConfigParser(); config.readfp(open(os.path.expanduser('~/.aws/config'))); print config.get('default','region')")

echo "\"aws_id\":\"$aws_id\"," >> $outputfile
echo "\"aws_key\":\"$aws_key\"," >> $outputfile
echo "\"aws_reg\":\"$aws_reg\"," >> $outputfile

echo create the vpc
ec2_vpc_create_response="$(aws ec2 create-vpc --cidr-block 10.0.0.0/16)"
ec2_vpc=$(python -c "import json; print json.loads('''$ec2_vpc_create_response''')['Vpc']['VpcId'];")

echo "\"vpc\":\"$ec2_vpc\"," >> $outputfile

echo create the subnet
ec2_subnet_create_response="$(aws ec2 create-subnet --vpc-id=$ec2_vpc --cidr-block 10.0.1.0/24)"
ec2_subnet=$(python -c "import json; print json.loads('''$ec2_subnet_create_response''')['Subnet']['SubnetId'];")

echo "\"subnet\":\"$ec2_subnet\"," >> $outputfile

echo create the security group
ec2_security_group_name="makeflow_ccl_sec_group_$time"
ec2_security_group_create_response="$(aws ec2 create-security-group --group-name $ec2_security_group_name --description "A Makeflow Security Group" --vpc-id $ec2_vpc)"
ec2_security_group_id=$(python -c "import json; print json.loads('''$ec2_security_group_create_response''')['GroupId'];")

echo "\"sec_group\":\"$ec2_security_group_name\"," >> $outputfile

echo creating the environment
env_name="makeflow_ccl_env_$time"
account_num_id=$(aws ec2 describe-security-groups --group-names 'Default' --query 'SecurityGroups[0].OwnerId' --output text) #got from stack overflow: "Quick Way to get AWS Account number from the cli tools?
batch_compenv_create_response="$(aws --region=$aws_reg batch create-compute-environment --compute-environment-name $env_name --type MANAGED --state ENABLED --compute-resources type=EC2,minvCpus=$min_cpus,maxvCpus=$max_cpus,desiredvCpus=$cpus,instanceTypes=optimal,subnets=$ec2_subnet,securityGroupIds=$ec2_security_group_id,instanceRole=ecsInstanceRole --service-role=arn:aws:iam::$account_num_id:role/service-role/AWSBatchServiceRole)"

env_done_check_output="$(aws batch describe-compute-environments --compute-environments $env_name)"
env_done=$(python -c "import json; print json.loads('''$env_done_check_output''')['computeEnvironments'][0]['status'] == 'VALID'")
while [ "$env_done" != "True" ]
do
env_done_check_output="$(aws batch describe-compute-environments --compute-environments $env_name)"
env_done=$(python -c "import json; print json.loads('''$env_done_check_output''')['computeEnvironments'][0]['status'] == 'VALID'")
done

echo "\"env_name\":[$env_name]," >> $outputfile

echo creating the queue
queue_name="makeflow_ccl_queue_$time"
batch_queue_create_response="$(aws --region=$aws_reg batch create-job-queue --state=ENABLED --priority=1 --job-queue-name=$queue_name --compute-environment-order order=1,computeEnvironment=$env_name)"

queue_done_check_output="$(aws batch describe-job-queues --job-queues $queue_name)"
queue_done=$(python -c "import json; print json.loads('''$queue_done_check_output''')['jobQueues'][0]['status'] == 'VALID'")
while [ "$queue_done" != "True" ]
do
queue_done_check_output="$(aws batch describe-job-queues --job-queues $queue_name)"
queue_done=$(python -c "import json; print json.loads('''$queue_done_check_output''')['jobQueues'][0]['status'] == 'VALID'")
done

echo "\"queue_name\":\"$queue_name\"" >> $outputfile

echo "}" >> $outputfile
