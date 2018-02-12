#!/bin/sh
# 
# File:   amazon_batch_script.sh
# Author: Kyle Sweeney
#
# Created on Jul 7, 2017, 1:48:40 PM
#

if [ -z "$1" ]; then
	echo "Empty number of CPUs requested."
	echo "Correct Ussage: ./makeflow_amazon_batch_setup [desired_num_cpus] [min_number_cpus] [max_number_cpus] (config-output)"
	exit -17
fi

if [ -z "$2" ]; then
	echo "Empty number of CPUs requested."
	echo "Correct Ussage: ./makeflow_amazon_batch_setup [desired_num_cpus] [min_number_cpus] [max_number_cpus] (config-output)"
	exit -17
fi

if [ -z "$3" ]; then
	echo "Empty number of CPUs requested."
	echo "Correct Ussage: ./makeflow_amazon_batch_setup [desired_num_cpus] [min_number_cpus] [max_number_cpus] (config-output)"
	exit -17
fi

cpus=$1
min_cpus=$2
max_cpus=$3
time=$(date +%s)

if [ $cpus -lt $min_cpus ]; then
	echo "Desired number of cpus must be greater than or equal to minimum number of cpus"
	echo "Correct Ussage: ./makeflow_amazon_batch_setup [desired_num_cpus] [min_number_cpus] [max_number_cpus] (config-output)"
        exit -17
fi

if [ $cpus -gt $max_cpus ]; then
	echo "Desired number of cpus must be less than or equal to maximum number of cpus"
        echo "Correct Ussage: ./makeflow_amazon_batch_setup [desired_num_cpus] [min_number_cpus] [max_number_cpus] (config-output)"
        exit -17
fi

if [ $max_cpus -lt $min_cpus ]; then
	echo "Minimum number of cpus must be less than or equal to maximum number of cpus"
        echo "Correct Ussage: ./makeflow_amazon_batch_setup [desired_num_cpus] [min_number_cpus] [max_number_cpus] (config-output)"
        exit -17
fi

if [ -n "$4" ]; then
	outputfile="$4"
else
	outputfile="makeflow_amazon_batch.config"
fi

if [ -e $outputfile ]; then
	echo "File: $outputfile already exists! Please cleanup/delete first"
	exit -12
fi

echo "{" >> $outputfile

#Grabbing the necessary keys
aws_id=$(python -c "import ConfigParser,os; config = ConfigParser.RawConfigParser(); config.readfp(open(os.path.expanduser('~/.aws/credentials'))); print config.get('default','aws_access_key_id')")
aws_key=$(python -c "import ConfigParser,os; config = ConfigParser.RawConfigParser(); config.readfp(open(os.path.expanduser('~/.aws/credentials'))); print config.get('default','aws_secret_access_key')")
aws_reg=$(python -c "import ConfigParser,os; config = ConfigParser.RawConfigParser(); config.readfp(open(os.path.expanduser('~/.aws/config'))); print config.get('default','region')")

echo "\"aws_id\":\"$aws_id\"," >> $outputfile
echo "\"aws_key\":\"$aws_key\"," >> $outputfile
echo "\"aws_reg\":\"$aws_reg\"," >> $outputfile

#echo create the vpc
ec2_vpc_create_response="$(aws ec2 create-vpc --cidr-block 10.0.0.0/16)"
ec2_vpc=$(python -c "import json; print json.loads('''$ec2_vpc_create_response''')['Vpc']['VpcId'];")
#ec2_vpc=$(aws ec2 describe-vpcs --query 'Vpcs[0].VpcId' --output text)
echo "created vpc: $ec2_vpc"

aws ec2 modify-vpc-attribute --vpc-id $ec2_vpc --enable-dns-hostnames
#aws ec2 modify-vpc-attribute --vpc-id $ec2_vpc --enable-dns-support

echo "\"vpc\":\"$ec2_vpc\"," >> $outputfile

#echo create the subnet
ec2_subnet_create_response="$(aws ec2 create-subnet --vpc-id $ec2_vpc --cidr-block 10.0.1.0/24)"
ec2_subnet=$(python -c "import json; print json.loads('''$ec2_subnet_create_response''')['Subnet']['SubnetId'];")
#ec2_subnet=$(aws ec2 describe-subnets --query 'Subnets[0].SubnetId' --output text)
echo "created subnet: $ec2_subnet"

aws ec2 modify-subnet-attribute --subnet-id $ec2_subnet --map-public-ip-on-launch

echo "\"subnet\":\"$ec2_subnet\"," >> $outputfile

#echo create the security group
#ec2_security_group_name="makeflow_ccl_sec_group_$time"
#ec2_security_group_create_response="$(aws ec2 create-security-group --group-name $ec2_security_group_name --description "A Makeflow Security Group" --vpc-id $ec2_vpc)"
#ec2_security_group_id=$(python -c "import json; print json.loads('''$ec2_security_group_create_response''')['GroupId'];")
ec2_security_group_id=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values=$ec2_vpc --query 'SecurityGroups[0].GroupId' --output text)
echo "created security group: $ec2_security_group_id"

aws ec2 authorize-security-group-ingress --group-id $ec2_security_group_id --protocol all --port all

echo "\"sec_group\":\"$ec2_security_group_id\"," >> $outputfile

#create an internet gateway
ec2_gateway=$(aws ec2 create-internet-gateway --query 'InternetGateway.InternetGatewayId' --output text)
echo "created gateway: $ec2_gateway"

aws ec2 attach-internet-gateway --vpc-id $ec2_vpc --internet-gateway-id $ec2_gateway

route_table=$(aws ec2 describe-route-tables --filters Name=vpc-id,Values=$ec2_vpc --query 'RouteTables[0].RouteTableId' --output text)
echo "This is the route table: $route_table"
should_be_true_json=$(aws ec2 create-route --route-table-id $route_table --destination-cidr-block 0.0.0.0/0 --gateway-id $ec2_gateway)

echo "\"gateway\":\"$ec2_gateway\"," >> $outputfile

echo "creating the environment"
env_name="makeflow_ccl_env_$time"
account_num_id=$(aws ec2 describe-security-groups --group-names 'Default' --query 'SecurityGroups[0].OwnerId' --output text) #got from stack overflow: "Quick Way to get AWS Account number from the cli tools?
env_output_response=$(aws batch create-compute-environment --compute-environment-name $env_name --type MANAGED --state ENABLED --compute-resources type=EC2,minvCpus=$min_cpus,maxvCpus=$max_cpus,desiredvCpus=$cpus,instanceTypes=optimal,subnets=$ec2_subnet,securityGroupIds=$ec2_security_group_id,instanceRole=ecsInstanceRole --service-role=arn:aws:iam::$account_num_id:role/service-role/AWSBatchServiceRole)
#echo $batch_compenv_create_response

env_done_check_output="$(aws batch describe-compute-environments --compute-environments $env_name)"
env_done=$(python -c "import json; print json.loads('''$env_done_check_output''')['computeEnvironments'][0]['status'] == 'VALID'")
while [ "$env_done" != "True" ]
do
env_done_check_output="$(aws batch describe-compute-environments --compute-environments $env_name)"
env_done=$(python -c "import json; print json.loads('''$env_done_check_output''')['computeEnvironments'][0]['status'] == 'VALID'")
done

echo "\"env_name\":\"$env_name\"," >> $outputfile

#echo creating the queue
queue_name="makeflow_ccl_queue_$time"
batch_queue_create_response="$(aws --region=$aws_reg batch create-job-queue --state=ENABLED --priority=1 --job-queue-name=$queue_name --compute-environment-order order=1,computeEnvironment=$env_name)"
echo $batch_queue_create_response

queue_done_check_output="$(aws batch describe-job-queues --job-queues $queue_name)"
queue_done=$(python -c "import json; print json.loads('''$queue_done_check_output''')['jobQueues'][0]['status'] == 'VALID'")
while [ "$queue_done" != "True" ]
do
queue_done_check_output="$(aws batch describe-job-queues --job-queues $queue_name)"
queue_done=$(python -c "import json; print json.loads('''$queue_done_check_output''')['jobQueues'][0]['status'] == 'VALID'")
done

echo "created queue: $queue_name"
echo "\"queue_name\":\"$queue_name\"," >> $outputfile

#echo creating the bucket
bucket_pre=$(python -c "print ''.join([chr(ord('a')+int(x)) for x in '$time'])")
echo "created bucket: ccl$bucket_pre" 
bucket_name="ccl$bucket_pre"
make_bucket_response="$(aws s3 mb s3://$bucket_name)"

echo "\"bucket\":\"$bucket_name\"" >> $outputfile

echo "}" >> $outputfile
