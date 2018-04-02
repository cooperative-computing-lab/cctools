#!/bin/sh
# 
# File:   amazon_batch_script.sh
# Author: Kyle Sweeney
#
# Created on Jul 7, 2017, 1:48:40 PM
#



#AmazonS3FullAccess
#AmazonEC2ContainerServiceFullAccess
#AmazonEC2ContainerServiceRole
#}
#    "Version": "2012-10-17",
#    "Statement": [
#        {
#            "Sid": "Stmt1497492706000",
#            "Effect": "Allow",
#            "Action": [
#                "batch:*"
#            ],
#            "Resource": [
#                "*"
#            ]
#        }
#    ]
#}

load_fieldINI()
{
	grep $1 $2 | cut -d "=" -f 2 | tr -d ' '
}

load_fieldJSON()
{
	grep $1 $2 | cut -d ":" -f 2 | tr -d ' ' | tr -d ','
}

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


#taken from professor Thain's makeflow_ec2_setup
if which aws >/dev/null 2>&1
then
	echo "found aws, moving on to config file check"
else
	echo "aws not found in path, please include it"
	exit -9
fi

if [ -f ~/.aws/config ]; then
	echo "found config file, moving on to credentials file check"
else
	echo "aws not configured: please configure before running the script"
	exit -10
fi
if [ -f ~/.aws/credentials ]; then
	echo "found credentials file!"
else
	echo "aws not configured: please configure before running the script"
	exit -10
fi

if aws batch list-jobs --job-queue HighPriority > /dev/null 2>&1
then
	echo "config and credentials configured correctly, proceeding to setup"
else
	echo "configuration and credentials files not properly set, please reconfig aws and try again"
	exit -33
fi

#check_arn_iam=$(aws --output json iam get-role --role-name AWSBatchServiceRole)
#if [[ $check_arn_iam = *"CreateDate"*  ]]; then
#	echo "credentials have IAM with AWSBatchServiceRole set up, good!"
#else
#	echo "current user doesn't have AWSBatchServiceRole attached to credientials: please fix in amazon console"
#	exit -44
#fi

#Grabbing the necessary keys
aws_id=$(load_fieldINI aws_access_key_id ~/.aws/credentials)
aws_key=$(load_fieldINI aws_secret_access_key ~/.aws/credentials)
aws_reg=$(load_fieldINI region ~/.aws/config)

echo "{" >> $outputfile

echo "\"aws_id\":\"$aws_id\"," >> $outputfile
echo "\"aws_key\":\"$aws_key\"," >> $outputfile
echo "\"aws_reg\":\"$aws_reg\"," >> $outputfile

#echo create the vpc
ec2_vpc="$(aws --output text --query 'Vpc.VpcId' ec2 create-vpc --cidr-block 10.0.0.0/16)"
echo "created vpc: $ec2_vpc"

aws --output json ec2 modify-vpc-attribute --vpc-id $ec2_vpc --enable-dns-hostnames
#aws --output json ec2 modify-vpc-attribute --vpc-id $ec2_vpc --enable-dns-support

echo "\"vpc\":\"$ec2_vpc\"," >> $outputfile

#echo create the subnet
ec2_subnet="$(aws --output text --query 'Subnet.SubnetId' ec2 create-subnet --vpc-id $ec2_vpc --cidr-block 10.0.1.0/24)"
echo "created subnet: $ec2_subnet"

aws --output json ec2 modify-subnet-attribute --subnet-id $ec2_subnet --map-public-ip-on-launch

echo "\"subnet\":\"$ec2_subnet\"," >> $outputfile

#echo create the security group
ec2_security_group_id=$(aws --output json ec2 describe-security-groups --filters Name=vpc-id,Values=$ec2_vpc --query 'SecurityGroups[0].GroupId' --output text)
echo "created security group: $ec2_security_group_id"

aws --output json ec2 authorize-security-group-ingress --group-id $ec2_security_group_id --protocol all --port all

echo "\"sec_group\":\"$ec2_security_group_id\"," >> $outputfile

#create an internet gateway
ec2_gateway=$(aws --output json ec2 create-internet-gateway --query 'InternetGateway.InternetGatewayId' --output text)
echo "created gateway: $ec2_gateway"

aws --output json ec2 attach-internet-gateway --vpc-id $ec2_vpc --internet-gateway-id $ec2_gateway

route_table=$(aws --output json ec2 describe-route-tables --filters Name=vpc-id,Values=$ec2_vpc --query 'RouteTables[0].RouteTableId' --output text)
echo "This is the route table: $route_table"
should_be_true_json=$(aws --output json ec2 create-route --route-table-id $route_table --destination-cidr-block 0.0.0.0/0 --gateway-id $ec2_gateway)

echo "\"gateway\":\"$ec2_gateway\"," >> $outputfile

echo "creating the environment"
env_name="makeflow_ccl_env_$time"
account_num_id=$(aws ec2 describe-security-groups --group-names 'Default' --query 'SecurityGroups[0].OwnerId' --output text) #got from stack overflow: "Quick Way to get AWS Account number from the cli tools?
env_output_response=$(aws --output json batch create-compute-environment --compute-environment-name $env_name --type MANAGED --state ENABLED --compute-resources type=EC2,minvCpus=$min_cpus,maxvCpus=$max_cpus,desiredvCpus=$cpus,instanceTypes=optimal,subnets=$ec2_subnet,securityGroupIds=$ec2_security_group_id,instanceRole=ecsInstanceRole --service-role=arn:aws:iam::$account_num_id:role/service-role/AWSBatchServiceRole)

env_done="$(aws --output text --query 'computeEnvironments[0].status' batch describe-compute-environments --compute-environments $env_name)"
while [ "$env_done" != "VALID" ]
do
	env_done="$(aws --output text --query 'computeEnvironments[0].status' batch describe-compute-environments --compute-environments $env_name)"
done

echo "\"env_name\":\"$env_name\"," >> $outputfile

#echo creating the queue
queue_name="makeflow_ccl_queue_$time"
batch_queue_create_response="$(aws --output json --region=$aws_reg batch create-job-queue --state=ENABLED --priority=1 --job-queue-name=$queue_name --compute-environment-order order=1,computeEnvironment=$env_name)"
echo $batch_queue_create_response

queue_done="$(aws --output text --query 'jobQueues[0].status' batch describe-job-queues --job-queues $queue_name)"
while [ "$queue_done" != "VALID" ]
do
queue_done="$(aws --output text --query 'jobQueues[0].status' batch describe-job-queues --job-queues $queue_name)"
done

echo "created queue: $queue_name"
echo "\"queue_name\":\"$queue_name\"," >> $outputfile

#echo creating the bucket
bucket_name="ccl-$time"
make_bucket_response="$(aws --output json s3 mb s3://$bucket_name)"
echo "created bucket: ccl-$time" 

echo "\"bucket\":\"$bucket_name\"" >> $outputfile

echo "}" >> $outputfile
