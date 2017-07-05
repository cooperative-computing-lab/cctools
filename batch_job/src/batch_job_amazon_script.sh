#!/bin/sh

# This script executes a remote job on Amazon EC2.
# We assume that the user has already run makeflow_ec2_setup,
# which creates a VPC, subnet, security group, etc., and drops
# the necessary information into a config file.

# This script loads the config file, creates a VM, loads the
# input files into the VM, runs the task, the removes the output files.
# The VM is automatically destroyed on exit.

# XXX The config file should be configurable.

source ./amazon.config

INSTANCE_ID=X

cleanup () {
	if [ "$INSTANCE_ID" != X ]
	then
		echo "Terminating EC2 instance..."
		aws ec2 terminate-instances --instance-ids $INSTANCE_ID --output text

		# Note that there is no benefit to waiting for instance termination.
	fi
}

run_ssh_cmd () {
	ssh -o StrictHostKeyChecking=no -i $EC2_KEYPAIR_NAME.pem ec2-user@$IP_ADDRESS $1 2> /dev/null
}

get_file_from_server_to_destination () {
	echo "Copying file to $2"
	scp -o StrictHostKeyChecking=no -i $EC2_KEYPAIR_NAME.pem ec2-user@$IP_ADDRESS:~/"$1" $2
}

copy_file_to_server () {
	scp -o StrictHostKeyChecking=no -i $EC2_KEYPAIR_NAME.pem $* ec2-user@$IP_ADDRESS:~
}

trap cleanup EXIT

if [ "$#" -lt 2 ]; then
	echo "Incorrect arguments passed to program"
	echo "Usage: $0 CMD INPUT_FILES OUTPUT_FILES" >&2
	exit 1
fi

# No inputs passed
if [ "$#" -eq 2 ]; then
	CMD=$1
	INPUT_FILES=""
	OUTPUT_FILES=$2
else
	CMD=$1
	INPUT_FILES=$2
	OUTPUT_FILES=$3
fi

echo "Starting EC2 instance..."
INSTANCE_ID=$(aws ec2 run-instances --image-id $EC2_AMI --instance-type $EC2_INSTANCE_TYPE --key-name $EC2_KEYPAIR_NAME --security-group-ids $EC2_SECURITY_GROUP --output text | grep "INSTANCE" | cut -f 8 )

echo "Instance: $INSTANCE_ID"

sleep 5

echo "Waiting for instance to start..."
INSTANCE_STATUS="pending"
while [ "$INSTANCE_STATUS" = "pending" ]; do
	INSTANCE_STATUS=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID --output text | grep "INSTANCE" | cut -f 8 )
	sleep 1
	echo -n .
done

echo "Getting IP address..."
IP_ADDRESS=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID --output text | grep "INSTANCE" | cut -f 16 )

echo "Connecting to instance at $IP_ADDRESS"
tries=30
SUCCESSFUL_SSH=-1
while [ $tries -ne 0 ]
do
	run_ssh_cmd "echo 'Connection to remote server successful'" && SUCCESSFUL_SSH=0 && break
	tries=$(expr $tries - 1)
	sleep 1
	echo -n .
done

# Run rest of ssh commands
if [ $SUCCESSFUL_SSH -eq 0 ]
then
	# Pass input files
	if ! [ -z "$INPUT_FILES" ]; then
		INPUTS="$(echo $INPUT_FILES | sed 's/,/ /g')"
		copy_file_to_server $INPUTS
	fi

	# Run command
	run_ssh_cmd "$CMD"

	# Get output files
	OUTPUTS="$(echo $OUTPUT_FILES | sed 's/,/ /g')"
	get_file_from_server_to_destination $OUTPUTS $OUTPUT_FILES_DESTINATION
fi
