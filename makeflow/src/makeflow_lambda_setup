#!/bin/sh

#######################################################################
# This script setups up the AWS environment necessary for the         #
# successful execution of 'makeflow -T lambda'. The steps necessary   #
# for this are:                                                       #
# 1. Creating an Identity and Access Management Role that allows the  #
#    function to access S3 file storage and execute it's code.        #
#                                                                     #
# 2. Deploying a function to the Lambda service that will retrieve    #
#    inputs from S3, run arbitrary shell commands, and put outputs    #
#    back into S3.                                                    #
#                                                                     #
# 3. Creating a bucket in S3 dedicated to storing objects for the     #
#    purposes of makeflow                                             #
#                                                                     #
# This is achieved by calls to the AWS-CLI given user input through   #
# the option flags                                                    #
#######################################################################

SCRIPT_NAME="$0"

REQUIRED_VERSION_STRING="1.10.64"
REQUIRED_VERSION=$(echo "$REQUIRED_VERSION_STRING" | tr -d .)

ROLE_NAME_DEFAULT="Makeflow-Role"
BUCKET_NAME_DEFAULT="$USER-Makeflow-Bucket"
BUCKET_NAME_DEFAULT_STRING="\$USER-Makeflow-Bucket"
FUNCTION_NAME_DEFAULT="Makeflow-Function"
REGION_NAME_DEFAULT="us-east-1"
PROFILE_NAME_DEFAULT="default"
OUTPUT_NAME_DEFAULT="lambda_config.json"

throw_error() {
    echo "$SCRIPT_NAME: $1"
    exit 1
}

print_help() {
    printf "Use: $SCRIPT_NAME [options]\n"
    printf " %-25s AWS Identity and Access Management Role (IAM Role) for Lambda function execution. Creates if necessary. (default is '%s')\n" "-r <role-name>" "$ROLE_NAME_DEFAULT"
    printf " %-25s AWS S3 bucket to hold objects during Lambda function execution. Creates if necessary.       (default is '%s')\n" "-b <bucket-name>" "$BUCKET_NAME_DEFAULT_STRING"
    printf " %-25s AWS Lambda function to execute jobs. Creates if necessary.      (default is '%s')\n" "-f <function-name>" "$FUNCTION_NAME_DEFAULT"
    printf " %-25s Use this AWS Region.      (default is '%s')\n" "-g <region-name>" "$REGION_NAME_DEFAULT"
    printf " %-25s Use this specific profile from your credential file.      (default is '%s')\n" "-p <profile-name>" "$PROFILE_NAME_DEFAULT"
    printf " %-25s Place the output here.      (default is '%s')\n" "-o <file-name>" "$OUTPUT_NAME_DEFAULT"
    printf " %-25s Show this help screen.\n" "-h"
    exit "$1"
}

# Create the role that will be attributed to the Lambda function. This
# gives the Lambda function 'permission' to access S3 and to execute
role_create() {
    # Test if the role already exists by looking for a Amazon Resource
    # Name (ARN) associated with it
    #
    # Perhaps this should not be a fatal error, but for now it is
    ROLE_ARN=$($AWS_CMD iam list-roles | grep "$ROLE_NAME"$ | awk '{print $2}')
    if [ -n "$ROLE_ARN" ]; then
        throw_error "Role '""$ROLE_NAME""' already exists..."
    fi

    # This policy, when attached to the role, gives 'permission' to
    # access S3
    S3_ROLE_POLICY_ARN=arn:aws:iam::aws:policy/AmazonS3FullAccess
    # This policy, when attached to the role, makes the Lambda
    # function 'recognizable' by the Lambda service
    LAMBDA_ROLE_POLICY_ARN=arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

    # The skeleton of the role, to which we will attach the policies
    TEMP_ROLE_POLICY_DOC=/tmp/_temp_role_policy_document.json
    cat > "$TEMP_ROLE_POLICY_DOC" <<- EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

    # Create the role from the skeleton and clean up the skeleton
    ROLE_ARN=$($AWS_CMD iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document file://"$TEMP_ROLE_POLICY_DOC" | grep "$ROLE_NAME"$ | awk '{print $2}')
    STATUS="$?"
    rm "$TEMP_ROLE_POLICY_DOC"
    if [ "$STATUS" != "0" ]; then
        throw_error "Role creation failed"
    fi

    # Attach the S3 policy to the role
    $AWS_CMD iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "$S3_ROLE_POLICY_ARN"
    if [ "$?" != "0" ]; then
        throw_error "Failed to attach Amazon S3 Full Access ARN to role"
    fi

    # Attach the Lambda execution policy to the role
    $AWS_CMD iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "$LAMBDA_ROLE_POLICY_ARN"
    if [ "$?" != "0" ]; then
        throw_error "Failed to attach Amazon Lambda Basic Execution ARN to role"
    fi

    return 0
}

# Deploys the actual function, written in Python 2.7, to the Lambda
# Service
function_create() {
    TEMP_DIR=/tmp
    FUNCTION_SOURCE="$FUNCTION_NAME".py
    FUNCTION_PACKAGE="$FUNCTION_NAME".zip

    if [ -n "$($AWS_CMD lambda list-functions | cut -f6 | grep "$FUNCTION_NAME")" ]; then
        throw_error "Function '""$FUNCTION_NAME""' already exists..."
    fi

    PREV="$(pwd)"
    cd "$TEMP_DIR"

    # This is the Lambda function. 'event' is a JSON object passed to
    # the function by the invoker.
    cat > "$FUNCTION_SOURCE" <<- EOF
import boto3
import subprocess
import os
import shutil

def handler(event, context):
    s3 = boto3.client(service_name='s3', region_name=event["region_name"], config=boto3.session.Config(signature_version='s3v4'))
    # get inputs from S3 into filesystem
    bucket_name = event["bucket_name"]
    bucket_folder = event["bucket_folder"]
    work_dir = os.path.join("/tmp/", bucket_folder)
    os.mkdir(work_dir)
    for input_name in event["input_names"]:
        input_key = os.path.join(bucket_folder, input_name)
        input_path = os.path.join(work_dir, input_name)
        try:
            input_response = s3.get_object(Bucket=bucket_name, Key=input_key)
            try:
                os.makedirs(os.path.dirname(input_path))
            except OSError, e:
                if e.errno != 17:
                    raise
                pass
            input_file = open(input_path, "wb")
            input_file.write(input_response["Body"].read())
            input_file.close()
        except Exception as e:
            return "Error encountered while retrieving input\nbucket_name = {}\nbucket_folder = {}\nwork_dir = {}\ninput_name = {}\ninput_key = {}\ninput_path = {}\nevent = {}\nexception = {}\n".format(bucket_name, bucket_folder, work_dir, input_name, input_key, input_path, event, e)

    # run the command
    subprocess.call("cd "+work_dir+"; {}".format(event["cmd"]), shell=True)

    # put outputs into S3
    for output_name in event["output_names"]:
        output_key = os.path.join(bucket_folder, output_name)
        output_path = os.path.join(work_dir, output_name)
        try:
            output_file = open(output_path, "rb")
            output_response = s3.put_object(Bucket=bucket_name, Key=output_key, Body=output_file)
            output_file.close()
        except Exception as e:
            return "Error encountered while storing output\nbucket_name = {}\nbucket_folder = {}\nwork_dir = {}\noutput_name = {}\noutput_key = {}\noutput_path = {}\nevent = {}\nexception = {}\n".format(bucket_name, bucket_folder, work_dir, output_name, output_key, output_path, event, e)

    shutil.rmtree(work_dir)

    return "Lambda invocation successful\nevent = {}\n".format(event)
EOF

    # This packages the code for deployment
    zip -q "$FUNCTION_PACKAGE" "$FUNCTION_SOURCE"
    # Cleanup the Lambda source
    rm "$FUNCTION_SOURCE"

    # The role creation is asynchronous, so the role may or may not be
    # accessible at this point, but should be eventually. So we will
    # attempt function creation until it succeeds or times out.
    SLEEP_TIME="1"
    STATUS="1"
    while [ "$STATUS" != "0" -a "$SLEEP_TIME" != "64" ]; do
        sleep "$SLEEP_TIME"

        # Attempt to deploy the Lambda function
        $AWS_CMD lambda create-function \
                 --function-name "$FUNCTION_NAME" \
                 --zip-file fileb://"$FUNCTION_PACKAGE" \
                 --role "$ROLE_ARN" \
                 --handler "$FUNCTION_NAME".handler \
                 --runtime python2.7 \
                 --timeout 300 > /dev/null 2> /dev/null

        STATUS="$?"
        SLEEP_TIME=$(expr "$SLEEP_TIME" \* 2)
    done

    # Clean up
    rm "$FUNCTION_PACKAGE"
    cd "$PREV"

    if [ "$STATUS" != "0" ]; then
        throw_error "Failed to create function"
    fi

    return 0
}

bucket_create() {
    # The common error is duplicating a bucket you already own
    EXTANT_BUCKETS=$($AWS_CMD s3 ls | cut -d' ' -f3)
    for BUCKET in $EXTANT_BUCKETS; do
        if [ "$BUCKET" = "$BUCKET_NAME" ]; then
            throw_error "Bucket '$BUCKET_NAME' already belongs to you..."
        fi
    done

    # Create the bucket
    # The namespace for buckets is shared by ALL S3 users, so a
    # non-unique name is a common case of failure.
    $AWS_CMD s3 mb s3://"$BUCKET_NAME" > /dev/null
    if [ "$?" != "0" ]; then
        throw_error "Failed to make bucket '$BUCKET_NAME'"
    fi

    return 0
}

#### MAIN EXECUTION ####

ROLE_NAME="$ROLE_NAME_DEFAULT"
BUCKET_NAME="$BUCKET_NAME_DEFAULT"
FUNCTION_NAME="$FUNCTION_NAME_DEFAULT"
REGION_NAME="$REGION_NAME_DEFAULT"
PROFILE_NAME="$PROFILE_NAME_DEFAULT"
OUTPUT_NAME="$OUTPUT_NAME_DEFAULT"

while getopts hr::b::f::g::p::o:: FLAGNAME
do
    case "$FLAGNAME" in
        h)print_help 0;;
        r)ROLE_NAME="$OPTARG";;
        b)BUCKET_NAME="$OPTARG";;
        f)FUNCTION_NAME="$OPTARG";;
        g)REGION_NAME="$OPTARG";;
        p)PROFILE_NAME="$OPTARG";;
        o)OUTPUT_NAME="$OPTARG";;
        *)print_help 1;;
    esac
done
shift $(($OPTIND -1))

VERSION=$(aws --version 2>&1 | tr / \  | cut -d\  -f2 | tr -d .)
if [ $VERSION -lt $REQUIRED_VERSION ]; then
    throw_error "aws-cli version $REQUIRED_VERSION_STRING or above required"
fi

AWS_CMD="aws --region $REGION_NAME --profile $PROFILE_NAME"

$AWS_CMD s3 ls > /dev/null 2> /dev/null
if [ "$?" != "0" ]; then
    throw_error "Could not run 'aws --region $REGION_NAME --profile $PROFILE_NAME' correctly. Check that it is properly installed and configured"
fi

role_create
function_create
bucket_create

# This prints the json object containing all the relevant
# information. The resulting file is to be interpreted by makeflow for
# execution
printf '{"%s":"%s","%s":"%s","%s":"%s","%s":"%s","%s":"%s","%s":"%s","%s":"%s"}\n' "role_name" "$ROLE_NAME" "bucket_name" "$BUCKET_NAME" "function_name" "$FUNCTION_NAME" "region_name" "$REGION_NAME" "profile_name" "$PROFILE_NAME" "s3_role_policy_arn" "$S3_ROLE_POLICY_ARN" "lambda_role_policy_arn" "$LAMBDA_ROLE_POLICY_ARN" > "$OUTPUT_NAME"

exit 0
