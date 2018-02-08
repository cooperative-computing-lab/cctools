#!/bin/sh

###################################################################
# This script destroys an environment previously created for the  #
# execution of 'makeflow -T lambda'. It does this by:             #
# 1. Deleting the Identity and Access Managmeent Role             #
#                                                                 #
# 2. Deleting the Lambda function                                 #
#                                                                 #
# 3. Deleting the S3 bucket and everything in it                  #
#                                                                 #
# Any combination of these may be preserved through flags         #
###################################################################

SCRIPT_NAME="$0"

REQUIRED_VERSION_STRING="1.10.64"
REQUIRED_VERSION=$(echo "$REQUIRED_VERSION_STRING" | tr -d .)

throw_error() {
    echo "$SCRIPT_NAME: $1"
    exit 1
}

printHelp() {
    printf "Use: $SCRIPT_NAME [options] <config file>\n"
    printf " %-10s Preserve the Identity and Access Management Role (IAM Role).\n" "-r"
    printf " %-10s Preserve the AWS S3 bucket.\n" "-b"
    printf " %-10s Preserve the AWS Lambda function.\n" "-f"
    printf " %-10s Show this help screen.\n" "-h"
    exit "$1"
}

# This naively parses the JSON config file output by
# batch_job_lambda_setup.sh
parseConfigFile() {
    if [ ! -e "$CONFIG_FILE" ]; then
        throw_error "$0: Config file '$CONFIG_FILE' does not exist"
    fi

    ROLE_NAME=""
    BUCKET_NAME=""
    FUNCTION_NAME=""
    REGION_NAME=""
    PROFILE_NAME=""
    OUTPUT_NAME=""
    S3_ROLE_POLICY_ARN=""
    LAMBDA_ROLE_POLICY_ARN=""

    # Turn the commas into newlines for easy 'for-loop' iteration of
    # the JSON-objects
    JSON_PAIRS=$(cat "$CONFIG_FILE" | tr -d {}\" | tr , '\n')
    for PAIR in $JSON_PAIRS; do
        # Seperate the JSON pair via the first ':'. Vulnerable to
        # colons in the keys, but those are programmer specified, so
        # just be careful.
        KEY=$(echo "$PAIR" | cut -f1 -d:)
        VALUE=$(echo "$PAIR" | cut -f2- -d:)

        # Parse the pair
        case "$KEY" in
            "role_name")ROLE_NAME="$VALUE";;
            "bucket_name")BUCKET_NAME="$VALUE";;
            "function_name")FUNCTION_NAME="$VALUE";;
            "region_name")REGION_NAME="$VALUE";;
            "profile_name")PROFILE_NAME="$VALUE";;
            "s3_role_policy_arn")S3_ROLE_POLICY_ARN="$VALUE";;
            "lambda_role_policy_arn")LAMBDA_ROLE_POLICY_ARN="$VALUE";;
            *)echo "$0: Unrecognized key '$KEY' in json file"; exit 1;;
        esac
    done

    if [ -z "$ROLE_NAME" ]; then
        throw_error "$0: Key 'ROLE_NAME' not found in json file"
    fi
    if [ -z "$BUCKET_NAME" ]; then
        throw_error "$0: Key 'BUCKET_NAME' not found in json file"
    fi
    if [ -z "$FUNCTION_NAME" ]; then
        throw_error "$0: Key 'FUNCTION_NAME' not found in json file"
    fi
    if [ -z "$REGION_NAME" ]; then
        throw_error "$0: Key 'REGOIN_NAME' not found in json file"
    fi
    if [ -z "$PROFILE_NAME" ]; then
        throw_error "$0: Key 'PROFILE_NAME' not found in json file"
    fi
    if [ -z "$S3_ROLE_POLICY_ARN" ]; then
        throw_error "$0: Key 'S3_ROLE_POLICY_ARN' not found in json file"
    fi
    if [ -z "$LAMBDA_ROLE_POLICY_ARN" ]; then
        throw_error "$0: Key 'LAMBDA_ROLE_POLICY_ARN' not found in json file"
    fi

    AWS_CMD="aws --profile $PROFILE_NAME --region $REGION_NAME"
}

# Permanently removes the Role associated with the Lambda function
deleteRole() {
    # First we must detach the policies from the role

    # Detach the S3 policy
    $AWS_CMD iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "$S3_ROLE_POLICY_ARN"
    if [ "$?" != "0" ]; then
       throw_error "Couldn't detach the role policy responsible for S3 manipulation from the role '$ROLE_NAME'"
    fi

    # Detach the Lambda execution policy
    $AWS_CMD iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "$LAMBDA_ROLE_POLICY_ARN"
    if [ "$?" != "0" ]; then
       throw_error "Couldn't detach the role policy responsible for Lambda execution from the role '$ROLE_NAME'"
    fi

    # Finally delete the role
    $AWS_CMD iam delete-role --role-name "$ROLE_NAME"
    if [ "$?" != "0" ]; then
        throw_error "Couldn't delete the role '$ROLE_NAME'"
    fi
}

# Permanently delete the bucket and its contents from S3
deleteBucket() {
    $AWS_CMD s3 rb s3://"$BUCKET_NAME" --force > /dev/null
    if [ "$?" != "0" ]; then
        throw_error "Couldn't delete the bucket '$BUCKET_NAME'"
    fi
}

# Permanently delete the Lambda function from the Lambda service
deleteFunction() {
    $AWS_CMD lambda delete-function --function-name "$FUNCTION_NAME"
    if [ "$?" != "0" ]; then
        throw_error "Couldn't delete the function '$FUNCTION_NAME'"
    fi
}

#### Main Execution ####

PRESERVE_ROLE=false
PRESERVE_BUCKET=false
PRESERVE_FUNCTION=false

while getopts hrbf FLAGNAME
do
    case "$FLAGNAME" in
        h)printHelp "$0" 0;;
        r)PRESERVE_ROLE=true;;
        b)PRESERVE_BUCKET=true;;
        f)PRESERVE_FUNCTION=true;;
        *)printHelp "$0" 1;;
    esac
done
shift $(($OPTIND -1))

if [ -z "$1" ]; then
    printHelp "$0" 1
fi

VERSION=$(aws --version 2>&1 | tr / \  | cut -d\  -f2 | tr -d .)
if [ $VERSION -lt $REQUIRED_VERSION ]; then
    throw_error "aws-cli version $REQUIRED_VERSION_STRING or above required"
fi

CONFIG_FILE="$1"

parseConfigFile

if ! "$PRESERVE_ROLE"; then
    deleteRole
fi

if ! "$PRESERVE_BUCKET"; then
    deleteBucket
fi

if ! "$PRESERVE_FUNCTION"; then
    deleteFunction
fi

exit 0
