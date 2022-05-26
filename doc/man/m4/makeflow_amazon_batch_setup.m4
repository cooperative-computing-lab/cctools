include(manual.h)dnl
HEADER(makeflow_amazon_batch_setup)

SECTION(NAME)
BOLD(makeflow_amazon_batch_setup) - set up Amazon services for running Makeflow

SECTION(SYNOPSIS)
CODE(makeflow_amazon_batch_setup PARAM(desired-num-cores) PARAM(min-num-cores) PARAM(max-num-cores) PARAM(config-file) )

SECTION(DESCRIPTION)

BOLD(makeflow_ec2_setup) prepares Amazon services for running a Makeflow. It creates a virtual private cluster, subnets, key pairs, and other details, and writes these out to a configuration file given by the fourth argument. If the fourth argument is not provided, then it will be writen out to CODE(makeflow_amazon_batch.config) in the director this script is called.  The first argument is the desired number of cores for the environment. The second argument is the minimum number of cores acceptable for the environment. The third argument is the maximum number of cores that the environment should ever have at its disposal.

Once the configuration file is created, you may run CODE(makeflow)
with the CODE(-T amazon-batch) option and give the name of the created
config file with CODE(--amazon-batch-config).

When complete, you can clean up the virtual cluster and related
items by running CODE(makeflow_amazon_batch_cleanup)

SECTION(PRE-RUN SETUP)

Before using CODE(makeflow_amazon_batch_setup), make sure that you install the AWS Command
Line Interface tools, have the CODE(aws) command in your PATH,
and run CODE(aws configure) to set up your secret keys and default region.

Next, ensure that the IAM associated with the secret keys passed to CODE(aws configure) has the following policies attached to it:

*AmazonEC2FullAccess

*AmazonS3FullAccess

*AWSBatchServiceRole

*AWSBatchFullAccess

After ensuring that, the script requires that you have the following Roles enabled on your account:

*AWSBatchServiceRole -- This should have an arn which looks like CODE(arn:aws:iam:ACCOUNT_ID_NUM:role/service-role/AWSBatchServiceRole) which has the policy AWSBatchServiceRole attached.

*ecsInstanceRole -- This should have an arn which looks like CODE(arn:aws:iam:ACCOUNT_ID_NUM:role/ecsInstanceRole) which has the policy AmazonEC2ContainerServiceforEC2Role attached.

The easiest way to create these roles is to enter the Amazon Batch dashboard and create an environment via their wizard. In the "Serice role" and "Instance role" fields, have "Create new role" selected.

When these steps have been accomplished, then the script will run correctly and setup for you a batch environment for you to run your makeflow in. 


SECTION(OPTIONS)
None.

SECTION(EXAMPLES)

LONGCODE_BEGIN
makeflow_amazon_batch_setup 3 2 4 my.config
makeflow -T amazon-batch --amazon-batch-config=my.config --amazon-ami=USER_PROVIDED_ECS_IMAGE_ARN example.makeflow
makeflow_amazon_batch_cleanup my.config
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER
