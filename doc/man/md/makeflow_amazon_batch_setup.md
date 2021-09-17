






















# makeflow_amazon_batch_setup(1)

## NAME
**makeflow_amazon_batch_setup** - set up Amazon services for running Makeflow

## SYNOPSIS
**makeflow_amazon_batch_setup _&lt;desired-num-cores&gt;_ _&lt;min-num-cores&gt;_ _&lt;max-num-cores&gt;_ _&lt;config-file&gt;_ **

## DESCRIPTION

**makeflow_ec2_setup** prepares Amazon services for running a Makeflow. It creates a virtual private cluster, subnets, key pairs, and other details, and writes these out to a configuration file given by the fourth argument. If the fourth argument is not provided, then it will be writen out to **makeflow_amazon_batch.config** in the director this script is called.  The first argument is the desired number of cores for the environment. The second argument is the minimum number of cores acceptable for the environment. The third argument is the maximum number of cores that the environment should ever have at its disposal.

Once the configuration file is created, you may run **makeflow**
with the **-T amazon-batch** option and give the name of the created
config file with **--amazon-batch-config**.

When complete, you can clean up the virtual cluster and related
items by running **makeflow_amazon_batch_cleanup**

## PRE-RUN SETUP

Before using **makeflow_amazon_batch_setup**, make sure that you install the AWS Command
Line Interface tools, have the **aws** command in your PATH,
and run **aws configure** to set up your secret keys and default region.

Next, ensure that the IAM associated with the secret keys passed to **aws configure** has the following policies attached to it:

*AmazonEC2FullAccess

*AmazonS3FullAccess

*AWSBatchServiceRole

*AWSBatchFullAccess

After ensuring that, the script requires that you have the following Roles enabled on your account:

*AWSBatchServiceRole -- This should have an arn which looks like **arn:aws:iam:ACCOUNT_ID_NUM:role/service-role/AWSBatchServiceRole** which has the policy AWSBatchServiceRole attached.

*ecsInstanceRole -- This should have an arn which looks like **arn:aws:iam:ACCOUNT_ID_NUM:role/ecsInstanceRole** which has the policy AmazonEC2ContainerServiceforEC2Role attached.

The easiest way to create these roles is to enter the Amazon Batch dashboard and create an environment via their wizard. In the "Serice role" and "Instance role" fields, have "Create new role" selected.

When these steps have been accomplished, then the script will run correctly and setup for you a batch environment for you to run your makeflow in. 


## OPTIONS
None.

## EXAMPLES

```
makeflow_amazon_batch_setup 3 2 4 my.config
makeflow -T amazon-batch --amazon-batch-config=my.config --amazon-ami=USER_PROVIDED_ECS_IMAGE_ARN example.makeflow
makeflow_amazon_batch_cleanup my.config
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools
