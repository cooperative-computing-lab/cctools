






















# makeflow_ec2_setup(1)

## NAME
**makeflow_ec2_setup** - set up Amazon services for running Makeflow

## SYNOPSIS
**makeflow_ec2_setup _&lt;config-file&gt;_ _&lt;ami&gt;_**

## DESCRIPTION

Before using **makeflow_ec2_setup**, make sure that you install the AWS Command
Line Interface tools, have the **aws** command in your PATH,
and run **aws configure** to set up your secret keys and default region.

**makeflow_ec2_setup** prepares Amazon services for running a Makeflow.
It creates a virtual private cluster, subnets, key pairs, and other
details, and writes these out to a configuration file given by the 
first argument.  The second argument is the Amazon Machine Image (AMI)
that will be used by default in the workflow, if not specified for 
individual jobs.

Once the configuration file is created, you may run **makeflow**
with the **-T amazon** option and give the name of the created
config file with **--amazon-config**.

When complete, you can clean up the virtual cluster and related
items by running **makeflow_ec2_cleanup**

## OPTIONS
None.

## EXAMPLES

```
makeflow_ec2_setup my.config ami-343a694f
makeflow -T amazon --amazon-config my.config example.makeflow
makeflow_ec2_cleanup my.config
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools
