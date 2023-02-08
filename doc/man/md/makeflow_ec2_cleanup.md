






















# makeflow_ec2_cleanup(1)

## NAME
**makeflow_ec2_cleanup** - clean up Amazon services after running Makeflow

## SYNOPSIS
**makeflow_ec2_cleanup _&lt;config-file&gt;_**

## DESCRIPTION

**makeflow_ec2_cleanup** cleans up the virtual private cluster,
subnets, and other details that were created by **makeflow_ec2_setup**
and stored in the config file passed on the command line.

It is possible that cleanup may fail, if the cluster or other elements
are still in use, and display a message from AWS.  In this case, make
sure that you have terminated any workflows running in the cluster and
try again.  If cleanup still fails, you may need to use the AWS console
to view and delete the relevant items listed in the config file.

## OPTIONS
None.

## EXAMPLES

```
makeflow_ec2_cleanup my.config ami-343a694f
makeflow -T amazon --amazon-config my.config example.makeflow
makeflow_ec2_cleanup my.config
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools
