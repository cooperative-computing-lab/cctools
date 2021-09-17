






















# condor_submit_makeflow(1)

## NAME
**condor_submit_makeflow** - submit workflow to HTCondor batch system

## SYNOPSIS
**condor_submit_makeflow [options] _&lt;workflow&gt;_**

## DESCRIPTION
**condor_submit_makeflow** submits Makeflow itself as a batch job,
allowing HTCondor to monitor and control the job, so the user does
not need to remain logged in while the workflow runs.  All options
given to condor_submit_makeflow are passed along to the underlying Makeflow.

## EXIT STATUS
On success, returns zero. On failure, returns non-zero.

## EXAMPLE
```
condor_submit_makeflow example.mf

Submitting example.mf as a background job to HTCondor...
Submitting job(s).
1 job(s) submitted to cluster 364.
Makeflow Output : makeflow.364.output
Makeflow Error  : makeflow.364.error
Condor Log      : makeflow.364.condorlog
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools
