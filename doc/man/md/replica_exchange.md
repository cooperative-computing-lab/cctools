






















# replica_exchange(1)

## NAME
**replica_exchange** -  Work Queue application for running replica exchange simulations using ProtoMol

## SYNOPSIS
**replica_exchange [options] _&lt;pdb_file&gt;_ _&lt;psf_file&gt;_ _&lt;par_file&gt;_ _&lt;min_temp&gt;_ _&lt;max_temp&gt;_ _&lt;num_replicas&gt;_**

## DESCRIPTION
**replica_exchange** is a Work Queue application for running replica exchange simulations using the ProtoMol simulation package. The application supports both barrier and non-barrier based runs.

The barrier based run transfers the configuration files and input data for each replica to the connected [work_queue_worker()](work_queue_worker.md) instances, runs the ProtoMol simulation package, and gathers the output, at each Monte Carlo step. It waits for the completion of simulation of all replicas at each step before proceeding to the next step and, therefore, incorporates a barrier at each step. At the end of every step, it randomly picks two neigboring replicas, applies the metropolis criterion, and if it is satisfied, swaps the parameters of the two replicas and continues simulations.

The non-barrier based run is equivalent to the barrier run in the output and results produced. However, it avoids the use of a barrier by running multiple monte carlo steps for each replica until that replica is picked to attempt an exchange. By default, the application will run using this non-barrier implementation.

The **pdb_file**, **psf_file**, and **par_file** arguments specify the input files required for the simulation run. The **min_temp** and **max_temp** specify the temperature range in which the replicas are simulated. The number of replicas simulated is given by **num_replicas**.

**replica_exchange** can be run on any machine accesible to work_queue_worker instances.

## OPTIONS

- **-n** _&lt;name&gt;_<br />Specify a project name for using exclusive work_queue_worker instances.
- **-x** _&lt;filename&gt;_<br />Specify the name of the xyz file for output.
- **-d** _&lt;filename&gt;_<br />Specify the name of the dcd file for output.
- **-m** _&lt;number&gt;_<br />Specify the number of monte carlo steps. Default = 100.
- **-s** _&lt;number&gt;_<br />Specify the number of molecular dynamics steps. Default = 10000.
- **-p** _&lt;path&gt;_<br />Specify path for storing output files.
- **-q**<br />Assign closer temperature values to replicas in the first and last quartile.
- **-i**<br />Assume ProtoMol is installed and available in PATH on worker site.
- **-b**<br />Use barrier in waiting for all replicas to finish their steps before attempting exchange.
- **-l**<br />Print debuggging information.
- **-h**<br />Show this help message.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## ENVIRONMENT VARIABLES

If the cctools are installed in a non-system directory, such as your
home directory, then you must set the **PYTHONPATH** environment
so that the workqueue python module can be found.  For example:

```
% setenv PYTHONPATH $HOME/cctools/lib/python2.4/site-packages
```

## EXAMPLES

To run a replica exchange experiment with 84 replicas in the temperature range 278 to 400K using the sample input files:
```
% replica_exchange ww_exteq_nowater1.pdb ww_exteq_nowater1.psf par_all27_prot_lipid.inp 278 400 84
```

To run a replica exchange experiment, with project name ReplExch, over 250 Monte Carlo steps running 1000 molecular dynamics steps
and involving 84 replicas in the temperature range 278 to 400K using the sample input files:
```
% replica_exchange -N ReplExch -m 250 -s 1000 ww_exteq_nowater1.pdb ww_exteq_nowater1.psf par_all27_prot_lipid.inp 278 400 84
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [uge_submit_workers(1)](uge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools
