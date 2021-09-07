






















# makeflow_mpi_starter(1)

## NAME
**makeflow_mpi_starter** - mpi wrapper program for makeflow and workqueue

## SYNOPSIS
**makeflow_mpi_starter [options]**

## DESCRIPTION

**makeflow_mpi_starter** is an MPI wrapper program that will start Makeflow and
WorkQueue on the nodes allocated to it. It is intended as a simple, easy way for 
users to take advantage of MPI-based system while using Makeflow and WorkQueue. To
use it, the user simply needs to call it as one would a regular MPI program. For
the program to work, cctools needs to be configured with **--with-mpi-path**.

## OPTIONS
When **makeflow_mpi_starter** is ran without arguments, it will attempt to execute the
workflow specified by the **Makeflow** dagfile.

### Commands

- **-m**,**--makeflow-arguments=_&lt;option&gt;_**<br />Options to pass to makeflow, such as dagfile, etc
- **-p**,**--makeflow-port=_&lt;port&gt;_**<br />The port for Makeflow to use when communicating with workers
- **-q**,**--workqueue-arguments=_&lt;option&gt;_**<br />Options to pass to work_queue_worker
- **-c**,**--copy-out=_&lt;location&gt;_**<br />Where to copy out all files produced
- **-d**,**--debug=_&lt;debugprefix&gt;_**<br />Base Debug file name
- **-h**,**--help**<br />Print out this help


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Run with debugging:
```
mpirun -np $NUM_PROC makeflow_mpi_starter -m "Makeflow.mf" -d mydebug
```

Run makeflow with custom port:
```
mpirun -np $NUM_PROC makeflow_mpi_starter -m "Makeflow.mf" -d mydebug -p 9001
```

Run makeflow with garbage collection
```
mpirun -np $NUM_PROC makeflow_mpi_starter -m "-gall Makeflow.mf" -d mydebug
```

Example SGE submission script
```

#!/bin/csh

#$ -pe mpi-* 120         
#$ -q debug              
#$ -N mpi_starter_example     

module load ompi

mpirun -np $NSLOTS makeflow_mpi_starter -m "Makeflow.mf" -d mydebug
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


SEE_ALSO_WORKQUEUE

CCTools 7.3.2 FINAL
