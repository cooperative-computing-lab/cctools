include(manual.h)dnl
HEADER(makeflow_mpi_starter)

SECTION(NAME)
BOLD(makeflow_mpi_starter) - mpi wrapper program for makeflow and workqueue

SECTION(SYNOPSIS)
CODE(makeflow_mpi_starter [options])

SECTION(DESCRIPTION)

BOLD(makeflow_mpi_starter) is an MPI wrapper program that will start Makeflow and
WorkQueue on the nodes allocated to it. It is intended as a simple, easy way for 
users to take advantage of MPI-based system while using Makeflow and WorkQueue. To
use it, the user simply needs to call it as one would a regular MPI program. For
the program to work, cctools needs to be configured with CODE(--with-mpi-path).

SECTION(OPTIONS)
When CODE(makeflow_mpi_starter) is ran without arguments, it will attempt to execute the
workflow specified by the BOLD(Makeflow) dagfile.

SUBSECTION(Commands)
OPTIONS_BEGIN
OPTION_ARG(m, makeflow-arguments, option)Options to pass to makeflow, such as dagfile, etc
OPTION_ARG(p, makeflow-port, port)The port for Makeflow to use when communicating with workers
OPTION_ARG(q, workqueue-arguments, option)Options to pass to work_queue_worker
OPTION_ARG(c, copy-out, location)Where to copy out all files produced
OPTION_ARG(d, debug, debugprefix)Base Debug file name
OPTION_FLAG(h,help)Print out this help
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Run with debugging:
LONGCODE_BEGIN
mpirun -np $NUM_PROC makeflow_mpi_starter -m "Makeflow.mf" -d mydebug
LONGCODE_END

Run makeflow with custom port:
LONGCODE_BEGIN
mpirun -np $NUM_PROC makeflow_mpi_starter -m "Makeflow.mf" -d mydebug -p 9001
LONGCODE_END

Run makeflow with garbage collection
LONGCODE_BEGIN
mpirun -np $NUM_PROC makeflow_mpi_starter -m "-gall Makeflow.mf" -d mydebug
LONGCODE_END

Example SGE submission script
LONGCODE_BEGIN

#!/bin/csh

#$ -pe mpi-* 120         
#$ -q debug              
#$ -N mpi_starter_example     

module load ompi

mpirun -np $NSLOTS makeflow_mpi_starter -m "Makeflow.mf" -d mydebug
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

SEE_ALSO_WORKQUEUE

FOOTER
