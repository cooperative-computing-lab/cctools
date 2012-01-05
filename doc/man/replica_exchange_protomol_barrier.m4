include(manual.h)dnl
HEADER(replica_exchange_protomol_barrier)

SECTION(NAME) 
BOLD(replica_exchange_protomol_barrier) -  Work Queue application for running the barrier implementation of replica exchange simulations using ProtoMol

SECTION(SYNOPSIS)
CODE(BOLD(replica_exchange_protomol_barrier [options] PARAM(pdb_file) PARAM(psf_file) PARAM(par_file) PARAM(min_temp) PARAM(max_temp) PARAM(num_replicas)))

SECTION(DESCRIPTION)
BOLD(replica_exchange_protomol_barrier) is a Work Queue application for running the barrier implementation of replica exchange simulations using the ProtoMol simulation package. At each Monte Carlo step, it transfers the configuration files and input data for each replica to the connected MANPAGE(work_queue_worker) instances, runs the ProtoMol simulation package, and gathers the output. It waits for the completion of simulation of all replicas at each step before proceeding to the next step and, therefore, incorporates a barrier at each step. At the end of every step, it randomly picks two neigboring replicas, applies the metropolis criterion, and if it is satisfied, swaps the parameters of the two replicas and continues simulations.

The BOLD(pdb_file), BOLD(psf_file), and BOLD(par_file) arguments specify the input files required for the simulation run. The BOLD(min_temp) and BOLD(max_temp) specify the temperature range in which the replicas are simulated. The number of replicas simulated is given by BOLD(num_replicas). 

BOLD(replica_exchange_protomol_barrier) can be run on any machine accesible to work_queue_worker instances. 

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_PAIR(-n, name)Specify a project name for using exclusive work_queue_worker instances.
OPTION_PAIR(-x, filename)Specify the name of the xyz file for output.
OPTION_PAIR(-d, filename)Specify the name of the dcd file for output.
OPTION_PAIR(-m, number)Specify the number of monte carlo steps. Default = 100.
OPTION_PAIR(-s, number)Specify the number of molecular dynamics steps. Default = 10000.
OPTION_PAIR(-p, path)Specify path for storing output files.
OPTION_ITEM(-q)Assign closer temperature values to replicas in the first and last quartile.
OPTION_ITEM(-l)Print debuggging information.
OPTION_ITEM(-h)Show this help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(ENVIRONMENT VARIABLES)

If the cctools are installed in a non-system directory, such as your
home directory, then you must set the CODE(PYTHONPATH) environment
so that the workqueue python module can be found.  For example:

LONGCODE_BEGIN
% setenv PYTHONPATH $HOME/cctools/lib/python2.4/site-packages
LONGCODE_END

SECTION(EXAMPLES)

To run a replica exchange experiment with 84 replicas in the temperature range 278 to 400K using the sample input files:
LONGCODE_BEGIN
% replica_exchange_protomol_barrier ww_exteq_nowater1.pdb ww_exteq_nowater1.psf par_all27_prot_lipid.inp 278 400 84
LONGCODE_END

To run a replica exchange experiment, with project name ReplExch, over 250 Monte Carlo steps running 1000 molecular dynamics steps
and involving 84 replicas in the temperature range 278 to 400K using the sample input files:
LONGCODE_BEGIN
% replica_exchange_protomol_barrier -N ReplExch -m 250 -s 1000 ww_exteq_nowater1.pdb ww_exteq_nowater1.psf par_all27_prot_lipid.inp 278 400 84
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE

FOOTER

