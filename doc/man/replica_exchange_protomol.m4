include(manual.h)dnl
HEADER(replica_exchange_protomol)

SECTION(NAME) 
BOLD(replica_exchange_protomol) -  Work Queue application for running replica exchange simulations using ProtoMol

SECTION(SYNOPSIS)
CODE(BOLD(replica_exchange_protomol [options] PARAM(pdb_file) PARAM(psf_file) PARAM(par_file) PARAM(min_temp) PARAM(max_temp) PARAM(num_replicas)))

SECTION(DESCRIPTION)

BOLD(replica_exchange_protomol) is a Work Queue application for running replica exchange simulations
using the ProtoMol simulation package. It creates configuration files for the replicas at each Monte Carlo
step, transfers the inputs to the connected MANPAGE(work_queue_worker) instances, runs the ProtoMol simulation
package, and gathers the output after each step. It randomly picks two neigboring replicas, applies the 
metropolis criterion, and if it is satisfied, swaps the parameters of the two replicas and continues 
simulations.

The BOLD(pdb_file), BOLD(psf_file), and BOLD(par_file) arguments specify the input files required for the
simulation run. The BOLD(min_temp) and BOLD(max_temp) specify the temperature range in which the replicas
are simulated. The number of replicas simulated is given by BOLD(num_replicas). 

BOLD(replica_exchange_protomol) can be run on any machine accesible to work_queue_worker instances. 

SECTION(OPTIONS)
OPTIONS_BEGIN
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

SECTION(EXAMPLES)
To run a replica exchange experiment with 84 replicas in the temperature range 278 to 400K using the input files in sample_input_files:
LONGCODE_BEGIN
python wq_replica_exchange_checkpoint sample_input_files/ww_exteq_nowater1.pdb sample_input_files/ww_exteq_nowater1.psf sample_input_files/par_all27_prot_lipid.inp 278 400 84
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE
