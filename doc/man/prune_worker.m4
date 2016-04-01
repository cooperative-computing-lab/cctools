include(manual.h)dnl
HEADER(prune_worker)

SECTION(NAME)
BOLD(prune_worker) - Execute pending Prune calls.

SECTION(SYNOPSIS)
CODE(BOLD(prune_worker --type [local|wq] --concurrency [num_processes] --name [work_queue_master_name]))

SECTION(DESCRIPTION)

BOLD(prune_worker) spawns workers (either locally or by initiating a Work Queue master) to execute Prune calls.

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(--type local|wq) Choose local or work queue workers. (default: local)
OPTION_ITEM(--concurrency process_count) Number of concurrent local workers.
OPTION_ITEM(--name master_name) The name of the Work Queue master to send Work Queue workers to. (default: prune)
OPTIONS_END

SECTION(EXAMPLES)

To use up to 16 local processes for executing Prune calls:

LONGCODE_BEGIN
% prune_worker --concurrency 16
LONGCODE_END

To start up a Work Queue master with the name 'my_wq_master':

LONGCODE_BEGIN
% prune_worker --type wq --name my_wq_master
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_PRUNE

FOOTER
