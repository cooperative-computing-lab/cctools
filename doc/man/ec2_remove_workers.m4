include(manual.h)dnl
HEADER(ec2_remove_workers)dnl

SECTION(NAME)
BOLD(ec2_remove_workers) - remove work_queue_worker instances on the Amazon EC2 service.

SECTION(SYNOPSIS)
CODE(BOLD(ec2_remove_workers [options] PARAM(num-workers)))

SECTION(DESCRIPTION)
CODE(ec2_remove_workers) removes MANPAGE(work_queue_worker,1) running on the Amazon EC2 service
and deletes the EC2 instances running them. It calls EC2kill that is part of the EC2 API tools 
to delete the EC2 instances.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(-a)Remove all running workers and terminate their EC2 instances.
OPTION_PAIR(-i, image_id)EC2 OS image ID of instances running the workers. Default = ami-fa01f193.
OPTION_ITEM(-h)Show help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
Remove 10 work_queue_worker instances and delete the EC2 instances running them.
LONGCODE_BEGIN
ec2_remove_workers 10 
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_WORK_QUEUE

FOOTER

