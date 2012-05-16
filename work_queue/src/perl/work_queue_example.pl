#!/usr/bin/perl 

use strict;
use work_queue;

if ($#ARGV < 0) {
	print "work_queue_example <file1> [file2] [file3] ...\n";
	print "Each file given on the command line will be compressed using a remote worker.\n";
	exit 1;
}

my $wq = work_queue::work_queue_create($work_queue::WORK_QUEUE_DEFAULT_PORT);
if (not defined($wq)) {
	print "Instantiation of Work Queue failed!\n";
	exit 1;
}

my $port = work_queue::work_queue_port($wq);
print "listening on port $port...\n"; 

for (my $i = 0; $i <= $#ARGV; $i++) {
	my $infile = $ARGV[$i]; 
	my $outfile = $ARGV[$i] . ".gz";
	my $command = "/usr/bin/gzip < $infile > $outfile";

    my $task = work_queue::work_queue_task_create($command);

    work_queue::work_queue_task_specify_file($task, $infile, $infile, $work_queue::WORK_QUEUE_INPUT, $work_queue::WORK_QUEUE_CACHE);
    work_queue::work_queue_task_specify_file($task, $outfile, $outfile, $work_queue::WORK_QUEUE_OUTPUT, $work_queue::WORK_QUEUE_CACHE);

    work_queue::work_queue_submit($wq, $task);
    print "submitted task (id# $task->{taskid}): $task->{command_line}\n";
}

print "waiting for tasks to complete...\n";

while (not work_queue::work_queue_empty($wq)) {
    my $task = work_queue::work_queue_wait($wq, 5);

    if (defined($task)) {
		print "task (id# $task->{taskid}) complete: $task->{command_line} (return code $task->{return_status})\n";
    }
}

print "all tasks complete!\n";

exit 0;
