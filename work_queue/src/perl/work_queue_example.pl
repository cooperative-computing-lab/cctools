#!/usr/bin/perl 

use work_queue;

my $port = $WORK_QUEUE_DEFAULT_PORT;

if ($#ARGV < 0) {
	print "work_queue_example <file1> [file2] [file3] ...\n";
	print "Each file given on the command line will be compressed using a remote worker.\n";
	exit 1;
}

my $q = work_queue_create($port);
if (not defined($q)) {
	print "Instantiation of Work Queue failed!\n";
	exit 1;
}

$port = work_queue_port($q);
print "listening on port $port...\n"; 

for (my $i = 0; $i <= $#ARGV; $i++) {
	my $infile = $ARGV[$i]; 
	my $outfile = $ARGV[$i] . ".gz";
	my $command = "/usr/bin/gzip < $infile > $outfile";

    my $t = work_queue_task_create($command);

    work_queue_task_specify_file($t, $infile, $infile, $WORK_QUEUE_INPUT, $WORK_QUEUE_CACHE);
    work_queue_task_specify_file($t, $outfile, $outfile, $WORK_QUEUE_OUTPUT, $WORK_QUEUE_CACHE);

    my $taskid = work_queue_submit($q, $t);
    print "submitted task (id# $t->{taskid}): $t->{command_line}\n";
}

print "waiting for tasks to complete...\n";

while (not work_queue_empty($q)) {
    my $t = work_queue_wait($q, 5);

    if (defined($t)) {
		print "task (id# $t->{taskid}) complete: $t->{command_line} (return code $t->{return_status})\n";
		work_queue_task_delete($t);
	}
}

print "all tasks complete!\n";

work_queue_delete($q);

exit 0;
