#!/usr/bin/perl 

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#
# This program is a very simple example of how to use Work Queue.
# It accepts a list of files on the command line.
# Each file is compressed with gzip and returned to the user.

use work_queue;

# Main program:
my $port = $WORK_QUEUE_DEFAULT_PORT;

if ($#ARGV < 0) {
	print "work_queue_example <file1> [file2] [file3] ...\n";
	print "Each file given on the command line will be compressed using a remote worker.\n";
	exit 1;
}

# Usually, we can execute the gzip utility by simply typing its name at a
# terminal. However, this is not enough for work queue; we have to specify
# precisely which files need to be transmitted to the workers. We record the
# location of gzip in 'gzip_path', which is usually found in /bin/gzip or
# /usr/bin/gzip.
$gzip_path = "/bin/gzip";
unless(-e $gzip_path) {
	$gzip_path = "/usr/bin/gzip";
	unless(-e $gzip_path) {
		print "gzip was not found. Please modify the gzip_path variable accordingly. To determine the location of gzip, from the terminal type: which gzip (usual locations are /bin/gzip and /usr/bin/gzip)\n";
		exit 1;
	}
}

# We create the tasks queue using the default port. If this port is already
# been used by another program, you can try setting port = 0 to use an
# available port.
my $q = work_queue_create($port);
if (not defined($q)) {
	print "Instantiation of Work Queue failed!\n";
	exit 1;
}

$port = work_queue_port($q);
print "listening on port $port...\n"; 

# We create and dispatch a task for each filename given in the argument list
for (my $i = 0; $i <= $#ARGV; $i++) {
	my $infile = $ARGV[$i]; 
	my $outfile = $ARGV[$i] . ".gz";

	# Note that we write ./gzip here, to guarantee that the gzip version we are
	# using is the one being sent to the workers.
	my $command = "./gzip < $infile > $outfile";

    my $t = work_queue_task_create($command);

	# gzip is the same across all tasks, so we can cache it in the * workers.
	# Note that when specifying a file, we have to name its local * name (e.g.
	# gzip_path), and its remote name (e.g. "gzip"). Unlike the * following
	# line, more often than not these are the same. */
    work_queue_task_specify_file($t, $gzip_path, "gzip", $WORK_QUEUE_INPUT, $WORK_QUEUE_CACHE); 

	# files to be compressed are different across all tasks, so we do not cache
	# them. This is, of course, application specific. Sometimes you may want to
	# cache an output file if is the input of a later task.
    work_queue_task_specify_file($t, $infile, $infile, $WORK_QUEUE_INPUT, $WORK_QUEUE_NOCACHE); 
    work_queue_task_specify_file($t, $outfile, $outfile, $WORK_QUEUE_OUTPUT, $WORK_QUEUE_NOCACHE); 

	# Once all files has been specified, we are ready to submit the task to the queue.
    my $taskid = work_queue_submit($q, $t);
    print "submitted task (id# $t->{taskid}): $t->{command_line}\n";
}

print "waiting for tasks to complete...\n";

while (not work_queue_empty($q)) {
    my $t = work_queue_wait($q, 5);

    if(defined($t)) {
		print "task (id# $t->{taskid}) complete: $t->{command_line} (return code $t->{return_status})\n";
		if($t->{return_status} != 0) {
			# Task failed. Error handling here.
		}
		work_queue_task_delete($t);
	}
}

print "all tasks complete!\n";

work_queue_delete($q);

exit 0;
