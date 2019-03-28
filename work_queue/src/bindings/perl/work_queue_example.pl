#! /usr/bin/env perl

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#
# This program is a very simple example of how to use Work Queue.
# It accepts a list of files on the command line.
# Each file is compressed with gzip and returned to the user.

use strict;
use warnings;

use Work_Queue;


# Main program:
unless (@ARGV) {
	die "work_queue_example <file1> [file2] [file3] ...\nEach file given on the command line will be compressed using a remote worker.\n";
}

# Usually, we can execute the gzip utility by simply typing its name at a
# terminal. However, this is not enough for work queue; we have to specify
# precisely which files need to be transmitted to the workers. We record the
# location of gzip in 'gzip_path', which is usually found in /bin/gzip or
# /usr/bin/gzip.
my $gzip_path = find_executable('gzip') || die "Could not find gzip anywhere in \$PATH";

# We create the tasks queue using the default port. If this port is already
# been used by another program, you can try setting port = 0 to use an
# available port.

my $port = $Work_Queue::WORK_QUEUE_DEFAULT_PORT;
my $q    = Work_Queue->new($port) || die "Instantiation of Work Queue failed! ($!)\n";

print "listening on port $port...\n";

# We create and dispatch a task for each filename given in the argument list
for my $infile (@ARGV) {
	my $outfile = $infile . ".gz";

	# Note that we write ./gzip here, to guarantee that the gzip
	# version we are using is the one being sent to the workers.
	my $command = "./gzip <$infile >$outfile";
	my $t = Work_Queue::Task->new($command);

	# gzip is the same across all tasks, so we can cache (the default)
	# it in the workers.  Note that when specifying a file, we have
	# to name its local name (e.g.  gzip_path), and its remote name
	# (e.g. "gzip"). Unlike the following line, more often than not
	# these are the same.
	$t->specify_input_file(local_name => $gzip_path, remote_name=>'gzip');

	# files to be compressed are different across all tasks, so we do
	# not cache them. This is, of course, application
	# specific. Sometimes you may want to cache an output file if is
	# the input of a later task. Note that remote_name defaults to
	# local_name when absent.
	$t->specify_input_file(local_name => $infile,   cache => $Work_Queue::WORK_QUEUE_NOCACHE);
	$t->specify_output_file(local_name => $outfile, cache => $Work_Queue::WORK_QUEUE_NOCACHE);

	# Once all files has been specified, we are ready to submit the
	# task to the queue.
	my $taskid = $q->submit($t);
	print "submitted task (id# ",  $t->id, '): ', $t->command, "\n";
}

print "waiting for tasks to complete...\n";

while (not $q->empty) {
	my $t = $q->wait(5);

	if($t) {
	print 'task (id# ', $t->id, ') complete: ', $t->command, '(return code ', $t->return_status, ")\n";

	if($t->return_status != 0) {
		# Task failed. Error handling here.
	}
	}
}

print "all tasks complete!\n";

sub find_executable {
	my $executable = shift;

	my @dirs = split ':', $ENV{PATH};
	push @dirs, '.';

	my @paths = map { "$_/$executable" } @dirs;

	for my $path (@paths) {
	return $path if -x $path;
	}

	undef;
}
