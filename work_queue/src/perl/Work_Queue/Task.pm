######################################################################
# Copyright (C) 2014- The University of Notre Dame
# This software is distributed under the GNU General Public License.
#    See the file COPYING for details.
######################################################################

### See module documentation at the end of this file.

package Work_Queue::Task;

use strict;
use warnings;

use work_queue;

use Carp qw(croak);

our $VERSION = 4.3.0;

sub Work_Queue::Task::new {
	my ($class, $command) = @_;
	my $_task = work_queue_task_create($command);

	croak "Could not create task." unless $_task;

	return bless {_task => $_task}, $class;
}

sub DESTROY {
	my $self = shift;

	my $id = eval { $self->id };
	if(!$@) {
	# Ignore possible message in global cleanup.
	eval { work_queue_task_delete($self->{_task}) };
	}
}

sub _determine_file_flags {
	my ($flags, $cache) = @_;

	$flags //= $WORK_QUEUE_CACHE;
	if($cache) {
	$flags |= $WORK_QUEUE_CACHE;
	} else {
	$flags &= ~$WORK_QUEUE_CACHE;
	}

	return $flags;
}

sub specify_tag {
	my ($self, $tag) = @_;
	return work_queue_task_specify_tag($self->{_task}, $tag);;
}

sub clone {
	my ($self) = @_;
	my $copy = $self;

	$copy->{_task} = work_queue_task_clone($self->{_task});

	return $copy;
}

sub specify_command {
	my ($self) = @_;
	return work_queue_task_specify_command($self->{_task});;
}

sub specify_algorithm {
	my ($self, $algorithm) = @_;
	return work_queue_task_specify_algorithm($self->{_task}, $algorithm);
}

sub specify_preferred_host {
	my ($self, $host) = @_;
	return work_queue_task_specify_preferred_host($self->{_task}, $host);
}

sub specify_file {
	my $self = shift;
	my %args = @_;

	croak "At least local_name should be specified." unless $args{local_name};

	$args{remote_name} //= $args{local_name};
	$args{type}        //= $WORK_QUEUE_INPUT;
	$args{cache}       //= 1;
	$args{flags}         = _determine_file_flags($args{flags}, $args{cache});

	return work_queue_task_specify_file($self->{_task},
					$args{local_name},
					$args{remote_name},
					$args{type},
					$args{flags});
}

sub specify_file_piece {
	my $self = shift;
	my %args = @_;

	croak "At least local_name should be specified." unless $args{local_name};

	$args{remote_name} //= $args{local_name};
	$args{start_byte}  //= 0;
	$args{end_byte}    //= 0;
	$args{type}        //= $WORK_QUEUE_INPUT;
	$args{cache}       //= 1;
	$args{flags}         = _determine_file_flags($args{flags});

	return work_queue_task_specify_file_piece($self->{_task},
						  $args{local_name},
						  $args{remote_name},
						  $args{start_byte},
						  $args{end_byte},
						  $args{type},
						  $args{flags});
}

sub specify_input_file {
	my $self = shift;
	unshift @_, 'local_name', if @_ == 1;
	my %args = @_;

	return $self->specify_file(local_name  => $args{local_name},
				   remote_name => $args{remote_name},
				   type        => $WORK_QUEUE_INPUT,
				   flags       => $args{flags},
				   cache       => $args{cache});
}

sub specify_output_file {
	my $self = shift;

	unshift @_, 'local_name', if @_ == 1;
	my %args = @_;

	return $self->specify_file(local_name  => $args{local_name},
				   remote_name => $args{remote_name},
				   type        => $WORK_QUEUE_OUTPUT,
				   flags       => $args{flags},
				   cache       => $args{cache});
}

sub specify_directory {
	my $self = shift;
	my %args = @_;
	croak "At least local_name should be specified." unless $args{local_name};

	$args{remote_name} //= $args{local_name};
	$args{type}        //= $WORK_QUEUE_INPUT;
	$args{recursive}   //= 0;

	$args{cache}       //= 1;
	$args{flags}         = _determine_file_flags($args{flags}, $args{cache});

	return work_queue_task_specify_directory($self->{_task},
						 $args{local_name},
						 $args{type},
						 $args{flags},
						 $args{recursive});
}

sub specify_buffer {
	my $self = shift;
	my %args = @_;

	croak "The buffer and remote_name should be specified."
	unless ($args{remote_name} and $args{buffer});

	$args{cache}       //= 1;
	$args{flags}         = _determine_file_flags($args{flags}, $args{cache});

	return work_queue_task_specify_buffer($self->{_task},
					  $args{buffer},
					  $args{remote_name},
					  $args{flags},
					  $args{cache});
}

sub specify_cores {
	my ($self, $cores) = @_;
	return work_queue_task_specify_cores($self->{_task}, $cores);
}

sub specify_memory {
	my ($self, $memory) = @_;
	return work_queue_task_specify_memory($self->{_task}, $memory);
}

sub specify_disk {
	my ($self, $disk) = @_;
	return work_queue_task_specify_disk($self->{_task}, $disk);
}

sub specify_gpus {
	my ($self, $gpus) = @_;
	return work_queue_task_specify_gpus($self->{_task}, $gpus);
}

sub specify_end_time {
	my ($self, $seconds) = @_;
	return work_queue_task_specify_end_time($self->{_task}, $seconds);
}

sub specify_priority {
	my ($self, $priority) = @_;
	return work_queue_task_specify_priority($self->{_task}, $priority);
}

sub specify_environment_variable {
	my ($self, $name, $value) = @_;
	return work_queue_task_specify_enviroment_variable($self->{_task}, $name, $value);

sub tag {
	my ($self) = @_;
	return $self->{_task}->{tag};
}

sub priority {
	my ($self) = @_;
	return $self->{_task}->{priority};
}

sub command {
	my ($self) = @_;
	return $self->{_task}->{command_line};
}

sub algorithm {
	my ($self) = @_;
	return $self->{_task}->{algorithm};
}

sub output {
	my ($self) = @_;
	return $self->{_task}->{output};
}

sub id {
	my ($self) = @_;
	return $self->{_task}->{taskid};
}

sub return_status {
	my ($self) = @_;
	return $self->{_task}->{return_status};
}

sub result {
	my ($self) = @_;
	return $self->{_task}->{result};
}

sub total_submissions {
	my ($self) = @_;
	return $self->{_task}->{total_submissions};
}

sub host {
	my ($self) = @_;
	return $self->{_task}->{host};
}

sub hostname {
	my ($self) = @_;
	return $self->{_task}->{hostname};
}

sub commit_time {
	my ($self) = @_;
	return $self->{_task}->{time_committed};
}

sub submit_time {
	my ($self) = @_;
	return $self->{_task}->{time_task_submit};
}

sub finish_time {
	my ($self) = @_;
	return $self->{_task}->{time_task_finish};
}

sub time_app_delay {
	my ($self) = @_;
	return $self->{_task}->{time_app_delay};
}

sub send_input_start {
	my ($self) = @_;
	return $self->{_task}->{time_send_input_start};
}

sub send_input_finish {
	my ($self) = @_;
	return $self->{_task}->{time_send_input_finish};
}

sub execute_cmd_start {
	my ($self) = @_;
	return $self->{_task}->{time_execute_cmd_start};
}

sub execute_cmd_finish {
	my ($self) = @_;
	return $self->{_task}->{time_execute_cmd_finish};
}

sub receive_output_start {
	my ($self) = @_;
	return $self->{_task}->{time_receive_output_start};
}

sub receive_output_finish {
	my ($self) = @_;
	return $self->{_task}->{time_receive_output_finish};
}

sub total_bytes_received {
	my ($self) = @_;
	return $self->{_task}->{total_bytes_received};
}

sub total_bytes_sent {
	my ($self) = @_;
	return $self->{_task}->{total_bytes_sent};
}

sub total_bytes_transferred {
	my ($self) = @_;
	return $self->{_task}->{total_bytes_transferred};
}

sub total_transfer_time {
	my ($self) = @_;
	return $self->{_task}->{total_transfer_time};
}

sub cmd_execution_time {
	my ($self) = @_;
	return $self->{_task}->{cmd_execution_time};
}

sub total_cmd_execution_time {
	my ($self) = @_;
	return $self->{_task}->{total_cmd_execution_time};
}

sub resources_measured {
	my ($self) = @_;
	return $self->{_task}->{resources_measured};
}
1;

__END__

=head1 NAME

Work_Queue::Task - Perl Work Queue Task bindings.

=head1 SYNOPSIS

The objects and methods provided by this package correspond to the
native C API in work_queue.h for task creation and manipulation. This
module is automatically loaded with C<< Work_Queue >>.

		use Work_Queue;

		my $t = Work_Queue::Task->new($command);
		$t->specify_input_file(local_name => 'some_name', remote_name => 'some_other_name');
		$t->specify_output_file('some_name');

		$q->submit($t);

		$t = $q->wait(5);

		if($t) {
				my $resources = $t->resources_measured;
				print $resources->{resident_memory}, '\n';
		}

=head1 METHODS

=head2 Work_Queue::Task

=head3 C<< Work_Queue::Task->new('/some/command < input > output'); >>

Create a new task specification.

=over 12

=item command

The shell command line to be exected by the task.

=back

=head3 C<specify_tag>

Attach a user defined logical name to the task.

=over 12

=item tag

The tag to be executed.

=back

=head3 C<clone>

Return a copy of this task.

=head3 C<specify_command>

Set the command to be executed by the task.

=over 12

=item command

The command to be executed.

=back

=head3 C<specify_algorithm>

Set the worker selection algorithm for task.

=over 12

=item algorithm

One of the following algorithms to use in assigning a task to a worker:

=over 24

=item WORK_QUEUE_SCHEDULE_FCFS

=item WORK_QUEUE_SCHEDULE_FILES

=item WORK_QUEUE_SCHEDULE_TIME

=item WORK_QUEUE_SCHEDULE_RAND

=back

=back

=head3 C<specify_preferred_host>

Indicate that the task would be optimally run on a given host.

=over 12

=item hostname

The hostname to which this task would optimally be sent.

=back

=head3 C<specify_file>

Add a file to the task.

=over 12

=item local_name

The name of the file on local disk or shared filesystem.

=item remote_name

The name of the file at the execution site.

=item type

Must be one of the following values: $Work_Queue::WORK_QUEUE_INPUT or $Work_Queue::WORK_QUEUE_OUTPUT

=item flags

May be zero to indicate no special handling, or any of the following or'd together:

=over 24

=item $Work_Queue::WORK_QUEUE_NOCACHE

=item $Work_Queue::WORK_QUEUE_CACHE

=item $Work_Queue::WORK_QUEUE_WATCH

=back

=item cache

Legacy parameter for setting file caching attribute.  By default this is enabled.

=back

		$t->specify_file(local_name => ...);

		$t->specify_file(local_name => ..., remote_name => ..., );

=head3 C<specify_file_piece>

Add a file piece to the task.

=over 12

=item local_name

The name of the file on local disk or shared filesystem.

=item remote_name

The name of the file at the execution site.

=item start_byte

The starting byte offset of the file piece to be transferred.

=item end_byte

The ending byte offset of the file piece to be transferred.

=item type

Must be one of the following values: $Work_Queue::WORK_QUEUE_INPUT or
$Work_Queue::WORK_QUEUE_OUTPUT.

=item flags

May be zero to indicate no special handling, or any of the following
or'd together. See Work_Queue::Task->specify_file

=item cache

Legacy parameter for setting file caching attribute.  By default this is enabled.

=back


		$t->specify_file_piece(local_name => ..., start_byte => ..., ...);

		$t->specify_file_piece(local_name => ..., remote_name => ..., ...);

=head3 C<specify_input_file>

Add a input file to the task.

This is just a wrapper for Work_Queue::Task->specify_file with type
set to $Work_Queue::WORK_QUEUE_INPUT. If only one argument is given,
it defaults to both local_name and remote_name.

=head3 C<specify_output_file>

Add a output file to the task.

This is just a wrapper for Work_Queue::Task->specify_file with type
set to $Work_Queue::WORK_QUEUE_OUTPUT. If only one argument is given,
then it defaults to both local_name and remote_name.

=head3 C<specify_directory>

Add a directory to the task.

=over 12

=item local_name

The name of the directory on local disk or shared filesystem. Optional
if the directory is empty.

=item remote_name

The name of the directory at the remote execution site.

=item type

Must be one of $Work_Queue::WORK_QUEUE_INPUT or $Work_Queue::WORK_QUEUE_OUTPUT.

=item flags May be zero to indicate no special handling. See Work_Queue::Task->specify_file.

=item recursive

Indicates whether just the directory (0) or the directory and all of
its contents (1) should be included.

=item cache

Legacy parameter for setting file caching attribute.  By default this is enabled.

=back

Returns 1 if the task directory is successfully specified, 0 if either
of @a local_name, or @a remote_name is null or @a remote_name is an
absolute path.

=head3 C<specify_buffer>

Add an input bufer to the task.

=over 12

=item buffer

The contents of the buffer to pass as input.

=item remote_name

The name of the remote file to create.

=item flags

May take the same values as Work_Queue::Task->specify_file.

=item cache

Legacy parameter for setting file caching attribute.  By default this is enabled.

=back

=head3 C<specify_cores>

Specify the number of cores the task requires.

=over 12

=item n

Number of cores.

=back

=head3 C<specify_memory>

Specify the size of the memory the task requires.

=over 12

=item n

Memory size, in megabytes.

=back

=head3 C<specify_disk>

Specify the size of disk the task requires.

=over 12

=item n

Disk size, in megabytes.

=back

=head3 C<specify_gpus>

Specify the number of gpus the task requires.

=over 12

=item n

Number of gpus.

=back

=head3 C<specify_end_time>

Indicate the maximum end time (in seconds from the Epoch) of this
task.

=over 12

=item seconds

Number of seconds.

=back

=head3 C<specify_priority>

Indicate the the priority of this task (larger means better priority,
default is 0).

=over 12

=item n

Integer priority.

=back

=head3 C<specify_environment_variable>

Set the environment variable to value before the task is run.

=over 12

=item name

Name of the environment variable.

=item value

Value of the environment variable. Variable is unset if value is not given.

=back

=head3 C<tag>

Get the tag value of the task.

=head3 C<priority>

Get the priority value of the task.

=head3 C<command>

Get the command line of the task.

=head3 C<algorithm>

Get the algorithm specified for this task to be dispatched.

=head3 C<output>

Get the standard output of the task. Must be called only after the task
completes execution.

=head3 C<id>

Get the task id number.

=head3 C<return_status>

Get the exit code of the command executed by the task. Must be called only
after the task completes execution.

=head3 C<result>

Get the result of the task (successful, failed return_status, missing input file, missing output file).

Must be called only after the task completes execution.

=head3 C<total_submissions>

Get the number of times the task has been resubmitted internally.

Must be called only after the task completes execution.

=head3 C<host>

Get the address and port of the host on which the task ran.
Must be called only after the task completes execution.

=head3 C<hostname>

Get the name of the host on which the task ran.
Must be called only after the task completes execution.

=head3 C<commit_time>

Get the time at which this task was committed to a worker.
Must be called only after the task completes execution.

=head3 C<submit_time>

Get the time at which this task was submitted.

Must be called only after the task completes execution.

=head3 C<finish_time>

Get the time at which this task was finished.

Must be called only after the task completes execution.

=head3 C<time_app_delay>

Get the time spent in upper-level application (outside of work_queue_wait).

Must be called only after the task completes execution.

=head3 C<send_input_start>

Get the time at which the task started to transfer input files.

Must be called only after the task completes execution.

=head3 C<send_input_finish>

Get the time at which the task finished transferring input files.

Must be called only after the task completes execution.

=head3 C<execute_cmd_start>

The time at which the task began.

Must be called only after the task completes execution.


=head3 C<execute_cmd_finish>

Get the time at which the task finished (discovered by the master).

Must be called only after the task completes execution.

=head3 C<receive_output_start>

Get the time at which the task started to transfer output files.

Must be called only after the task completes execution.

=head3 C<receive_output_finish>

Get the time at which the task finished transferring output files.

Must be called only after the task completes execution.

=head3 C<total_bytes_received>

Get the number of bytes received since task started receiving input data.

Must be called only after the task completes execution.

=head3 C<total_bytes_sent>

Get the number of bytes sent since task started sending input data.

Must be called only after the task completes execution.

=head3 C<total_bytes_transferred>

Get the number of bytes transferred since task started transferring input data.

Must be called only after the task completes execution.

=head3 C<total_transfer_time>

Get the time comsumed in microseconds for transferring total_bytes_transferred.

Must be called only after the task completes execution.

=head3 C<cmd_execution_time>

Get the time spent in microseconds for executing the command on the worker.

Must be called only after the task completes execution.

=head3 C<total_cmd_execution_time>

	Get the time spent in microseconds for executing the command on any worker.

	Must be called only after the task completes execution.

=head3 C<resources_measured>

	Get the resources measured when monitoring is enabled.

	Must be called only after the task completes execution.

		$t->resources_measured{bytes_read};
		$t->resources_measured{bytes_written};
		$t->resources_measured{cores};
		$t->resources_measured{cpu_time};
		$t->resources_measured{gpus};
		$t->resources_measured{max_concurrent_processes};
		$t->resources_measured{resident_memory};
		$t->resources_measured{resident_memory};
		$t->resources_measured{resident_memory};
		$t->resources_measured{swap_memory};
		$t->resources_measured{task_id};
		$t->resources_measured{total_processes};
		$t->resources_measured{virtual_memory};
		$t->resources_measured{wall_time};
		$t->resources_measured{workdir_footprint};
		$t->resources_measured{workdir_num_files};

=cut
