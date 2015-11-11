######################################################################
# Copyright (C) 2014- The University of Notre Dame
# This software is distributed under the GNU General Public License.
#    See the file COPYING for details.
######################################################################

### See module documentation at the end of this file.

package Work_Queue;

use strict;
use warnings;
use Carp qw(croak);

use work_queue;
use Work_Queue::Task;

our $VERSION = 4.3.0;

local $SIG{INT} = sub {
	croak "Got terminate signal!\n";
};

sub Work_Queue::new {
	my $class = shift;

	unshift @_, 'port' if @_ == 1;
	my %args = @_;

	$args{port} //= $Work_Queue::WORK_QUEUE_DEFAULT_PORT;

	my $_work_queue = work_queue::work_queue_create($args{port});
	croak "Could not create a work queue on port $args{port}" unless $_work_queue;

	my $_stats           = work_queue::work_queue_stats->new();
	my $_stats_hierarchy = work_queue::work_queue_stats->new();

	my $q = bless {
	_work_queue      => $_work_queue,
	_task_table      => {},
	_stats           => $_stats,
	_stats_hierarchy => $_stats_hierarchy,
	_shutdown        => $args{shutdown} // 0
	}, $class;

	$q->specify_name($args{name})           if $args{name};
	$q->specify_master_mode($args{catalog}) if $args{catalog};

	return $q;
}

sub DESTROY {
	my ($self) = @_;
	$self->shutdown_workers(0) if $self->{_shutdown};

	return work_queue_delete($self->{_work_queue});
}

sub set_debug_flag {
	my $self = shift;

	foreach my $flag (@_) {
	cctools_debug_flags_set($flag);
	}
}

sub set_debug_config_file {
	my ($self, $filename) = @_;
	return cctools_debug_config_file($filename);
}

sub name {
	my $self = shift;
	return work_queue_name($self->{_work_queue});
}

sub port {
	my ($self) = @_;
	return work_queue_port($self->{_work_queue})
}

sub stats {
	my ($self) = @_;
	work_queue_get_stats($self->{_work_queue}, $self->{_stats});
	return $self->{_stats};
}

sub stats_hierarchy {
	my ($self) = @_;
	work_queue_get_stats_hierarchy($self->{_work_queue}, $self->{_stats_hierarchy});
	return $self->{_stats_hierarchy};
}

sub task_state {
	my ($self, $taskid) = @_;
	return work_queue_task_state($self->{_work_queue}, $taskid);
}

sub enable_monitoring {
	my ($self, $summary_file) = @_;
	return work_queue_enable_monitoring($self->{_work_queue}, $summary_file);
}

sub enable_monitoring_full {
	my ($self, $dir_name) = @_;
	return work_queue_enable_monitoring_full($self->{_work_queue}, $dir_name);
}

sub activate_fast_abort {
	my ($self, $multiplier) = @_;
	return work_queue_activate_fast_abort($self->{_work_queue}, $multiplier);
}

sub empty {
	my ($self) = @_;
	return work_queue_empty($self->{_work_queue});
}

sub hungry {
	my ($self) = @_;
	return work_queue_hungry($self->{_work_queue});
}

sub specify_algorithm {
	my ($self, $algorithm) = @_;
	return work_queue_specify_algorithm($self->{_work_queue}, $algorithm);
}

sub specify_task_order {
	my ($self, $order) = @_;
	return work_queue_specify_task_order($self->{_work_queue}, $order);
}

sub specify_name {
	my ($self, $name) = @_;
	return work_queue_specify_name($self->{_work_queue}, $name);
}

sub specify_priority {
	my ($self, $priority) = @_;
	return work_queue_specify_priority($self->{_work_queue}, $priority);
}

sub specify_num_tasks_left {
	my ($self, $ntasks) = @_;
	return work_queue_specify_num_tasks_left($self->{_work_queue}, $ntasks);
}

sub specify_master_mode {
	my ($self, $mode) = @_;
	return work_queue_specify_master_mode($self->{_work_queue}, $mode);
}

sub specify_catalog_server {
	my ($self, $hostname, $port) = @_;
	return work_queue_specify_catalog_server($self->{_work_queue}, $hostname, $port);
}

sub specify_log {
	my ($self, $logfile) = @_;
	return work_queue_specify_log($self->{_work_queue}, $logfile);
}

sub specify_password {
	my ($self, $password) = @_;
	return work_queue_specify_password($self->{_work_queue}, $password);
}

sub specify_password_file {
	my ($self, $file) = @_;
	return work_queue_specify_password_file($self->{_work_queue}, $file);
}

sub cancel_by_taskid {
	my ($self, $id) = @_;
	return work_queue_cancel_by_taskid($self->{_work_queue}, $id);
}

sub cancel_by_tasktag {
	my ($self, $tag) = @_;
	return work_queue_cancel_by_tasktag($self->{_work_queue}, $tag);
}

sub shutdown_workers {
	my ($self, $n) = @_;
	return work_queue_shut_down_workers($self->{_work_queue}, $n);
}

sub blacklist {
	my ($self, $host) = @_;
	return work_queue_blacklist_add($self->{_work_queue}, $host);
}

sub blacklist_clear {
	my ($self, $host) = @_;

	if($host) {
		return work_queue_blacklist_remove($self->{_work_queue}, $host);
	}
	else {
		return work_queue_blacklist_clear($self->{_work_queue});
	}
}

sub specify_keepalive_interval {
	my ($self, $interval) = @_;
	return work_queue_specify_keepalive_interval($self->{_work_queue}, $interval);
}

sub specify_keepalive_timeout {
	my ($self, $timeout) = @_;
	return work_queue_specify_keepalive_timeout($self->{_work_queue}, $timeout);
}

sub estimate_capacity {
	my ($self) = @_;
	return work_queue_specify_estimate_capacity_on($self->{_work_queue}, 1);
}

sub activate_worker_waiting {
	my ($self, $workers) = @_;
	return work_queue_activate_worker_waiting($self->{_work_queue}, $workers);
}

sub tune {
	my ($self, $name, $value) = @_;
	return work_queue_tune($self->{_work_queue}, $name, $value);
}

sub submit {
	my ($self, $task) = @_;
	my $taskid = work_queue_submit($self->{_work_queue}, $task->{_task});
	$self->{_task_table}{$taskid} = $task;

	return $taskid;
}

sub wait {
	my ($self, $timeout) = @_;
	$timeout //= $WORK_QUEUE_WAITFORTASK;
	my $_task = work_queue_wait($self->{_work_queue}, $timeout);
	if($_task) {
	my $task = delete $self->{_task_table}{$_task->{taskid}};
	return $task;
	} else {
	return 0;
	}
}
1;

__END__

=head1 NAME

Work_Queue - Perl Work Queue bindings.

=head1 SYNOPSIS

The objects and methods provided by this package correspond to the
native C API in work_queue.h. See also Work_Queue::Task, which is
automatically loaded with this module.

The SWIG-based Perl bindings provide a higher-level interface, such as:

		use Work_Queue;

		my $q = Work_Queue->new( port => $port, name => 'my_queue_name');

		my $t = Work_Queue::Task->new($command);
		$t->specify_input_file(local_name => 'some_name', remote_name => 'some_other_name');
		$t->specify_output_file('some_name');

		$q->submit($t);

		my $stats = $q->stats;
		print $stats->{tasks_running}, '\n';

		$t = $q->wait(5);

		if($t) {
				my $resources = $t->resources_measured;
				print $resources->{resident_memory}, '\n';
		}

=head1 METHODS

=head2 Work_Queue

=head3 C<< Work_Queue::new ( ) >>

=head3 C<< Work_Queue::new ( $port ) >>

=head3 C<< Work_Queue::new ( port => ..., name => ..., catalog => ..., shutdown => ...) >>

Create a new work queue.

=over 12

=item port

The port number to listen on. If zero is specified, then the default is chosen, and if -1 is specified, a random port is chosen.

=item name

The project name to use.

=item catalog

Whether or not to enable catalog mode.

=item shutdown

Automatically shutdown workers when queue is finished. Disabled by default.

=back

		my $q = Work_Queue->new( port => 0, name => 'my_queue' );

See work_queue_create in the C API for more information about environmental variables that affect the behavior this method.

=head3 C<name>

Get the project name of the queue.

		 print $q->name;

=head3 C<port>

Get the listening port of the queue.

		 print $q->port

=head3 C<stats>

Get the master's queue statistics.

		 print $q->stats->{workers_busy};

=head3 C<stats_hierarchy>

Get the queue statistics, including master and foremen.

		 print $q->stats_hierarchy->{workers_busy};

=head3 C<enable_monitoring($dir_name)>

Enables resource monitoring of tasks in the queue, and writes a summary per
task to the directory given. Additionally, all summaries are consolidate into
the file all_summaries-PID.log

Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).

=over 12

=item dirname    Directory name for the monitor output.

=back

=head3 C<enable_monitoring_full($dir_name)>

As @ref enable_monitoring, but it also generates a time series and a debug
file.  WARNING: Such files may reach gigabyte sizes for long running tasks.

Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).

=over 12

=item dirname    Directory name for the monitor output.

=back


=head3 C<fast_abort>

Turn on or off fast abort functionality for a given queue.

=over 12

=item multiplier

The multiplier of the average task time at which point to abort; if
negative (the default) fast_abort is deactivated.

=back

=head3 C<empty>

Determine whether there are any known tasks queued, running, or waiting to be collected.

Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".

=head3 C<hungry>

Determine whether the queue can support more tasks.

Returns the number of additional tasks it can support if "hungry" and 0 if "sated".

=head3 C<specify_algorithm>

Set the worker selection algorithm for queue.

=over 12

=item algorithm

One of the following algorithms to use in assigning a task to a worker:

=over 24

=item $Work_Queue::WORK_QUEUE_SCHEDULE_FCFS

=item $Work_Queue::WORK_QUEUE_SCHEDULE_FILES

=item $Work_Queue::WORK_QUEUE_SCHEDULE_TIME

=item $Work_Queue::WORK_QUEUE_SCHEDULE_RAND

=back

=back

=head3 C<specify_task_order>

Set the order for dispatching submitted tasks in the queue to workers:

=over 12

=item order

One of the following algorithms to use in dispatching

=over 24

=item $Work_Queue::WORK_QUEUE_TASK_ORDER_FIFO

=item $Work_Queue::WORK_QUEUE_TASK_ORDER_LIFO

=back

=back

=head3 C<specify_name>

Change the project name for the given queue.

=over 12

=item name

The new project name.

=back

=head3 C<specify_priority>

Change the project priority for the given queue.

=over 12

=item priority

An integer that presents the priorty of this work queue master. The higher the value, the higher the priority.

=back

=head3 C<specify_num_tasks_left>

Specify the number of tasks not yet submitted to the queue.
It is used by work_queue_pool to determine the number of workers to launch.
If not specified, it defaults to 0.
work_queue_pool considers the number of tasks as:
num tasks left + num tasks running + num tasks read.

=over 12

=item ntasks

ntasks Number of tasks yet to be submitted.

=back


=head3 C<specify_master_mode>

Specify the master mode for the given queue.

=over 12

=item mode

This may be one of the following values:

=over 24

=item $Work_Queue::WORK_QUEUE_MASTER_MODE_STANDALONE

=item $Work_Queue::WORK_QUEUE_MASTER_MODE_CATALOG.

=back

=back

=head3 C<specify_catalog_server>

Specify the catalog server the master should report to.

=over 12

=item hostname

The hostname of the catalog server.

=item port

The port the catalog server is listening on.

=back

=head3 C<specify_log>

Specify a log file that records the states of connected workers and
submitted tasks.

=over 12

=item logfile

Name of the file to write the log. If the file exists, then new records are appended.

=back

=head3 C<specify_password>

Add a mandatory password that each worker must present.

=over 12

=item password

The password, as a string.

=back

=head3 C<specify_password_file>

Add a mandatory password file that each worker must present.

=over 12

=item file

Name of the file containing the password.

=back

=head3 C<cancel_by_taskid>

Cancel task identified by its taskid and remove from the given queue.

=over 12

=item id

The taskid returned from Work_Queue->submit.

=back

=head3 C<cancel_by_tasktag>

Cancel task identified by its tag and remove from the given queue.

=over 12

=item tag

The tag assigned to task using $t->speficy_tag($tag);

=back

=head3 C<shutdown_workers>

Shutdown workers connected to queue. Gives a best effort and then returns the number of workers given the shutdown order.

=over 12

=item n

The number to shutdown.  To shut down all workers, specify 0.

=back

=head3 C<blacklist>

Blacklist workers running on host.

=over 12

=item host

The hostname the host running the workers.

=back

=head3 C<blacklist_clear>

Remove host from blacklist. Clear all blacklist if host not provided.

=over 12

=item host

The of the hostname the host.

=back

=head3 C<specify_keepalive_interval>

Change keepalive interval for a given queue.

=over 12

=item interval Minimum number of seconds to wait before sending new
keepalive checks to workers.

=back

=head3 C<specify_keepalive_timeout>

Change keepalive timeout for a given queue.

=over 12

=item timeout

Minimum number of seconds to wait for a keepalive response from worker before marking it as dead.

=back

=head3 C<estimate_capacity>

Turn on master capacity measurements.

=head3 C<activate_worker_waiting>

Wait for at least n workers to connect before continuing.

=over 12

=item n

Number of workers.

=back

=head3 C<tune>

Tune advanced parameters for work queue.

=over 12

=item name  The name fo the parameter to tune. Can be one of following:

=over 24

=item "asynchrony-multiplier"

Treat each worker as having (actual_cores * multiplier) total cores. (default = 1.0)

=item "asynchrony-modifier"

Treat each worker as having an additional "modifier" cores. (default=0)

=item "min-transfer-timeout"

Set the minimum number of seconds to wait for files to be transferred to or from a worker. (default=300)

=item "foreman-transfer-timeout"

Set the minimum number of seconds to wait for files to be transferred to or from a foreman. (default=3600)

=item "fast-abort-multiplier"

Set the multiplier of the average task time at which point to abort; if negative or zero fast_abort is deactivated. (default=0)

=item "keepalive-interval"

Set the minimum number of seconds to wait before sending new keepalive checks to workers. (default=300)

=item "keepalive-timeout"

Set the minimum number of seconds to wait for a keepalive response from worker before marking it as dead. (default=30)

=back

=item value The value to set the parameter to.

=back

Return 0 on succes, -1 on failure.

=head3 C<submit>

Submit a task to the queue.

=over 12

=item task

A task description created from Work_Queue::Task.

=back

		$q->submit($task);

=head3 C<wait>

Wait for tasks to complete.

This call will block until the timeout has elapsed

=over 12

=item timeout

The number of seconds to wait for a completed task back before
returning.  Use an integer to set the timeout or the constant
$Work_Queue::WORK_QUEUE_WAITFORTASK to block until a task has
completed.

=back

		while( !$q->empty ) {
				...
				$task = $q->wait($seconds);

				if($task) {
					...
				}
				...
		}
=cut
