######################################################################
# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
#    See the file COPYING for details.
######################################################################

### See module documentation at the end of this file.

package Data_Swarm;

use strict;
use warnings;
use Carp qw(croak);
use Scalar::Util qw(looks_like_number);

use data_Swarm;
use Data_Swarm::Task;

local $SIG{INT} = sub {
	croak "Got terminate signal!\n";
};

sub Data_Swarm::new {
	my $class = shift;

	unshift @_, 'port' if @_ == 1;
	my %args = @_;

    $args{port} = specify_port_range($args{port});

	set_debug_log(undef, $args{debug_log}) if $args{debug_log};

	my $_data_Swarm = data_Swarm::data_Swarm_create($args{port});
	croak "Could not create a work queue on port $args{port}" unless $_data_Swarm;

	my $_stats           = data_Swarm::data_Swarm_stats->new();
	my $_stats_hierarchy = data_Swarm::data_Swarm_stats->new();

	my $q = bless {
	_data_Swarm      => $_data_Swarm,
	_task_table      => {},
	_stats           => $_stats,
	_stats_hierarchy => $_stats_hierarchy,
	_shutdown        => $args{shutdown} // 0
	}, $class;

	$q->specify_name($args{name})           if $args{name};
	$q->specify_manager_mode($args{catalog}) if $args{catalog};

    $q->specify_transactions_log($args{transactions_log}) if $args{transactions_log};
    $q->specify_log($args{stats_log}) if $args{stats_log};

	return $q;
}

sub DESTROY {
	my ($self) = @_;
	$self->shutdown_workers(0) if $self->{_shutdown};

	return data_Swarm_delete($self->{_data_Swarm});
}

sub specify_port_range {
    my $port = shift;

    unless($port) {
        return $Data_Swarm::DATA_SWARM_DEFAULT_PORT;
    }

    if(looks_like_number($port)) {
        return $port;
    }

    unless(ref $port eq 'ARRAY') {
        die "port specified does not look like a number or an array.\n"
    }

    unless(@{$port} == 2) {
        die "Invalid port range. Port range should be of the form [lower, upper].\n";
    }

    my ($lower, $upper) = @{$port};

    if($lower <= $upper) {
        $ENV{TCP_LOW_PORT}  = $lower;
        $ENV{TCP_HIGH_PORT} = $upper;
        return 0;
    } else {
        die "[@{[join(',', @{$port})]}] is an invalid range of ports.\n";
    }
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

sub set_debug_log {
    # note debug log does not really depend on current queue, as it can be set
    # before any queue.
    my ($self, $filename) = @_;

    cctools_debug_flags_set('all');
    cctools_debug_config_file_size(0);
    cctools_debug_config_file($filename);
}

sub name {
	my $self = shift;
	return data_Swarm_name($self->{_data_Swarm});
}

sub port {
	my ($self) = @_;
	return data_Swarm_port($self->{_data_Swarm})
}

sub stats {
	my ($self) = @_;
	data_Swarm_get_stats($self->{_data_Swarm}, $self->{_stats});
	return $self->{_stats};
}

sub stats_hierarchy {
	my ($self) = @_;
	data_Swarm_get_stats_hierarchy($self->{_data_Swarm}, $self->{_stats_hierarchy});
	return $self->{_stats_hierarchy};
}

sub stats_category {
	my ($self, $category) = @_;
	my $stats = data_Swarm::data_Swarm_stats->new();
	data_Swarm_get_stats_category($self->{_data_Swarm}, $category, $stats);
	return $stats;
}

sub specify_category_mode {
	my ($self, $category, $mode) = @_;
	return data_Swarm_specify_category_mode($self->{_data_Swarm}, $category, $mode);
}

sub specify_category_autolabel_resource {
	my ($self, $category, $resource, $mode, $autolabel) = @_;
	return data_Swarm_enable_category_resource($self->{_data_Swarm}, $category, $category, $resource, $autolabel);
}

sub task_state {
	my ($self, $taskid) = @_;
	return data_Swarm_task_state($self->{_data_Swarm}, $taskid);
}

sub enable_monitoring {
	my ($self, $dir_name, $watchdog) = @_;

    $watchdog = defined $watchdog ? $watchdog : 1;
	return data_Swarm_enable_monitoring($self->{_data_Swarm}, $dir_name, $watchdog);
}

sub enable_monitoring_full {
	my ($self, $dir_name, $watchdog) = @_;

    $watchdog = defined $watchdog ? $watchdog : 1;
	return data_Swarm_enable_monitoring_full($self->{_data_Swarm}, $dir_name, $watchdog);
}


sub enable_monitoring_snapshots {
	my ($self, $filename) = @_;
	return data_Swarm_enable_monitoring_snapshots($self->{_data_Swarm}, $filename);
}

sub activate_fast_abort {
	my ($self, $multiplier) = @_;
	return data_Swarm_activate_fast_abort($self->{_data_Swarm}, $multiplier);
}

sub activate_fast_abort_category {
	my ($self, $name, $multiplier) = @_;
	return data_Swarm_activate_fast_abort_category($self->{_data_Swarm}, $name, $multiplier);
}


sub specify_draining_by_hostname {
    my ($self, $hostname, $drain_mode) = @_;

    unless(defined $drain_mode) { 
        $drain_mode ||= 1;
    }

    return data_Swarm_specify_draining($self->{_data_Swarm}, $hostname, $drain_mode);
}

sub empty {
	my ($self) = @_;
	return data_Swarm_empty($self->{_data_Swarm});
}

sub hungry {
	my ($self) = @_;
	return data_Swarm_hungry($self->{_data_Swarm});
}

sub specify_algorithm {
	my ($self, $algorithm) = @_;
	return data_Swarm_specify_algorithm($self->{_data_Swarm}, $algorithm);
}

sub specify_task_order {
	my ($self, $order) = @_;
	return data_Swarm_specify_task_order($self->{_data_Swarm}, $order);
}

sub specify_name {
	my ($self, $name) = @_;
	return data_Swarm_specify_name($self->{_data_Swarm}, $name);
}

sub specify_manager_preferred_connection {
	my ($self, $mode) = @_;
	return data_Swarm_manager_preferred_connection($self->{_data_Swarm}, $mode);
}

sub specify_master_preferred_connection {
	my ($self, $mode) = @_;
	return data_Swarm_manager_preferred_connection($self->{_data_Swarm}, $mode);
}

sub specify_min_taskid {
	my ($self, $minid) = @_;
	return data_Swarm_specify_min_taskid($self->{_data_Swarm}, $minid);
}

sub specify_priority {
	my ($self, $priority) = @_;
	return data_Swarm_specify_priority($self->{_data_Swarm}, $priority);
}

sub specify_num_tasks_left {
	my ($self, $ntasks) = @_;
	return data_Swarm_specify_num_tasks_left($self->{_data_Swarm}, $ntasks);
}

sub specify_manager_mode {
	my ($self, $mode) = @_;
	return data_Swarm_specify_manager_mode($self->{_data_Swarm}, $mode);
}

sub specify_master_mode {
	my ($self, $mode) = @_;
	return data_Swarm_specify_master_mode($self->{_data_Swarm}, $mode);
}

sub specify_catalog_server {
	my ($self, $hostname, $port) = @_;
	return data_Swarm_specify_catalog_server($self->{_data_Swarm}, $hostname, $port);
}

sub specify_log {
	my ($self, $logfile) = @_;
	return data_Swarm_specify_log($self->{_data_Swarm}, $logfile);
}

sub specify_transactions_log {
	my ($self, $logfile) = @_;
	return data_Swarm_specify_transactions_log($self->{_data_Swarm}, $logfile);
}

sub specify_password {
	my ($self, $password) = @_;
	return data_Swarm_specify_password($self->{_data_Swarm}, $password);
}

sub specify_password_file {
	my ($self, $file) = @_;
	return data_Swarm_specify_password_file($self->{_data_Swarm}, $file);
}

sub cancel_by_taskid {
	my ($self, $id) = @_;
	return data_Swarm_cancel_by_taskid($self->{_data_Swarm}, $id);
}

sub cancel_by_tasktag {
	my ($self, $tag) = @_;
	return data_Swarm_cancel_by_tasktag($self->{_data_Swarm}, $tag);
}

sub shutdown_workers {
	my ($self, $n) = @_;
	return data_Swarm_shut_down_workers($self->{_data_Swarm}, $n);
}

sub block_host {
	my ($self, $host) = @_;
	return data_Swarm_block_host($self->{_data_Swarm}, $host);
}

sub blacklist {
	my ($self, $host) = @_;
	return $self->block_host($host);
}

sub block_host_with_timeout {
	my ($self, $host, $timeout) = @_;
	return data_Swarm_block_host_with_timeout($self->{_data_Swarm}, $host, $timeout);
}

sub blacklist_with_timeout {
	my ($self, $host, $timeout) = @_;
	return $self->block_host($host, $timeout);
}

sub unblock_host {
	my ($self, $host) = @_;

	if($host) {
		return data_Swarm_unblock_host($self->{_data_Swarm}, $host);
	}
	else {
		return data_Swarm_unblock_all($self->{_data_Swarm});
	}
}

sub blacklist_clear {
	my ($self, $host) = @_;
    return $self->unblock_host($host);
}

sub invalidate_cache_file {
    my ($self, $local_name) = @_;

    return data_Swarm_invalidate_cached_file($self->{_data_Swarm}, $local_name, $DATA_SWARM_FILE);
}

sub specify_keepalive_interval {
	my ($self, $interval) = @_;
	return data_Swarm_specify_keepalive_interval($self->{_data_Swarm}, $interval);
}

sub specify_keepalive_timeout {
	my ($self, $timeout) = @_;
	return data_Swarm_specify_keepalive_timeout($self->{_data_Swarm}, $timeout);
}

sub estimate_capacity {
	my ($self) = @_;
	return data_Swarm_specify_estimate_capacity_on($self->{_data_Swarm}, 1);
}

sub activate_worker_waiting {
	my ($self, $workers) = @_;
	return data_Swarm_activate_worker_waiting($self->{_data_Swarm}, $workers);
}

sub tune {
	my ($self, $name, $value) = @_;
	return data_Swarm_tune($self->{_data_Swarm}, $name, $value);
}

sub specify_max_resources {
	my ($self, $rm) = @_;
	return data_Swarm_specify_max_resources($self->{_data_Swarm}, $rm);
}

sub specify_min_resources {
	my ($self, $rm) = @_;
	return data_Swarm_specify_min_resources($self->{_data_Swarm}, $rm);
}

sub specify_category_max_resources {
	my ($self, $category, $rm) = @_;
	return data_Swarm_specify_category_max_resources($self->{_data_Swarm}, $category, $rm);
}

sub specify_category_min_resources {
	my ($self, $category, $rm) = @_;
	return data_Swarm_specify_category_min_resources($self->{_data_Swarm}, $category, $rm);
}

sub specify_category_first_allocation_guess {
	my ($self, $category, $rm) = @_;
	return data_Swarm_specify_category_first_allocation_guess($self->{_data_Swarm}, $category, $rm);
}

sub initialize_categories {
	my ($self, $rm, $filename) = @_;
	return data_Swarm_initialize_categories($self->{_data_Swarm}, $rm, $filename);
}

sub submit {
	my ($self, $task) = @_;
	my $taskid = data_Swarm_submit($self->{_data_Swarm}, $task->{_task});
	$self->{_task_table}{$taskid} = $task;

	return $taskid;
}

sub wait {
	my ($self, $timeout) = @_;
	$timeout //= $DATA_SWARM_WAITFORTASK;
	my $_task = data_Swarm_wait($self->{_data_Swarm}, $timeout);
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

Data_Swarm - Perl Work Queue bindings.

=head1 SYNOPSIS

The objects and methods provided by this package correspond to the
native C API in data_Swarm.h. See also Data_Swarm::Task, which is
automatically loaded with this module.

The SWIG-based Perl bindings provide a higher-level interface, such as:

		use Data_Swarm;

		my $q = Data_Swarm->new( port => $port, name => 'my_queue_name');

		my $t = Data_Swarm::Task->new($command);
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

=head2 Data_Swarm

=head3 C<< Data_Swarm::new ( ) >>

=head3 C<< Data_Swarm::new ( $port ) >>

=head3 C<< Data_Swarm::new ( port => ..., name => ..., catalog => ..., shutdown => ..., transactions_log => ..., stats_log => ..., debug_log => ...) >>

Create a new work queue.

=over 12

=item port

A single number indicating the port number to listen on, or an array of the
form [lower, upper], which indicates the inclusive range of available ports from which one is chosen at random. If
not specified, the default 9123 is chosen. If zero, a port is chosen at random.

=item name

The project name to use.

=item catalog

Whether or not to enable catalog mode.

=item transactions_log

The name of a file to write the queue's transactions log.

=item stats_log

The name of a file to write the queue's statistics log.

=item debug_log

The name of a file to write the queue's debug log.

=item shutdown

Automatically shutdown workers when queue is finished. Disabled by default.

=back

		my $q = Data_Swarm->new( port => 0, name => 'my_queue' );

See data_Swarm_create in the C API for more information about environmental variables that affect the behavior this method.

=head3 C<name>

Get the project name of the queue.

		 print $q->name;

=head3 C<port>

Get the listening port of the queue.

		 print $q->port

=head3 C<stats>

Get the manager's queue statistics.

		 print $q->stats->{workers_busy};

=head3 C<stats_hierarchy>

Get the queue statistics, including manager and foremen.

		 print $q->stats_hierarchy->{workers_busy};

=head3 C<stats_category>

Get the tasks statistics from the particular category.

		 $s = $q->stats_category("my_category")
		 print $s->{tasks_waiting}

=head3 C<specify_category_mode>

Turn on or off first-allocation labeling for a given category. By default, only
cores, memory, and disk resources are labeled, and gpus are unlabeled. Turn on/off specific resources
with C<specify_category_autolabel_resource>.  NOTE: autolabeling is only
meaningfull when task monitoring is enabled (C<enable_monitoring>). When
monitoring is enabled and a task exhausts resources in a worker, mode dictates
how work queue handles the exhaustion:

=over 12

=item category

A category name. If undefined, sets the mode by default for newly created categories.

=item mode

One of @ref category_mode_t:

=back

=over 24

=item $Data_Swarm::DATA_SWARM_ALLOCATION_MODE_FIXED

Task fails (default).

=item $Data_Swarm::DATA_SWARM_ALLOCATION_MODE_MAX

If maximum values are specified for cores, memory, disk, or gpus (e.g. via C<specify_max_category_resources> or C<specify_memory>), and one of those
resources is exceeded, the task fails.  Otherwise it is retried until a large
enough worker connects to the manager, using the maximum values specified, and
the maximum values so far seen for resources not specified. Use
C<specify_max_retries> to set a limit on the number of times work queue attemps
to complete the task.

=item $Data_Swarm::DATA_SWARM_ALLOCATION_MODE_MIN_WASTE

As above, but work queue tries allocations to minimize resource waste.

=item $Data_Swarm::DATA_SWARM_ALLOCATION_MODE_MAX_THROUGHPUT

As above, but work queue tries allocations to maximize throughput.

=back


=head3 C<specify_category_autolabel_resource>

Turn on or off first-allocation labeling for a given category and resource.
This function should be use to fine-tune the defaults from
C<specify_category_mode>.

=over 12

=item category

A category name.

=item resource

A resource name.

=item autolabel

0/1 for off/on.

=back


=head3 C<enable_monitoring($dir_name, $watchdog)>

Enables resource monitoring of tasks in the queue, and writes a summary per
task to the directory given. Additionally, all summaries are consolidate into
the file all_summaries-PID.log

Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).

=over 12

=item dirname    Directory name for the monitor output.

=item watchdog   If non-zero, kill tasks that exhaust their declared resources. (if not given, defaults to 1)

=back

=head3 C<enable_monitoring_full($dir_name, $watchdog)>

As @ref enable_monitoring, but it also generates a time series and a debug
file.  WARNING: Such files may reach gigabyte sizes for long running tasks.

Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).

=over 12

=item dirname    Directory name for the monitor output.

=item watchdog   If non-zero, kill tasks that exhaust their declared resources. (if not given, defaults to 1)

=back

=head3 C<enable_monitoring_snapshots($filename)>

When monitoring, indicates a file that when present, directs the resource
monitor to take a snapshot of the resources. Snapshots appear in the JSON
summary file of the task, under the key "snapshots". The file is removed after
the snapshot, so that a new snapshot can be taken when it is recreated by a
task. Optionaly, the first line of the file can be used to give an identifying
label to the snapshot.

=over 12

=item self 	Reference to the current work queue object.

item signal_file Name of the file which presence directs the resource monitor to take a snapshot. After the snapshot, THIS FILE IS REMOVED.

=back


=head3 C<activate_fast_abort>

Turn on or off fast abort functionality for a given queue for tasks without
an explicit category. Given the multiplier, abort a task which running time is
larger than the average times the multiplier.  Fast-abort is computed per task
category. The value specified here applies to all the categories for which @ref
activate_fast_abort_category was not explicitely called.

=over 12

=item multiplier

The multiplier of the average task time at which point to abort; if less than zero, fast_abort is deactivated (the default).

=back


=head3 C<activate_fast_abort_category>

Turn on or off fast abort functionality for a given category. Given the
multiplier, abort a task which running time is larger than the average times
the multiplier.  The value specified here applies only to tasks in the given
category.  (Note: data_Swarm_activate_fast_abort_category(q, "default", n) is
the same as data_Swarm_activate_fast_abort(q, n).)

=over 12

=item name

The name of the category.

=item multiplier

The multiplier of the average task time at which point to abort; if zero,
fast_abort is deactivated. If less than zero (default), use the fast abort of
the "default" category.

=back



=head3 C<specify_draining_by_hostname>

Set draining mode for workers at hostname.

=over12

=item The hostname the host running the workers.

=item If 0, workers at hostname work as usual, else no new tasks are dispatched, and empty workers are shutdown.

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

=item $Data_Swarm::DATA_SWARM_SCHEDULE_FCFS

=item $Data_Swarm::DATA_SWARM_SCHEDULE_FILES

=item $Data_Swarm::DATA_SWARM_SCHEDULE_TIME

=item $Data_Swarm::DATA_SWARM_SCHEDULE_RAND

=back

=back

=head3 C<specify_task_order>

Set the order for dispatching submitted tasks in the queue to workers:

=over 12

=item order

One of the following algorithms to use in dispatching

=over 24

=item $Data_Swarm::DATA_SWARM_TASK_ORDER_FIFO

=item $Data_Swarm::DATA_SWARM_TASK_ORDER_LIFO

=back

=back

=head3 C<specify_name>

Change the project name for the given queue.

=over 12

=item name

The new project name.

=back

=head3 C<specify_min_taskid>


Set the minimum taskid of future submitted tasks.

Further submitted tasks are guaranteed to have a taskid larger or equal to
minid.  This function is useful to make taskids consistent in a workflow that
consists of sequential managers. (Note: This function is rarely used).  If the
minimum id provided is smaller than the last taskid computed, the minimum id
provided is ignored.

Returns the actual minimum taskid for future tasks.

=over 12

=item q

A work queue object.

=item minid

Minimum desired taskid

=back

=head3 C<specify_priority>

Change the project priority for the given queue.

=over 12

=item priority

An integer that presents the priorty of this work queue manager. The higher the value, the higher the priority.

=back

=head3 C<specify_num_tasks_left>

Specify the number of tasks not yet submitted to the queue.
It is used by data_Swarm_factory to determine the number of workers to launch.
If not specified, it defaults to 0.
data_Swarm_factory considers the number of tasks as:
num tasks left + num tasks running + num tasks read.

=over 12

=item ntasks

ntasks Number of tasks yet to be submitted.

=back


=head3 C<specify_manager_mode>

Specify the manager mode for the given queue.

=over 12

=item mode

This may be one of the following values:

=over 24

=item $Data_Swarm::DATA_SWARM_MASTER_MODE_STANDALONE

=item $Data_Swarm::DATA_SWARM_MASTER_MODE_CATALOG.

=back

=back

=head3 C<specify_catalog_server>

Specify the catalog server the manager should report to.

=over 12

=item hostname

The hostname of the catalog server.

=item port

The port the catalog server is listening on.

=back

=head3 C<specify_log>

Specify a log file that records cummulative stats of connected workers and
submitted tasks.

=over 12

=item logfile

Name of the file to write the log. If the file exists, then new records are appended.

=back

=head3 C<specify_transactions_log>

Specify a log file that records the states of submitted tasks.

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

The taskid returned from Data_Swarm->submit.

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

=head3 C<block_host>

Block workers running on host from working to this queue.

=over 12

=item host

The hostname the host running the workers.

=back

=head3 C<block_host_with_timeout>

Temporarily block workers running on host timeout seconds.

=over 12

=item host

The hostname the host running the workers.

=item timeout

The duration of the block.

=back

=head3 C<unblock_host>

Unblock workers in host from work for the queue. Clear all blocks if host not provided.

=over 12

=item host

The of the hostname the host.

=back

=head3 C<invalidate_cache_file>

Delete file from workers's caches.

=over 12

=item self

Reference to the current work queue object.

=item local_name

Name of the file as seen by the manager.

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

Turn on manager capacity measurements.

=head3 C<activate_worker_waiting>

Wait for at least n workers to connect before continuing.

=over 12

=item n

Number of workers.

=back

=head3 C<tune>

Tune advanced parameters for work queue.  Return 0 on succes, -1 on failure.

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

=item "transfer-outlier-factor"

Transfer that are this many times slower than the average will be aborted.  (default=10x)

=item "default-transfer-rate"

The assumed network bandwidth used until sufficient data has been collected.  (1MB/s)

=item "fast-abort-multiplier"

Set the multiplier of the average task time at which point to abort; if negative or zero fast_abort is deactivated. (default=0)

=item "keepalive-interval"

Set the minimum number of seconds to wait before sending new keepalive checks to workers. (default=300)

=item "keepalive-timeout"

Set the minimum number of seconds to wait for a keepalive response from worker before marking it as dead. (default=30)

=item value The value to set the parameter to.

=item "short-timeout"

Set the minimum timeout when sending a brief message to a single worker. (default=5s)

=item "long-timeout"

Set the minimum timeout when sending a brief message to a foreman. (default=1h)

=item "category-steady-n-tasks"

Set the number of tasks considered when computing category buckets.

=back

=head3 C<specify_max_resources>

Specifies the max resources for tasks without an explicit category ("default"
category).  rm specifies the maximum resources a task in the default category
may use.

=over 12

=item rm

Hash reference indicating maximum values. See @resources_measured for possible fields.

=back

A maximum of 4 cores is found on any worker:

		q->specify_max_resources({'cores' => 4});

A maximum of 8 cores, 1GB of memory, and 10GB disk are found on any worker:

		q->specify_max_resources({'cores' => 8, 'memory' => 1024, 'disk' => 10240});


=head3 C<specify_min_resources>

Specifies the min resources for tasks without an explicit category ("default"
category).  rm specifies the minimum resources a task in the default category
may use.

=over 12

=item rm

Hash reference indicating minimum values. See @resources_measured for possible fields.

=back

A minimum of 2 cores is found on any worker:

		$q->specify_min_resources({'cores' => 2});

A minimum of 4 cores, 1GB of memory, and 10GB disk are found on any worker:

		$q->specify_min_resources({'cores' => 4, 'memory' => 1024, 'disk' => 10240});


=head3 C<specify_category_max_resources>

Specifies the max resources for tasks in the given category.

=over 12

=item category

Name of the category

=item rm

Hash reference indicating maximum values. See @resources_measured for possible fields.

A maximum of 4 cores is found on any worker:

		$q->specify_category_max_resources('my_category', {'cores' => 4});

A maximum of 8 cores, 1GB of memory, and 10GB disk are found on any worker:

		$q->specify_category_max_resources('my_category', {'cores' => 8, 'memory' => 1024, 'disk' => 10240});


=head3 C<specify_category_min_resources>

Specifies the min resources for tasks in the given category.

=over 12

=item category

Name of the category

=item rm

Hash reference indicating minimum values. See @resources_measured for possible fields.

A minimum of 2 cores is found on any worker:

		q->specify_category_min_resources('my_category', {'cores' => 2});

A minimum of 2 cores, 1GB of memory, and 10GB disk are found on any worker:

		q->specify_category_min_resources('my_category', {'cores' => 2, 'memory' => 1024, 'disk' => 10240});


=head3 C<specify_category_first_allocation_guess

Specifies the first-allocation guess for the given category

=over 12

=item category

Name of the category

=item rm


=head3 C<initialize_categories>

Initialize first value of categories

=over 12

=item rm

Hash reference indicating maximum values. See @resources_measured for possible fields.

=item filename

JSON file with resource summaries.

=back


=head3 C<submit>

Submit a task to the queue.

=over 12

=item task

A task description created from Data_Swarm::Task.

=back

		$q->submit($task);

=head3 C<wait>

Wait for tasks to complete.

This call will block until the timeout has elapsed

=over 12

=item timeout

The number of seconds to wait for a completed task back before
returning.  Use an integer to set the timeout or the constant
$Data_Swarm::DATA_SWARM_WAITFORTASK to block until a task has
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
