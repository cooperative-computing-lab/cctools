######################################################################
# Copyright (C) 2014- The University of Notre Dame
# This software is distributed under the GNU General Public License.
#    See the file COPYING for details.
######################################################################

### See module documentation at the end of this file.

package Work_Queue;
use work_queue;
    
sub Work_Queue::new {
    my $class = shift;
    unshift @_, 'port' if @_ == 1;
    my %args = @_;
    $args{port} //= $Work_Queue::WORK_QUEUE_DEFAULT_PORT;
    my $_work_queue = work_queue::work_queue_create($args{port});
    die "Could not create a work queue on port $port" unless $_work_queue;
    my $_stats           = work_queuec::new_work_queue_stats();
    my $_stats_hierarchy = work_queuec::new_work_queue_stats();
    my $q = bless {
	_work_queue      => $_work_queue, 
	_task_table      => {}, 
	_stats           => $_stats, 
	_stats_hierarchy => $_stats_hierarchy, 
	_shutdown        => $args{shutdown} // 0 
    }, $class;
    $q->specify_name($args{name})           if $args{name};
    $q->specify_master_mode($args{catalog}) if $args{catalog};
    $q;
}

sub DESTROY {
    my ($self) = @_;
    $q->shutdown_workers(0) if $self->{_shutdown};
    work_queue_delete($self->{_work__queue});
}

sub set_debug_flag {
    my $self = shift;
    
    foreach my $flag (@_) {
	cctools_debug_flags_set($flag);
    }
}

sub set_debug_config_file {
    my ($self, $filename) = @_;
    cctools_debug_config_file($filename);
}

sub name {
    my $self = shift;
    work_queue_name($self->{_work_queue})
}

sub port {
    my ($self) = @_;
    work_queue_port($self->{_work_queue})
}

sub stats {
    my ($self) = @_;
    work_queue_get_stats($self->{_work_queue}, $self->{_stats});
    $self->{_stats}
}

sub stats_hierarchy {
    my ($self) = @_;
    work_queue_get_stats_hierarchy($self->{_work_queue}, $self->{_stats_hierarchy});
    $self->{_stats_hierarchy}
}

sub enable_monitoring {
    my ($self, $summary_file) = @_;
    work_queue_enable_monitoring($self->{_work_queue}, $summary_file);
}

sub activate_fast_abort {
    my ($self, $multiplier) = @_;
    work_queue_activate_fast_abort($self->{_work_queue}, $multiplier);
}

sub empty {
    my ($self) = @_;
    work_queue_empty($self->{_work_queue});
}

sub hungry {
    my ($self) = @_;
    work_queue_hungry($self->{_work_queue});
}

sub specify_algorithm {
    my ($self, $algorithm) = @_;
    work_queue_specify_algorithm($self->{_work_queue}, $algorithm);
}

sub specify_task_order {
    my ($self, $order) = @_;
    work_queue_specify_task_order($self->{_work_queue}, $order);
}

sub specify_name {
    my ($self, $name) = @_;
    work_queue_specify_name($self->{_work_queue}, $name);
}

sub specify_priority {
    my ($self, $priority) = @_;
    work_queue_specify_priority($self->{_work_queue}, $priority);
}

sub specify_master_mode {
    my ($self, $mode) = @_;
    work_queue_specify_master_mode($self->{_work_queue}, $mode);
}

sub specify_catalog_server {
    my ($self, $hostname, $port) = @_;
    work_queue_specify_catalog_server($self->{_work_queue}, $hostname, $port);
}

sub specify_log {
    my ($self, $logfile) = @_;
    work_queue_specify_log($self->{_work_queue}, $logfile);
}

sub specify_password {
    my ($self, $password) = @_;
    work_queue_specify_password($self->{_work_queue}, $password);
}
    

sub specify_password_file {
    my ($self, $file) = @_;
    work_queue_specify_password_file($self->{_work_queue}, $file);
}

sub cancel_by_taskid {
    my ($self, $id) = @_;
    work_queue_cancel_by_taskid($self->{_work_queue}, $id);
}

sub cancel_by_tasktag {
    my ($self, $tag) = @_;
    work_queue_cancel_by_tasktag($self->{_work_queue}, $tag);
}

sub shutdown_workers {
    my ($self, $n) = @_;
    work_queue_shutdown_workers($self->{_work_queue}, $n);
}

sub specify_keepalive_interval {
    my ($self, $interval) = @_;
    work_queue_specify_keepalive_interval($self->{_work_queue}, $interval);
}

sub specify_keepalive_timeout {
    my ($self, $timeout) = @_;
    work_queue_specify_keepalive_timeout($self->{_work_queue}, $timeout);
}

sub estimate_capacity {
    my ($self) = @_;
    work_queue_specify_estimate_capacity_on($self->{_work_queue}, 1);
}

sub activate_worker_waiting {
    my ($self, $workers) = @_;
    work_queue_activate_worker_waiting($self->{_work_queue}, $workers);
}

sub tune {
    my ($self, $name, $value) = @_;
    work_queue_tune($self->{_work_queue}, $name, $value);
}
           	
sub submit {
    my ($self, $task) = @_;
    my $taskid = work_queue_submit($self->{_work_queue}, $task->{_task});
    $self->{_task_table}{$taskid} = $task;
    $taskid;
}

sub wait {
    my ($self, $timeout) = @_;
    $timeout //= $WORK_QUEUE_WAITFORTASK;
    my $_task = work_queue_wait($self->{_work_queue}, $timeout);
    if($_task) {
	my $task  = $self->{_task_table}{$_task->{taskid}};
	$self->{_task_table}{$_task->{taskid}} = undef;
	$task;
    } else {
	0
    }
}




package Work_Queue::Task;
use work_queue;

sub Work_Queue::Task::new {
    my ($class, $command) = @_;
    my $_task = work_queue_task_create($command);
    die "Could not create task." unless $_task;
    bless {_task => $_task}, $class;
}
sub DESTROY {
    my $self = shift;
    work_queue_task_delete($self->{_task}) if $self->{_task};
}
sub _determine_file_flags {
    my ($flags, $cache) = @_;
    
    $flags //= $WORK_QUEUE_CACHE;
    if($cache) {
	$flags |= $WORK_QUEUE_CACHE;
    } else {
	$flags &= ~$WORK_QUEUE_CACHE;
    }
    $flags;
}

sub specify_tag {
    my ($self, $tag) = @_;
    
    work_queue_task_specify_tag($self->{_task}, $tag);;
}

sub clone {
    my ($self) = @_;
    my $copy = $self;
    $copy->{_task} = work_queue_task_clone($self->{_task});
}

sub specify_command {
    my ($self) = @_;
    
    work_queue_task_specify_command($self->{_task});;
}

sub specify_algorithm {
    my ($self, $algorithm) = @_;
    
    work_queue_task_specify_algorithm($self->{_task}, $algorithm);
}

sub specify_preferred_host {
    my ($self, $host) = @_;
    
    work_queue_task_specify_preferred_host($self->{_task}, $host);
}

sub specify_file {
    my $self = shift;
    my %args = @_;
    die "At least local_name should be specified." unless $args{local_name};
    $args{remote_name} //= $args{local_name};
    $args{type}        //= $WORK_QUEUE_INPUT;
    $args{cache}       //= 1;
    $args{flags}         = _determine_file_flags($args{flags}, $args{cache});
       
    work_queue_task_specify_file($self->{_task}, $args{local_name}, $args{remote_name}, $args{type}, $args{flags});
} 

sub specify_file_piece {
    my $self = shift;
    my %args = @_;
    die "At least local_name should be specified." unless $args{local_name};
    $args{remote_name} //= $args{local_name};
    $args{start_byte}  //= 0;
    $args{end_byte}    //= 0;
    $args{type}        //= $WORK_QUEUE_INPUT;
    $args{cache}       //= 1;    
    $args{flags}         = _determine_file_flags($args{flags});
    
    work_queue_task_specify_file_piece($self->{_task}, $args{local_name}, $args{remote_name}, $args{start_byte}, $args{end_byte}, $args{type}, $args{flags});
}

sub specify_input_file {
    my $self = shift;
    unshift @_, 'local_name', if @_ == 1;
    my %args = @_;
    $self->specify_file(local_name  => $args{local_name}, 
			remote_name => $args{remote_name}, 
			type        => $WORK_QUEUE_INPUT,
			flags       => $args{flags},
			cache       => $args{cache});
}

sub specify_output_file {
    my $self = shift;
    
    unshift @_, 'local_name', if @_ == 1;
    my %args = @_;
    
    $self->specify_file(local_name  => $args{local_name}, 
			remote_name => $args{remote_name}, 
			type        => $WORK_QUEUE_OUTPUT,
			flags       => $args{flags},
			cache       => $args{cache});
}

sub specify_directory {
    my $self = shift;
    my %args = @_;
    die "At least local_name should be specified." unless $args{local_name};
    
    $args{remote_name} //= $args{local_name};
    $args{type}        //= $WORK_QUEUE_INPUT;
    $args{recursive}   //= 0;
    
    $args{cache}       //= 1;
    $args{flags}         = _determine_file_flags($args{flags}, $args{cache});
    work_queue_task_specify_directory($self->{_task}, $args{local_name}, $args{type}, $args{flags}, $args{recursive});
}

sub specify_buffer {
    my $self = shift;
    my %args = @_;
    die "The buffer and remote_name should be specified." unless ($args{remote_name} and $args{buffer});
    $args{cache}       //= 1;
    $args{flags}         = _determine_file_flags($args{flags}, $args{cache});
    
    work_queue_task_specify_buffer($self->{_task}, $args{buffer}, $args{remote_name}, $args{flags}, $args{cache});
}

sub specify_cores {
    my ($self, $cores) = @_;
    work_queue_task_specify_cores($self->{_task}, $cores);
}

sub specify_memory {
    my ($self, $memory) = @_;
    work_queue_task_specify_memory($self->{_task}, $memory);
}

sub specify_disk {
    my ($self, $disk) = @_;
    work_queue_task_specify_disk($self->{_task}, $disk);
}

sub specify_gpus {
    my ($self, $gpus) = @_;
    work_queue_task_specify_gpus($self->{_task}, $gpus);
}

sub specify_end_time {
    my ($self, $seconds) = @_;
    work_queue_task_specify_end_time($self->{_task}, $seconds);
}

sub specify_priority {
    my ($self, $priority) = @_;
    work_queue_task_specify_priority($self->{_task}, $priority);
}

sub tag {
    my ($self) = @_;
    $self->{_task}->{tag};
}

sub priority {
    my ($self) = @_;
    $self->{_task}->{priority};
}

sub command {
    my ($self) = @_;
    $self->{_task}->{command_line};
}

sub algorithm {
    my ($self) = @_;
    $self->{_task}->{algorithm};
}

sub output {
    my ($self) = @_;
    $self->{_task}->{output};
}

sub id {
    my ($self) = @_;
    $self->{_task}->{taskid};
}

sub return_status {
    my ($self) = @_;
    $self->{_task}->{return_status};
}

sub result {
    my ($self) = @_;
    $self->{_task}->{result};
}

sub total_submissions {
    my ($self) = @_;
    $self->{_task}->{total_submissions};
}

sub host {
    my ($self) = @_;
    $self->{_task}->{host};
}

sub hostname {
    my ($self) = @_;
    $self->{_task}->{hostname};
}

sub commit_time {
    my ($self) = @_;
    $self->{_task}->{time_committed};
}


sub submit_time {
    my ($self) = @_;
    $self->{_task}->{time_task_submit};
}

sub finish_time {
    my ($self) = @_;
    $self->{_task}->{time_task_finish};
}
    

sub time_app_delay {
    my ($self) = @_;
    $self->{_task}->{time_app_delay};
}

sub send_input_start {
    my ($self) = @_;
    $self->{_task}->{time_send_input_start};
}

sub send_input_finish {
    my ($self) = @_;
    $self->{_task}->{time_send_input_finish};
}

sub execute_cmd_start {
    my ($self) = @_;
    $self->{_task}->{time_execute_cmd_start};
}

sub execute_cmd_finish {
    my ($self) = @_;
    $self->{_task}->{time_execute_cmd_finish};
}

sub receive_output_start {
    my ($self) = @_;
    $self->{_task}->{time_receive_output_start};
}

sub receive_output_finish {
    my ($self) = @_;
    $self->{_task}->{time_receive_output_finish};
}

sub total_bytes_received {
    my ($self) = @_;
    $self->{_task}->{total_bytes_received};
}

sub total_bytes_sent {
    my ($self) = @_;
    $self->{_task}->{total_bytes_sent};
}

sub total_bytes_transferred {
    my ($self) = @_;
    $self->{_task}->{total_bytes_transferred};
}

sub total_transfer_time {
    my ($self) = @_;
    $self->{_task}->{total_transfer_time};
}

sub cmd_execution_time {
    my ($self) = @_;
    $self->{_task}->{cmd_execution_time};
}

sub total_cmd_execution_time {
    my ($self) = @_;
    $self->{_task}->{total_cmd_execution_time};
}

sub resources_measured {
    my ($self) = @_;
    $self->{_task}->{resources_measured};
}
1;

__END__

=head1 NAME

Work_Queue - Perl Work Queue bindings.

=head1 SYNOPSIS 

The objects and methods provided by this package correspond to the
native C API in work_queue.h.

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

=head3 C<enable_monitoring($summary_file)>

Enables resource monitoring of tasks in the queue. And writes a
summary of the monitored information to a file.

 Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).

=over 12

=item summaryfile 

Filename for the summary log (If NULL, writes to wq-\<pid\>-resource-usage).

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


