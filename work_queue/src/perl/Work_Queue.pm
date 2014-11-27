######################################################################
# Copyright (C) 2014- The University of Notre Dame
# This software is distributed under the GNU General Public License.
#    See the file COPYING for details.
######################################################################

package Work_Queue;
use work_queue;

use Data::Dumper;

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

##
# Create a new task specification.
#
# @param self       Reference to the current task object.
# @param command    The shell command line to be exected by the task.

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

##
# Attach a user defined logical name to the task.
#
# @param self       Reference to the current task object.
# @param tag        The tag to be executed. 
sub specify_tag {
    my ($self, $tag) = @_;
    
    work_queue_task_specify_tag($self->{_task}, $tag);;
}

##
# Return a copy of this task (not implemented)
#
sub clone {
    ...;
}

##
# Set the command to be executed by the task.
#
# @param self       Reference to the current task object.
# @param command        The command to be executed.
sub specify_command {
    my ($self) = @_;
    
    work_queue_task_specify_command($self->{_task});;
}

##
# Set the worker selection algorithm for task.
#
# @param self       Reference to the current task object.
# @param algorithm  One of the following algorithms to use in assigning a
#                   task to a worker:
#                   - @ref WORK_QUEUE_SCHEDULE_FCFS
#                   - @ref WORK_QUEUE_SCHEDULE_FILES
#                   - @ref WORK_QUEUE_SCHEDULE_TIME
#                   - @ref WORK_QUEUE_SCHEDULE_RAND
sub specify_algorithm {
    my ($self, $algorithm) = @_;
    
    work_queue_task_specify_algorithm($self->{_task}, $algorithm);
}

##
# Indicate that the task would be optimally run on a given host.
#
# @param self       Reference to the current task object.
# @param hostname   The hostname to which this task would optimally be sent.
sub specify_preferred_host {
    my ($self, $host) = @_;
    
    work_queue_task_specify_preferred_host($self->{_task}, $host);
}

##
# Add a file to the task.
#
# @param self           Reference to the current task object.
# @param local_name     The name of the file on local disk or shared filesystem.
# @param remote_name    The name of the file at the execution site.
# @param type           Must be one of the following values: @ref WORK_QUEUE_INPUT or @ref WORK_QUEUE_OUTPUT
# @param flags          May be zero to indicate no special handling, or any of the following or'd together:
#                       - @ref WORK_QUEUE_NOCACHE
#                       - @ref WORK_QUEUE_CACHE
#                       - @ref WORK_QUEUE_WATCH
# @param cache          Legacy parameter for setting file caching attribute.  By default this is enabled.
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

##
# Add a file piece to the task.
#
# @param self           Reference to the current task object.
# @param local_name     The name of the file on local disk or shared filesystem.
# @param remote_name    The name of the file at the execution site.
# @param start_byte     The starting byte offset of the file piece to be transferred.
# @param end_byte       The ending byte offset of the file piece to be transferred. 
# @param type           Must be one of the following values: @ref WORK_QUEUE_INPUT or @ref WORK_QUEUE_OUTPUT
# @param flags          May be zero to indicate no special handling, or any of the following or'd together:
#                       - @ref WORK_QUEUE_NOCACHE
#                       - @ref WORK_QUEUE_CACHE
# @param cache          Legacy parameter for setting file caching attribute.  By default this is enabled.
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

##
# Add a input file to the task.
#
# This is just a wrapper for @ref specify_file with type set to @ref WORK_QUEUE_INPUT.

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

##
# Add a output file to the task.
#
# This is just a wrapper for @ref specify_file with type set to @ref WORK_QUEUE_OUTPUT.
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

##
# Add a directory to the task.
# @param self           Reference to the current task object.
# @param local_name     The name of the directory on local disk or shared filesystem. Optional if the directory is empty.
# @param remote_name    The name of the directory at the remote execution site.
# @param type           Must be one of the following values: @ref WORK_QUEUE_INPUT or @ref WORK_QUEUE_OUTPUT
# @param flags          May be zero to indicate no special handling, or any of the following or'd together:
#                       - @ref $WORK_QUEUE_NOCACHE
#                       - @ref $WORK_QUEUE_CACHE
# @param recursive      Indicates whether just the directory (0) or the directory and all of its contents (1) should be included.
# @param cache          Legacy parameter for setting file caching attribute.  By default this is enabled.
# @return 1 if the task directory is successfully specified, 0 if either of @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
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

##
# Add an input bufer to the task.
#
# @param self           Reference to the current task object.
# @param buffer         The contents of the buffer to pass as input.
# @param remote_name    The name of the remote file to create.
# @param flags          May take the same values as @ref specify_file.
# @param cache          Legacy parameter for setting file caching attribute.  By default this is enabled.
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

##
# Indicate the maximum end time (in seconds from the Epoch) of this task.
sub specify_end_time {
    my ($self, $seconds) = @_;

    work_queue_task_specify_end_time($self->{_task}, $seconds);
}

# Indicate the the priority of this task (larger means better priority, default is 0).
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

## 
# Get the standard output of the task. Must be called only after the task
# completes execution.
sub output {
    my ($self) = @_;

    $self->{_task}->{output};
}

## 
# Get the task id number. Must be called only after the task was submitted.
sub id {
    my ($self) = @_;

    $self->{_task}->{taskid};
}

## 
# Get the exit code of the command executed by the task. Must be called only
# after the task completes execution.
sub return_status {
    my ($self) = @_;

    $self->{_task}->{return_status};
}

## 
# Get the result of the task (successful, failed return_status, missing input file, missing output file). 
# Must be called only after the task completes execution.
sub result {
    my ($self) = @_;

    $self->{_task}->{result};
}

##
# Get the number of times the task has been resubmitted internally.
# Must be called only after the task completes execution.
sub total_submissions {
    my ($self) = @_;

    $self->{_task}->{total_submissions};
}

## 
# Get the address and port of the host on which the task ran.
# Must be called only after the task completes execution.
sub host {
    my ($self) = @_;

    $self->{_task}->{host};
}

## 
# Get the name of the host on which the task ran.  
# Must be called only after the task completes execution.
sub hostname {
    my ($self) = @_;

    $self->{_task}->{hostname};
}


## 
# Get the time at which this task was committed to a worker.
# Must be called only after the task completes execution.
sub commit_time {
    my ($self) = @_;

    $self->{_task}->{time_committed};
}


## 
# Get the time at which this task was submitted.
# Must be called only after the task completes execution.
sub submit_time {
    my ($self) = @_;

    $self->{_task}->{time_task_submit};
}

## 
# Get the time at which this task was finished. 
# Must be called only after the task completes execution.
sub finish_time {
    my ($self) = @_;

    $self->{_task}->{time_task_finish};
}
    
## 
# Get the time spent in upper-level application (outside of work_queue_wait).
# Must be called only after the task completes execution.
sub time_app_delay {
    my ($self) = @_;

    $self->{_task}->{time_app_delay};
}

## 
# Get the time at which the task started to transfer input files. 
# Must be called only after the task completes execution.
sub send_input_start {
    my ($self) = @_;

    $self->{_task}->{time_send_input_start};
}

## 
# Get the time at which the task finished transferring input files. 
# Must be called only after the task completes execution.
sub send_input_finish {
    my ($self) = @_;

    $self->{_task}->{time_send_input_finish};
}

## 
# The time at which the task began.
# Must be called only after the task completes execution.
sub execute_cmd_start {
    my ($self) = @_;

    $self->{_task}->{time_execute_cmd_start};
}

## 
# Get the time at which the task finished (discovered by the master). 
# Must be called only after the task completes execution.
sub execute_cmd_finish {
    my ($self) = @_;

    $self->{_task}->{time_execute_cmd_finish};
}

## 
# Get the time at which the task started to transfer output files. 
# Must be called only after the task completes execution.
sub receive_output_start {
    my ($self) = @_;

    $self->{_task}->{time_receive_output_start};
}

## 
# Get the time at which the task finished transferring output files. 
# Must be called only after the task completes execution.
sub receive_output_finish {
    my ($self) = @_;

    $self->{_task}->{time_receive_output_finish};
}

## 
# Get the number of bytes received since task started receiving input data.
# Must be called only after the task completes execution.
sub total_bytes_received {
    my ($self) = @_;

    $self->{_task}->{total_bytes_received};
}

## 
# Get the number of bytes sent since task started sending input data.
# Must be called only after the task completes execution.
sub total_bytes_sent {
    my ($self) = @_;

    $self->{_task}->{total_bytes_sent};
}

## 
# Get the number of bytes transferred since task started transferring input data.
# Must be called only after the task completes execution.
sub total_bytes_transferred {
    my ($self) = @_;

    $self->{_task}->{total_bytes_transferred};
}

## 
# Get the time comsumed in microseconds for transferring total_bytes_transferred. 
# Must be called only after the task completes execution.
sub total_transfer_time {
    my ($self) = @_;

    $self->{_task}->{total_transfer_time};
}

## 
# Get the time spent in microseconds for executing the command on the worker. 
# Must be called only after the task completes execution.
sub cmd_execution_time {
    my ($self) = @_;

    $self->{_task}->{cmd_execution_time};
}

## 
# Get the time spent in microseconds for executing the command on any worker. 
# Must be called only after the task completes execution.
sub total_cmd_execution_time {
    my ($self) = @_;

    $self->{_task}->{total_cmd_execution_time};
}

sub resources_measured {
    my ($self) = @_;

    $self->{_task}->{resources_measured};
}

1;
