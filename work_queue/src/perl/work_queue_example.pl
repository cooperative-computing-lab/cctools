#!/usr/bin/env perl

use strict;
use work_queue;

my $wq = work_queue::work_queue_create(int(*work_queue::WORK_QUEUE_RANDOM_PORT));

work_queue::work_queue_specify_name($wq, "work_queue_perl");

print work_queue::work_queue_port($wq) . "\n";

for (my $i = 0; $i < 1000; $i++) {
    my $task = work_queue::work_queue_task_create("date");

    work_queue::work_queue_task_specify_algorithm($task, int(*work_queue::WORK_QUEUE_SCHEDULE_FCFS));
    work_queue::work_queue_task_specify_tag($task, "current date/time " . $i);
    work_queue::work_queue_task_specify_input_file($task, "/bin/date", "date");

    print "$task->{worker_selection_algorithm}\n";
    print "$task->{command_line}\n";
    print "$task->{tag}\n";
    
    work_queue::work_queue_submit($wq, $task);
}

while (not work_queue::work_queue_empty($wq)) {
    my $task = work_queue::work_queue_wait($wq, 1);

    if (defined($task)) {
    	print "$task->{command_line}, $task->{result}, $task->{output}\n";
    }
}
