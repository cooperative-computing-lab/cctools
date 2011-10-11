#!/usr/bin/env python2

import work_queue
import os

work_queue.set_debug_flag('all')

wq = work_queue.WorkQueue(port=work_queue.WORK_QUEUE_RANDOM_PORT, exclusive=False, shutdown=True)
wq.specify_name('test')

for i in range(5):
    task = work_queue.Task('date')
    task.specify_algorithm(work_queue.WORK_QUEUE_SCHEDULE_FCFS)
    task.specify_tag('current date/time [%d]' % i)
    task.specify_input_file('/bin/date')

    print task.id
    print task.algorithm
    print task.command
    print task.tag

    wq.submit(task)

os.system('work_queue_worker -d all -t 5 localhost %d &' % wq.port)

while not wq.empty():
    print '** wait for task'
    task = wq.wait(1)
    if task:
    	print 'task'
    	print 'algorithm', task.algorithm
    	print 'command', task.command
    	print 'tag', task.tag
    	print 'output', task.output
    	print 'id', task.id
    	print task.preferred_host
    	print task.status
    	print task.return_status
    	print task.result
    	print task.host
    	print task.submit_time
    	print task.start_time
    	print task.finish_time
    	print task.transfer_start_time
    	print task.computation_time
    	print task.total_bytes_transferred
    	print task.total_transfer_time
    	del task
    print '** work queue'
    print wq.stats.workers_init
    print wq.stats.workers_ready
    print wq.stats.workers_busy
    print wq.stats.tasks_running
    print wq.stats.tasks_waiting
    print wq.stats.tasks_complete
    print wq.stats.total_tasks_dispatched
    print wq.stats.total_tasks_complete
    print wq.stats.total_workers_joined
    print wq.stats.total_workers_removed
    print wq.stats.total_bytes_sent
    print wq.stats.total_bytes_received
    print wq.stats.total_send_time
    print wq.stats.total_receive_time
