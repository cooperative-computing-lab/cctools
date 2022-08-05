#!/usr/bin/env python

import work_queue
import os

work_queue.set_debug_flag('all')

ds = work_queue.DataSwarm(port=work_queue.DS_RANDOM_PORT, exclusive=False, shutdown=True)
ds.specify_name('test')

for i in range(5):
    task = work_queue.Task('date')
    task.specify_algorithm(work_queue.DS_SCHEDULE_FCFS)
    task.specify_tag('current date/time [%d]' % i)
    task.specify_input_file('/bin/date')

    print task.id
    print task.algorithm
    print task.command
    print task.tag

    ds.submit(task)

os.environ['PATH'] = '../../../work_queue/src:' + os.environ['PATH']
os.system('ds_worker -d all -t 5 localhost %d &' % ds.port)

while not ds.empty():
    print '** wait for task'
    task = ds.wait(1)
    if task:
	print 'task'
	print 'algorithm', task.algorithm
	print 'command', task.command
	print 'tag', task.tag
	print 'output', task.output
	print 'id', task.id
	print task.return_status
	print task.result
	print task.host
	print task.submit_time
	print task.finish_time
	print task.app_delay
	print task.send_input_start
	print task.send_input_finish
	print task.execute_cmd_start
	print task.execute_cmd_finish
	print task.receive_output_start
	print task.receive_output_finish
	print task.total_bytes_transferred
	print task.total_transfer_time
	print task.cmd_execution_time
	del task
    print '** work queue'
    print ds.stats.workers_init
    print ds.stats.workers_ready
    print ds.stats.workers_busy
    print ds.stats.tasks_running
    print ds.stats.tasks_waiting
    print ds.stats.tasks_complete
    print ds.stats.total_tasks_dispatched
    print ds.stats.total_tasks_complete
    print ds.stats.total_workers_joined
    print ds.stats.total_workers_removed
    print ds.stats.total_bytes_sent
    print ds.stats.total_bytes_received
    print ds.stats.total_send_time
    print ds.stats.total_receive_time
    print ds.stats.efficiency
    print ds.stats.idle_percentage
    print ds.stats.capacity
    print ds.stats.avg_capacity
    print ds.stats.total_workers_connected
