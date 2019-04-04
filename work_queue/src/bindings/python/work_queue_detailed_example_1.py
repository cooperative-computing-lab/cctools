#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Python-WorkQueue test """

from work_queue import Task, WorkQueue, set_debug_flag
from work_queue import WORK_QUEUE_SCHEDULE_FCFS, WORK_QUEUE_SCHEDULE_FILES
from work_queue import WORK_QUEUE_RANDOM_PORT
from work_queue import WORK_QUEUE_OUTPUT
#from workqueue import WORK_QUEUE_MASTER_MODE_STANDALONE, WORK_QUEUE_WORKER_MODE_SHARED
from work_queue import WORK_QUEUE_TASK_ORDER_LIFO

import os
import sys
import time

set_debug_flag('debug')
set_debug_flag('wq')

wq = WorkQueue(WORK_QUEUE_RANDOM_PORT, name='workqueue_example', catalog=True, exclusive=False)
os.environ['PATH'] = '../../../work_queue/src:' + os.environ['PATH']
os.system('work_queue_worker -d all localhost %d &' % wq.port)

print wq.name

wq.specify_algorithm(WORK_QUEUE_SCHEDULE_FCFS)
#wq.specify_name('workqueue_example')
#wq.specify_master_mode(WORK_QUEUE_MASTER_MODE_STANDALONE)
#wq.specify_worker_mode(WORK_QUEUE_WORKER_MODE_SHARED)
wq.specify_task_order(WORK_QUEUE_TASK_ORDER_LIFO)

if wq.empty():
    print 'work queue is empty'

outputs = []

for i in range(5):
    ifile = 'msg.%d' % i
    ofile = 'out.%d' % i
    task  = Task('cat < %s > %s' % (ifile, ofile))

    task.specify_tag(str(time.time()))
    print task.command, task.tag

    task.specify_algorithm(WORK_QUEUE_SCHEDULE_FILES)
    print task.command, task.algorithm

    task.specify_buffer('hello from %d' % i, ifile, cache=False)
    if i % 2:
	task.specify_output_file(ofile, cache=False)
    else:
	task.specify_file(ofile, type=WORK_QUEUE_OUTPUT, cache=False)

    outputs.append(ofile)
    wq.submit(task)

if wq.empty():
    print 'work queue is empty'

while not wq.empty():
    t = wq.wait(10)
    if t:
	print t.tag

    print wq.stats.workers_init, wq.stats.workers_ready, wq.stats.workers_busy, \
	  wq.stats.tasks_running, wq.stats.tasks_waiting, wq.stats.tasks_complete

map(os.unlink, outputs)

for i in range(5):
    task = Task('hostname && date +%s.%N')
    task.specify_input_file('/bin/hostname')
    wq.submit(task)

if wq.hungry():
    print 'work queue is hungry'

wq.activate_fast_abort(1.5)

while not wq.empty():
    t = wq.wait(1)
    if t:
	print t.id, t.return_status, t.result, t.host
	print t.submit_time, t.finish_time, t.app_delay
	print t.send_input_start, t.send_input_finish
	print t.execute_cmd_start, t.execute_cmd_finish
	print t.receive_output_start, t.receive_output_finish
	print t.total_bytes_transferred, t.total_transfer_time
	print t.cmd_execution_time
	print t.output

    print wq.stats.workers_init, wq.stats.workers_ready, wq.stats.workers_busy, \
	  wq.stats.tasks_running, wq.stats.tasks_waiting, wq.stats.tasks_complete

wq.shutdown_workers(0)

print wq.stats.total_tasks_dispatched, wq.stats.total_tasks_complete, \
      wq.stats.total_workers_joined, wq.stats.total_workers_removed

# vim: sts=4 sw=4 ts=8 ft=python
