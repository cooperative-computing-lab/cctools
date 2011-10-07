#!/usr/bin/env python2

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Python-WorkQueue test """

from workqueue import Task, WorkQueue, set_debug_flag
from workqueue import WORK_QUEUE_SCHEDULE_FCFS, WORK_QUEUE_SCHEDULE_FILES
from workqueue import WORK_QUEUE_RANDOM_PORT
from workqueue import WORK_QUEUE_OUTPUT
#from workqueue import WORK_QUEUE_MASTER_MODE_STANDALONE, WORK_QUEUE_WORKER_MODE_SHARED

import os
import sys
import time

set_debug_flag('debug')
set_debug_flag('wq')

wq = WorkQueue(WORK_QUEUE_RANDOM_PORT, name='workqueue_example', catalog=False, exclusive=False)
os.system('work_queue_worker -d all localhost %d &' % wq.port)

wq.specify_algorithm(WORK_QUEUE_SCHEDULE_FCFS)
#wq.specify_name('workqueue_example')
#wq.specify_master_mode(WORK_QUEUE_MASTER_MODE_STANDALONE)
#wq.specify_worker_mode(WORK_QUEUE_WORKER_MODE_SHARED)

if wq.empty():
    print 'work queue is empty'

outputs = []

for i in range(5):
    ifile = 'msg.%d' % i
    ofile = 'out.%d' % i
    task  = Task('cat < %s > %s' % (ifile, ofile))

    task.specify_tag(str(i))
    print task.command, task.tag
    
    task.tag = str(time.time())
    print task.command, task.tag
    
    task.algorithm = WORK_QUEUE_SCHEDULE_FILES
    print task.command, task.algorithm

    task.specify_input_buffer('hello from %d' % i, ifile, cache=False)
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
	print t.taskid, t.return_status, t.result, t.host
	print t.preferred_host, t.status
	print t.submit_time, t.start_time, t.finish_time
	print t.transfer_start_time, t.computation_time
	print t.total_bytes_transferred, t.total_transfer_time
	print t.output
    
    print wq.stats.workers_init, wq.stats.workers_ready, wq.stats.workers_busy, \
	  wq.stats.tasks_running, wq.stats.tasks_waiting, wq.stats.tasks_complete

wq.shutdown_workers(0)

print wq.stats.total_tasks_dispatched, wq.stats.total_tasks_complete, \
      wq.stats.total_workers_joined, wq.stats.total_workers_removed

# vim: sts=4 sw=4 ts=8 ft=python
