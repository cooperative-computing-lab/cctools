#!/usr/bin/env python2

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Python-WorkQueue test """

from workqueue import Task, WorkQueue, set_debug_flag
from workqueue import WORK_QUEUE_SCHEDULE_FCFS, WORK_QUEUE_SCHEDULE_FILES

import os
import sys
import time

set_debug_flag('debug')
set_debug_flag('wq')

wq = WorkQueue()

wq.specify_algorithm(WORK_QUEUE_SCHEDULE_FCFS)
wq.specify_name('work_queue_example')

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

    task.specify_input_buf('hello from %d' % i, ifile)
    task.specify_output_file(ofile, ofile)

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
    wq.submit(task)

if wq.hungry():
    print 'work queue is hungry'

wq.activate_fast_abort(1.5)

while not wq.empty():
    t = wq.wait(10)
    if t:
	print t.taskid, t.return_status, t.result, t.host
	print t.submit_time, t.start_time, t.finish_time
	print t.output,
    
    print wq.stats.workers_init, wq.stats.workers_ready, wq.stats.workers_busy, \
	  wq.stats.tasks_running, wq.stats.tasks_waiting, wq.stats.tasks_complete

wq.shutdown_workers(0)

print wq.stats.total_tasks_dispatched, wq.stats.total_tasks_complete, \
      wq.stats.total_workers_joined, wq.stats.total_workers_removed

# vim: sts=4 sw=4 ts=8 ft=python
