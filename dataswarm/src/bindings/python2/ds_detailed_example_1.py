#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Python-DataSwarm test """

from work_queue import Task, DataSwarm, set_debug_flag
from work_queue import DS_SCHEDULE_FCFS, DS_SCHEDULE_FILES
from work_queue import DS_RANDOM_PORT
from work_queue import DS_OUTPUT
#from dataswarm import DS_MASTER_MODE_STANDALONE, DS_WORKER_MODE_SHARED
from work_queue import DS_TASK_ORDER_LIFO

import os
import sys
import time

set_debug_flag('debug')
set_debug_flag('ds')

ds = DataSwarm(DS_RANDOM_PORT, name='dataswarm_example', catalog=True, exclusive=False)
os.environ['PATH'] = '../../../work_queue/src:' + os.environ['PATH']
os.system('ds_worker -d all localhost %d &' % ds.port)

print ds.name

ds.specify_algorithm(DS_SCHEDULE_FCFS)
#ds.specify_name('dataswarm_example')
#ds.specify_manager_mode(DS_MASTER_MODE_STANDALONE)
#ds.specify_worker_mode(DS_WORKER_MODE_SHARED)
ds.specify_task_order(DS_TASK_ORDER_LIFO)

if ds.empty():
    print 'work queue is empty'

outputs = []

for i in range(5):
    ifile = 'msg.%d' % i
    ofile = 'out.%d' % i
    task  = Task('cat < %s > %s' % (ifile, ofile))

    task.specify_tag(str(time.time()))
    print task.command, task.tag

    task.specify_algorithm(DS_SCHEDULE_FILES)
    print task.command, task.algorithm

    task.specify_buffer('hello from %d' % i, ifile, cache=False)
    if i % 2:
	task.specify_output_file(ofile, cache=False)
    else:
	task.specify_file(ofile, type=DS_OUTPUT, cache=False)

    outputs.append(ofile)
    ds.submit(task)

if ds.empty():
    print 'work queue is empty'

while not ds.empty():
    t = ds.wait(10)
    if t:
	print t.tag

    print ds.stats.workers_init, ds.stats.workers_ready, ds.stats.workers_busy, \
	  ds.stats.tasks_running, ds.stats.tasks_waiting, ds.stats.tasks_complete

map(os.unlink, outputs)

for i in range(5):
    task = Task('hostname && date +%s.%N')
    task.specify_input_file('/bin/hostname')
    ds.submit(task)

if ds.hungry():
    print 'work queue is hungry'

ds.activate_fast_abort(1.5)

while not ds.empty():
    t = ds.wait(1)
    if t:
	print t.id, t.return_status, t.result, t.host
	print t.submit_time, t.finish_time, t.app_delay
	print t.send_input_start, t.send_input_finish
	print t.execute_cmd_start, t.execute_cmd_finish
	print t.receive_output_start, t.receive_output_finish
	print t.total_bytes_transferred, t.total_transfer_time
	print t.cmd_execution_time
	print t.output

    print ds.stats.workers_init, ds.stats.workers_ready, ds.stats.workers_busy, \
	  ds.stats.tasks_running, ds.stats.tasks_waiting, ds.stats.tasks_complete

ds.shutdown_workers(0)

print ds.stats.total_tasks_dispatched, ds.stats.total_tasks_complete, \
      ds.stats.total_workers_joined, ds.stats.total_workers_removed

# vim: sts=4 sw=4 ts=8 ft=python
