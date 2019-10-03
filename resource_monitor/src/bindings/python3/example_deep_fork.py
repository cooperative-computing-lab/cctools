# run with:
# LD_PRELOAD=$(pwd)/../../librminimonitor_helper.so  python3 example_deep_fork.py
# we need the preload to capture the fork/exit of processes

import resource_monitor
import os
import time
import multiprocessing

def print_resources(resources, finished, resource_exhaustion):
    label = 'now'
    if finished:
        label = 'max'
    print("{} memory {}   procs running/total {}/{}".format(label,
        resources['memory'],
        resources['max_concurrent_processes'],
        resources['total_processes'], ))

def deep_fork(i):
    """ Call recursevely in a child if i > 0. Wait for one second. """
    print('in process {}  parent {}'.format(os.getpid(), os.getppid()))
    time.sleep(1)
    if i > 0:
        child = multiprocessing.Process(target = deep_fork, args = (i-1,))
        child.start()
        child.join()
        print('process {} ended'.format(os.getpid()))
    time.sleep(1)

monitored_deep_fork = resource_monitor.make_monitored(deep_fork, callback = print_resources)
(result, resources) = monitored_deep_fork(3)


