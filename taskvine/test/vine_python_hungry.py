#! /usr/bin/env python

import ndcctools.taskvine as vine
import signal
import sys


def timeout(signum, frame):
    print("hungry test did not finish in time")
    sys.exit(1)


command = "sleep 120"


signal.signal(signal.SIGALRM, timeout)
signal.alarm(600)


m = vine.Manager(port=0)

m.tune("hungry-minimum", 11)
assert m.hungry() == 11

m.tune("hungry-minimum", 2)
assert m.hungry() == 2

t_1 = vine.Task(command)
i_1 = m.submit(t_1)

assert m.hungry() == 1

t_2 = vine.Task(command)
i_2 = m.submit(t_2)

assert m.hungry() == 0

worker_cores = 12
worker_memory = 1200
worker_disk = 1200

workers = vine.Factory("local", manager=m)
workers.max_workers = 1
workers.min_workers = 1
workers.cores = worker_cores
workers.disk = worker_disk
workers.memory = worker_memory

with workers:
    while m.stats.tasks_running < 1:
        m.wait(1)

    # hungry-minimum is 2, 2 tasks submitted, one waiting, thus hungry for 1 task
    assert m.hungry() == 1

    m.tune("hungry-minimum", 5)

    # hungry-minimum is 5, 2 tasks submitted, one waiting, thus hungry for 4 tasks
    assert m.hungry() == 4

    m.cancel_by_task_id(i_1)
    m.cancel_by_task_id(i_2)

    while m.stats.tasks_running > 0:
        m.wait(1)

    # hungry-minimum is 5, no tasks submitted, thus hungry for max of 5 tasks and 2 x worker_cores
    assert m.hungry() == 2 * worker_cores

    t_3 = vine.Task(command)
    t_3.set_cores(1)
    i_3 = m.submit(t_3)

    while m.stats.tasks_running < 1:
        m.wait(1)

    # hungry-minimum is 5, and tasks with one core is running. max of 5 and 2 x worker_cores - cores running
    assert m.hungry() == worker_cores * 2 - 1

    factor = 3
    m.tune("hungry-minimum-factor", factor)

    # as previous, but now available has different hungry factor
    assert m.hungry() == worker_cores * factor - 1

    t_4 = vine.Task(command)
    t_4.set_cores(worker_cores - 1)
    i_4 = m.submit(t_4)

    while m.stats.tasks_running < 2:
        m.wait(1)

    # hungry-minimum is 5, and all cores are being used
    assert m.hungry() == 5

    m.cancel_by_task_id(i_3)
    m.cancel_by_task_id(i_4)

    while m.stats.tasks_running > 0:
        m.wait(1)

    mem_task = int(2 * worker_memory / worker_cores)
    t_5 = vine.Task(command)
    t_5.set_cores(1)
    t_5.set_memory(mem_task)
    i_5 = m.submit(t_5)

    while m.stats.tasks_running < 1:
        m.wait(1)

    # memory should be the limit factor here
    assert m.hungry() == (factor * worker_memory - mem_task) / mem_task

    m.cancel_by_task_id(i_5)

    cores_t_6 = 1
    t_6 = vine.Task(command)
    t_6.set_cores(cores_t_6)
    t_6.set_memory(1)
    t_6.set_disk(1)
    i_6 = m.submit(t_6)

    cores_t_7 = 11
    t_7 = vine.Task(command)
    t_7.set_cores(cores_t_7)
    t_7.set_memory(1)
    t_7.set_disk(1)
    i_7 = m.submit(t_7)

    while m.stats.tasks_running < 2:
        m.wait(1)

    cores_t_8 = 2
    t_8 = vine.Task(command)
    t_8.set_cores(cores_t_8)
    i_8 = m.submit(t_8)

    factor = 10
    m.tune("hungry-minimum-factor", factor)

    # avg cores waiting should be the limiting factor
    # each task would get two cores, minus one task of the already waiting task
    print(m.hungry(), (factor * worker_cores - cores_t_6 - cores_t_7) / cores_t_8 - 1)
    assert m.hungry() == (factor * worker_cores - cores_t_6 - cores_t_7) / cores_t_8 - 1
