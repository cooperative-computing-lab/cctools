#! /usr/bin/env python

import ndcctools.work_queue as wq
import sys
import time

m = wq.WorkQueue(port=0, ssl=True ) #  debug_log=":stderr")
m.enable_monitoring()

m.tune("category-steady-n-tasks", 1)
m.tune("category-steady-n-tasks", 1)

bucket_size = 250
worker = {
        "cores": 4,
        "memory": bucket_size * 4,
        "disk": bucket_size * 8
        }

factory = wq.Factory(manager=m)
factory.max_workers = 1
factory.min_workers = 1
factory.cores = worker["cores"]
factory.memory = worker["memory"]
factory.disk = worker["disk"]

modes = {
        "max": wq.WORK_QUEUE_ALLOCATION_MODE_MAX,
        "thr": wq.WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT,
        "wst": wq.WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE,
        }

expected_proportions = {
        "max": 0.5,  # half of the disk, so half of the resources
        "thr": 1/worker["cores"],
        "wst": 1/worker["cores"]
        }

error_found = False

last_returned_time =  time.time()

with factory:
    for (category, mode) in modes.items():
        m.specify_category_mode(category, mode)

        # first task needs little less than half of the disk, which should round up to half the disk
        t = wq.Task(
            f"dd count=0 bs={int(worker['disk']/2.5)}M seek=1 of=nulls")
        t.specify_category(category)
        t.specify_priority(999)      # high priority, so it executes first
        m.submit(t)

        for i in range(10):
            t = wq.Task("dd count=0 bs=1M seek=1 of=nulls")
            t.specify_category(category)
            m.submit(t)

        print(f"\n{category}: ", end="")
        while not m.empty():
            t = m.wait(5)
            if t:
                print(".", end="")
                last_returned_time = time.time()

            # if no task for 60s, something went wrong with the test
            if time.time() - last_returned_time > 60:
                print("\nno task finished recently")
                sys.exit(1)

        rs = "cores memory disk".split()
        mr = {r: getattr(t.resources_measured, r) for r in rs}
        ar = {r: getattr(t.resources_allocated, r) for r in rs}

        print("")
        for r in rs:
            sign = "="
            expected = expected_proportions[category] * worker[r]
            if ar[r] != expected:
                error_found = True
                sign = "!"
            print(f"{r} measured {mr[r]}, allocated {ar[r]} {sign}= {expected}")

    sys.exit(error_found)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
