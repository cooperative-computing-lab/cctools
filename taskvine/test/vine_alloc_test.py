#! /usr/bin/env python

# taskvine python binding tests
# tests for missing/recursive inputs/outputs.

import ndcctools.taskvine as vine

import sys
import math


def check_task(category, category_mode, max, min, expected):
    q.set_category_resources_max(category, max)
    q.set_category_resources_min(category, min)
    q.set_category_mode(category, category_mode)

    t = vine.Task("/bin/echo hello")
    t.set_category(category)
    q.submit(t)

    t = q.wait(30)
    if not t:
        print("Task did not complete in the allotted time.")
        sys.exit(1)
    else:
        print("{}:".format(category))
        print("expected: {}".format(expected))
        print("allocated: {}".format({"cores": t.resources_allocated.cores, "memory": t.resources_allocated.memory, "disk": t.resources_allocated.disk, "gpus": t.resources_allocated.gpus}))

        assert t.resources_allocated.cores == expected["cores"]
        assert t.resources_allocated.memory == expected["memory"]
        assert t.resources_allocated.disk == expected["disk"]
        assert t.resources_allocated.gpus == expected["gpus"]


port_file = None
try:
    port_file = sys.argv[1]
    (worker_cores, worker_memory, worker_disk, worker_gpus) = (int(sys.argv[i]) for i in range(2, 6))

    if worker_disk % worker_cores != 0 or worker_memory % worker_cores != 0:
        sys.stderr.write("For this test, the number of worker's cores should divide evenly the worker's memory and disk.\n".format(sys.argv[0]))

except IndexError:
    sys.stderr.write("Usage: {} PORTFILE WORKER_CORES WORKER_MEMORY WORKER_DISK\n".format(sys.argv[0]))
    raise

q = vine.Manager(0)
with open(port_file, "w") as f:
    print("Writing port {port} to file {file}".format(port=q.port, file=port_file))
    f.write(str(q.port))

worker = vine.Factory("local", manager_host_port="localhost:{}".format(q.port))
worker.max_workers = 1
worker.min_workers = 1

worker.cores = worker_cores
worker.memory = worker_memory
worker.disk = worker_disk
worker.gpus = worker_gpus

worker.debug = "all"
worker.debug_file = "factory.log"

with worker:
    q.tune("force-proportional-resources", 0)

    r = {"cores": 1, "memory": 2, "disk": 3, "gpus": 4}
    check_task("all_specified", "fixed", max=r, min={}, expected=r)

    check_task("all_specified_no_gpu", "fixed", max={"cores": 1, "memory": 2, "disk": 3}, min={}, expected={"cores": 1, "memory": 2, "disk": 3, "gpus": 0})

    check_task("all_specified_no_cores", "fixed", max={"gpus": 4, "memory": 2, "disk": 3}, min={}, expected={"cores": 0, "memory": 2, "disk": 3, "gpus": 4})

    check_task("all_zero", "fixed", max={"cores": 0, "memory": 0, "disk": 0, "gpus": 0}, min={}, expected={"cores": worker_cores, "memory": worker_memory, "disk": worker_disk, "gpus": 0})

    q.tune("force-proportional-resources", 1)
    check_task("only_memory", "fixed", max={"memory": worker_memory / 2}, min={}, expected={"cores": worker_cores / 2, "memory": worker_memory / 2, "disk": worker_disk / 2, "gpus": 0})

    check_task("only_memory_w_minimum", "fixed", max={"memory": worker_memory / 2}, min={"cores": 3, "gpus": 2}, expected={"cores": 3, "memory": worker_memory / 2, "disk": worker_disk / 2, "gpus": 2})

    check_task("only_cores", "fixed", max={"cores": worker_cores}, min={}, expected={"cores": worker_cores, "memory": worker_memory, "disk": worker_disk, "gpus": 0})

    check_task("auto_whole_worker", "min_waste", max={}, min={}, expected={"cores": worker_cores, "memory": worker_memory, "disk": worker_disk, "gpus": 0})

    p = 1 / worker_cores
    r = {"cores": 1}
    e = {"cores": 1, "memory": math.floor(worker_memory * p), "disk": math.floor(worker_disk * p), "gpus": 0}
    check_task("only_cores_proportional", "fixed", max=r, min={}, expected=e)

    p = 2 / worker_cores
    e = {"cores": 2, "memory": math.floor(worker_memory * p), "disk": math.floor(worker_disk * p), "gpus": 0}
    check_task("exhaustive_bucketing", "exhaustive_bucketing", max={}, min={}, expected=e)

    check_task("greedy_bucketing", "greedy_bucketing", max={}, min={}, expected=e)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
