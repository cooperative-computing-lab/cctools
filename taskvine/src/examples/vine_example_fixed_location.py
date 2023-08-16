#!/usr/bin/env python

# This example program performs a merge sort
# to show how a task can be executed
# where particular files have been cached.

import ndcctools.taskvine as vine

import argparse
import os
import sys
from collections import defaultdict

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog="vine_example_strict_inputs.py",
            description="This example TaskVine program performs a merge sort. The intermediate merge steps occur where data already exists at the workers.")
    args = parser.parse_args()

    m = vine.Manager()

    # Needed to share partial results at workers for the final merge
    m.enable_peer_transfers()

    print(f"waiting for at least 2 workers on port {m.port}")
    m.tune("wait-for-workers", 2)

    # list of numbers are written to these temporary files. We keep a mapping from task.id -> file
    temporary_unsorted_lists = {}

    # generate 10 lists of 3 random numbers
    for i in range(10):
        out_file = m.declare_temp()
        t = vine.Task(f"seq 3 | awk 'BEGIN {{ srand({i}) }}; {{print(rand())}}' > unsorted.numbers")
        t.set_cores = 1

        t.add_output(out_file, "unsorted.numbers")

        tid = m.submit(t)
        temporary_unsorted_lists[tid] = out_file

    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task {t.id} -unsorted list- done on {t.addrport}")
            else:
                print(f"task {t.id} -unsorted list- failed with status: {t.result}")

    # now we sort the lists generated in the worker that they live
    temporary_sorted_lists = {}
    for in_file in temporary_unsorted_lists.values():
        out_file = m.declare_temp()
        t = vine.Task("sort unsorted.numbers > sorted.numbers")
        t.set_cores = 1

        t.add_output(out_file, "sorted.numbers")

        # with strict input, the task will be scheduled only to the worker that
        # has that file.
        t.add_input(in_file, "unsorted.numbers", strict_input=True)

        tid =  m.submit(t)
        temporary_sorted_lists[tid] = out_file

    # record which worker finished which task
    tasks_of_worker = defaultdict(lambda: [])
    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task sorted list {t.id} done on {t.addrport}")
                tasks_of_worker[t.addrport].append(t.id)
            else:
                print(f"task sorted list {t.id} failed with status: {t.result}")

    # we are done with the unsorted lists and we remove them from the cluster
    for f in temporary_unsorted_lists.values():
        m.remove_file(f)

    merge_outputs = []
    for tids in tasks_of_worker.values():
        out_file = m.declare_temp()
        t = vine.Task("sort --merge sorted.numbers.* > sorted.merge")
        t.set_cores = 1
        t.add_output(out_file, "sorted.merge")

        for tid in tids:
            t.add_input(temporary_sorted_lists[tid], f"sorted.numbers.{tid}", strict_input=True)

        m.submit(t)
        merge_outputs.append(out_file)

    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task merge lists {t.id} done on {t.addrport}")
            else:
                print(f"task merge lists {t.id} failed with status: {t.result}")

    # we are done with the sorted lists and we remove them from the cluster
    for f in temporary_sorted_lists.values():
        m.remove_file(f)

    # create final merge, and run it at whatever worker has the most inputs
    out_file = m.declare_file("final_output.txt")
    t = vine.Task("sort --merge sorted.merge.* > merged.output")
    t.add_output(out_file, "merged.output")

    for (i,merge) in enumerate(merge_outputs):
        t.add_input(merge, f"sorted.merge.{i}")

    # TODO: ugly constant, make more pythonic
    # run task at worker with the most inputs
    t.set_scheduler(vine.VINE_SCHEDULE_FILES)
    m.submit(t)

    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task final merge {t.id} done on {t.addrport}")
                with open("final_output.txt") as f_in:
                    print(f_in.read())
            else:
                print(f"task final merge {t.id} failed with status: {t.result}")

    # we are done with the merged lists and we remove them from the cluster
    for f in merge_outputs:
        m.remove_file(f)


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
