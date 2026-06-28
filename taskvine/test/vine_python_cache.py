#! /usr/bin/env python

# Test for task result caching in TaskVine.
#
# Verifies:
# 1. First run executes PythonTasks and stores results in cache.
# 2. Second run with identical tasks returns results from cache without re-execution.
# 3. A task with different arguments is NOT served from cache (cache miss).

import os
import sys
import ndcctools.taskvine as vine

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise

CACHE_DIR = "vine-task-cache-test"


def square(x):
    return x * x


m = vine.Manager(port=0)
m.enable_tasks_cache(cache_dir=CACHE_DIR)

with open(port_file, "w") as f:
    f.write(str(m.port))

# -----------------------------------------------------------------------
# First run: tasks execute on worker, results are cached.
# -----------------------------------------------------------------------
print("First run: submitting tasks to worker...")
for i in range(1, 6):
    t = vine.PythonTask(square, i)
    t.set_tag(str(i))
    m.submit(t)

first_results = {}
while not m.empty():
    t = m.wait(5)
    if t:
        if not t.successful():
            raise Exception("Task {} failed on first run: {}".format(t.tag, t.result))
        first_results[t.tag] = t.output
        print("  tag={} output={} host={}".format(t.tag, t.output, t.hostname))

assert len(first_results) == 5, "Expected 5 results on first run, got {}".format(len(first_results))

# Verify cache output files were written to disk.
cache_files = [f for f in os.listdir(CACHE_DIR) if f.endswith(".output")]
assert len(cache_files) == 5, "Expected 5 cached output files, got {}".format(len(cache_files))

# -----------------------------------------------------------------------
# Second run: identical tasks must be served entirely from cache.
# -----------------------------------------------------------------------
print("Second run: submitting same tasks (expect all cache hits)...")
for i in range(1, 6):
    t = vine.PythonTask(square, i)
    t.set_tag(str(i))
    m.submit(t)

second_results = {}
cached_count = 0
while not m.empty():
    t = m.wait(5)
    if t:
        if not t.successful():
            raise Exception("Task {} failed on second run: {}".format(t.tag, t.result))
        second_results[t.tag] = t.output
        if t.hostname == "CACHED":
            cached_count += 1
        print("  tag={} output={} host={}".format(t.tag, t.output, t.hostname))

assert len(second_results) == 5, "Expected 5 results on second run, got {}".format(len(second_results))
assert cached_count == 5, "Expected all 5 tasks served from cache, got {}".format(cached_count)

# Verify results are identical between runs.
for tag in first_results:
    assert first_results[tag] == second_results[tag], \
        "Result mismatch for tag {}: first={} second={}".format(tag, first_results[tag], second_results[tag])

# -----------------------------------------------------------------------
# Cache miss: a task with a different argument must execute on the worker.
# -----------------------------------------------------------------------
print("Cache miss test: submitting task with new argument...")
t = vine.PythonTask(square, 99)
t.set_tag("99")
m.submit(t)

miss_result = None
while not m.empty():
    t = m.wait(5)
    if t:
        if not t.successful():
            raise Exception("Cache-miss task failed: {}".format(t.result))
        miss_result = t.output
        assert t.hostname != "CACHED", "Task with new argument should not come from cache"
        print("  tag={} output={} host={}".format(t.tag, t.output, t.hostname))

assert miss_result == 99 * 99, "Expected {} got {}".format(99 * 99, miss_result)

print("All assertions passed.")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
