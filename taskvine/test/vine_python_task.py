#! /usr/bin/env python

import sys
import ndcctools.taskvine as vine

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise


# Define a function to invoke remotely
def my_sum(x, y, negate=False):
    from operator import add, mul

    f = 1
    if negate:
        f = -1
    s = mul(f, add(x, y))
    return s


# Create a new queue
queue = vine.Manager(port=[9123, 9130])
print("listening on port {}".format(queue.port))
with open(port_file, "w") as f:
    f.write(str(queue.port))


# Submit several tasks for execution:
print("submitting tasks...")
for value in range(1, 10):
    task = vine.PythonTask(my_sum, value, value)
    task.set_cores(1)
    queue.submit(task)

# add task outputs
positive_sum = 0
while not queue.empty():
    task = queue.wait(5)
    if task:
        print("task {} completed with result '{}': {}".format(task.id, task.result, task.output))
        positive_sum += task.output


# Submit several tasks for execution:
for value in range(1, 10):
    task = vine.PythonTask(my_sum, value, value, negate=True)
    task.set_cores(1)
    queue.submit(task)

# add task outputs
negative_sum = 0
while not queue.empty():
    task = queue.wait(5)
    if task:
        print("task {} completed with result '{}': {}".format(task.id, task.result, task.output))
        negative_sum += task.output

assert positive_sum == (-1 * negative_sum)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
