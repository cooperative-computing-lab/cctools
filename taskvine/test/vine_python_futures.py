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


# Create Executor
executor = vine.Executor(port=9123, manager_name='vine_matrtix_build_test', factory=False)
print("listening on port {}".format(executor.manager.port))
with open(port_file, "w") as f:
    f.write(str(executor.manager.port))


# Submit several tasks for execution:
print("submitting tasks...")
t1 = executor.task(my_sum, 3, 4)
t1.set_cores(1)
t2 = executor.task(my_sum, 2, 5)
t2.set_cores(1)
a = executor.submit(t1)
b = executor.submit(t2)

t3 = executor.task(my_sum, a, b)
t3.set_cores(1)
c = executor.submit(t3)
# Get result
print("waiting for result...")
assert c.result() == 14
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
