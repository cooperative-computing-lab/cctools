#! /usr/bin/env python

import sys, json
import work_queue as wq

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise

# Define a function to invoke remotely
def add(x, y):
    return x + y
def multiply(x, y):
    return x * y

# Create a new queue
queue = wq.WorkQueue(port=[9123,9130])
print("listening on port {}".format(queue.port))
with open(port_file, "w") as f:
    f.write(str(queue.port))


# Submit several tasks for execution:
print("submitting tasks...")
for value in range(1,10):
    task = wq.RemoteTask("add", "my_coprocess")
    task.specify_cores(1)
    task.specify_disk(1000)
    task.specify_fn_args({"x":value, "y":value})
    task.specify_exec_method("direct")
    queue.submit(task)

# add task outputs
add_sum = 0
while not queue.empty():
    task = queue.wait(5)
    if task:
        print("task {} completed with result {}".format(task.id,task.output))
        add_sum += json.loads(task.output)["Result"]
    else:
        break


# Submit several tasks for execution:
print("submitting tasks...")
for value in range(1,10):
    task = wq.RemoteTask("multiply", "my_coprocess")
    task.specify_cores(1)
    task.specify_disk(1000)
    task.specify_fn_args({"x":value, "y":value})
    task.specify_exec_method("direct")
    queue.submit(task)

# add task outputs
multiply_sum = 0
while not queue.empty():
    task = queue.wait(5)
    if task:
        print("task {} completed with result {}".format(task.id,task.output))
        multiply_sum += json.loads(task.output)["Result"]
    else:
        break

print(add_sum, multiply_sum)
assert(add_sum == 0)
assert(multiply_sum == 0)
