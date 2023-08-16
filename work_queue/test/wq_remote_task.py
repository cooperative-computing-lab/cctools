#! /usr/bin/env python3

import sys, json
import ndcctools.work_queue as wq
port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise

# Define functions to invoke remotely
def add(x, y):
    return x + y
def multiply(x, y):
    return x * y
def kwargs_test(x=5, y=6, z=7):
    return x + y * z
def no_arguments_test(a, b, c):
    return a + b + c
def exception_test():
    raise Exception("I am a bad funtion")

# Create a new queue
queue = wq.WorkQueue(port=[9123,9130])
print("listening on port {}".format(queue.port))
with open(port_file, "w") as f:
    f.write(str(queue.port))


# Submit several tasks for execution:
print("submitting tasks...")
for value in range(1,10):
    # simple addition of two arguments using thread execution and passing all arguments positionally 
    # should return value + value
    task = wq.RemoteTask("add", "my_coprocess")
    task.specify_cores(1)
    task.specify_disk(10)
    task.specify_memory(10)
    task.specify_gpus(0)
    task.specify_fn_args([value, value])
    task.specify_exec_method("thread")
    queue.submit(task)

    # multiplication of two arguments using fork as the execution method and passing arguments as a mix of positional arguments and dictionary arguments
    # should return value * value
    task = wq.RemoteTask("multiply", "my_coprocess")
    task.specify_cores(1)
    task.specify_disk(10)
    task.specify_memory(10)
    task.specify_gpus(0)
    task.specify_fn_args([value], {"y":value})
    task.specify_exec_method("fork")
    queue.submit(task)

    # testing of passing entirely keyword arguments using the direct execution method
    # should return 7 for every iteration (1 + 2 * 3)
    task = wq.RemoteTask("kwargs_test", "my_coprocess", x=1, y=2, z=3)
    task.specify_cores(1)
    task.specify_disk(10)
    task.specify_memory(10)
    task.specify_gpus(0)
    task.specify_exec_method("direct")
    queue.submit(task)

    # testing whether functions that do not recieve enough arguments properly create errors
    # should return with status code 500, saying that positional arguments are missing
    task = wq.RemoteTask("no_arguments_test", "my_coprocess")
    task.specify_cores(1)
    task.specify_disk(10)
    task.specify_memory(10)
    task.specify_gpus(0)
    task.specify_exec_method("thread")
    queue.submit(task)

    # testing whether functions that raise exceptions properly have their exceptions captured and returned in the result
    # should return with status code 500 and the result should be the exception thrown in the function
    task = wq.RemoteTask("exception_test", "my_coprocess")
    task.specify_cores(1)
    task.specify_disk(10)
    task.specify_memory(10)
    task.specify_gpus(0)
    task.specify_exec_method("thread")
    queue.submit(task)

# keep track of task outputs
add_sum = 0
multiply_sum = 0
kwargs_sum = 0
no_arguments_errors = 0
num_exceptions = 0
while not queue.empty():
    task = queue.wait(5)
    if task:
        print("task {} completed with result {}".format(task.id,task.output))
        # update variable depending on task command
        if task.command == "add":
            add_sum += int(json.loads(task.output)["Result"])
        elif task.command == "multiply":
            multiply_sum += int(json.loads(task.output)["Result"])
        elif task.command == "kwargs_test":
            kwargs_sum += int(json.loads(task.output)["Result"])
        elif task.command == "no_arguments_test":
            no_arguments_errors += 1
        elif task.command == "exception_test":
            num_exceptions += 1

assert(add_sum == 90)
assert(multiply_sum == 285)
assert(kwargs_sum == 63)
assert(no_arguments_errors == 9)
assert(num_exceptions == 9)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
