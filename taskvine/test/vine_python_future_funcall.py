#! /usr/bin/env python

import sys
import ndcctools.taskvine as vine

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise


# Define a library function
def divide(dividend, divisor):
    import math
    return dividend / math.sqrt(divisor)


# Create Executor
executor = vine.Executor(port=9123, manager_name='vine_future_funcall_test', factory=False)
print("listening on port {}".format(executor.port))
with open(port_file, "w") as f:
    f.write(str(executor.port))

# Create library task
print("creating library from functions...")
libtask = executor.create_library_from_functions('test-library', divide, import_modules=None, add_env=False)

# Install library on executor.manager
executor.install_library(libtask)

# Submit several tasks for execution
print("creating future function call tasks...")
t1 = executor.future_funcall('test-library', 'divide', 128, 4)
t2 = executor.future_funcall('test-library', 'divide', t1.future, t1.future)
t3 = executor.future_funcall('test-library', 'divide', t2.future, t1.future)
print("all tasks are created")

executor.submit(t1)
executor.submit(t2)
executor.submit(t3)
print("all tasks are submitted")

print("submitting future function call tasks...")
print(f"t1 output is {t1.output}")
print(f"t2 output is: {t2.output}")
print(f"t3 output is: {t3.output}")



# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
