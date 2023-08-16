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
def my_fun():
    print("the function")
    return "look ma no serialization".encode('utf8')


# Create a new m
m = vine.Manager(port=[9123, 9130])
print("listening on port {}".format(m.port))
with open(port_file, "w") as f:
    f.write(str(m.port))

# Submit several tasks for execution:
print("submitting tasks...")
t = vine.PythonTask(my_fun)
t.disable_output_serialization()
m.submit(t)

while not m.empty():
    t = m.wait(5)
    if t:
        if t.successful():
            print(t.output)
            assert t.output == "look ma no serialization".encode('utf8')
        else:
            print(f"Something went wrong with the task: {t.result}\nstdout: {t.std_output}")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
