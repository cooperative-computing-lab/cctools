#! /usr/bin/env python

import sys
import ndcctools.taskvine as vine

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise


# Define functions to invoke remotely
def fun_a(arg):
    return f"{arg}"


def fun_b(remote_data):
    import cloudpickle
    with open(remote_data, "rb") as f:
        r = cloudpickle.load(f)
    return f"{r} world!"


# Create a new m
m = vine.Manager(port=[9123, 9130])
print("listening on port {}".format(m.port))
with open(port_file, "w") as f:
    f.write(str(m.port))

t_a = vine.PythonTask(fun_a, "hello")
t_a.enable_temp_output()

m.submit(t_a)
while not m.empty():
    t = m.wait(5)
    if t and not t.successful():
        print(f"Something went wrong with task a: {t.result}")
        sys.exit(1)

# use the output of t_a, which t_b knows is encoded as a remote temp file
t_b = vine.PythonTask(fun_b, "remote_data")
t_b.add_input(t_a.output_file, "remote_data")
m.submit(t_b)
while not m.empty():
    t = m.wait(5)
    if t and not t.successful():
        print(f"Something went wrong with task b: {t.result}")
        sys.exit(1)

# we now can remove the temp file from the worker
m.remove_file(t_a.output_file)

print(f"final output: {t_b.output}")

assert t_b.output == "hello world!"
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
