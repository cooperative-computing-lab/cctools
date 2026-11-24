#! /usr/bin/env python

# Test chaining of both regular and temporary tasks.
# This evaluates whether the file state tracking is working correctly.
# An initial test generates three files, each consumed by a different
# task with a small sleep to ensure that they run on different worker,
# and require a file transfer to move it.

import os
import sys
import ndcctools.taskvine as vine

m = vine.Manager(port=0)

# Disable peer transfers so that we are testing the
# correct sequencing of files brought home and then
# sent back out.
m.disable_peer_transfers()

factory = vine.Factory(manager=m,batch_type="local")
factory.min_workers = 3
factory.max_workers = 3

# Create a 10MB input file to have something to transfer
os.system("dd if=/dev/zero of=infile bs=1M count=10")

f = m.declare_file("infile")
a = m.declare_file("out.1")
b = m.declare_file("out.2")
c = m.declare_file("out.3")

t = vine.Task("cp infile out.1; cp infile out.2; cp infile out.3")
t.add_input(f,"infile")
t.add_output(a,"out.1")
t.add_output(b,"out.2")
t.add_output(c,"out.3")

x = vine.Task("sleep 3; cat infile > /dev/null")
x.add_input(a,"infile")

y = vine.Task("sleep 4; cat infile > /dev/null")
y.add_input(b,"infile")

z = vine.Task("sleep 5; cat infile > /dev/null")
z.add_input(c,"infile")

print(f"manager listening on port {m.port}")
       
print(f"submitting tasks...")
       
m.submit(t)
m.submit(x)
m.submit(y)
m.submit(z)

print(f"waiting for tasks...")
nsuccess = 0
nfailure = 0

with factory: 
    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"Task {t.id} [{t.command}] succeeded")
                nsuccess += 1
            else:
                print(f"Task {t.id} [{t.command}] FAILED: {t.result} output: {t.output}")
                nfailure += 1
      

print(f"{nsuccess} tasks succeeded, {nfailure} tasks failed.")

if nsuccess == 4:
    print("test success")
    sys.exit(0)
else:
    print("test failure")
    sys.exit(1)


