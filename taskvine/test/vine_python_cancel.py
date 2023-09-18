#! /usr/bin/env python3

# taskvine python binding tests
# tests for submission and cancel of tasks

import sys
import ndcctools.taskvine as vine

ntasks = 10
tasks_done = 0

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise

q = vine.Manager(port=0)

with open(port_file, "w") as f:
    print("Writing port {port} to file {file}".format(port=q.port, file=port_file))
    f.write(str(q.port))

print("Submitting {} tasks...".format(ntasks))
    
for i in range(1,ntasks):
    t = vine.Task("sleep 10")
    t.set_cores(1)
    q.submit(t)

print("Waiting for tasks to start...".format(ntasks))

t = q.wait(3)

print("Cancelling all tasks...")
q.cancel_all()

print("Collecting cancelled tasks...")
while not q.empty():
    t = q.wait(10)
    if t:
        print("Task {} completed with status {}".format(t.id,t.result))
        tasks_done+=1
                            

              
