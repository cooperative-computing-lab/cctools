#! /usr/bin/env python3

# Test of task cancellation in Python.
# Submit 10 tasks that sleep for 10 seconds.
# After 3 seconds, cancel all of them.
# Wait for task return and verify that status was cancelled.

import sys
import ndcctools.taskvine as vine

ntasks = 10
ntasks_cancelled = 0

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
    
for i in range(0,ntasks):
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
        if t.result=="cancelled":
                ntasks_cancelled+=1

                
if ntasks_cancelled==ntasks:
    print("Success: {} of {} tasks cancelled\n".format(ntasks_cancelled,ntasks))
    sys.exit(0)
else:
    print("Failure: {} of {} tasks cancelled\n".format(ntasks_cancelled,ntasks))
    sys.exit(1)
    

              
