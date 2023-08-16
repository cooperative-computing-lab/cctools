#! /usr/bin/env python

# taskvine python binding tests
# tests for missing/recursive inputs/outputs.

import sys
import ndcctools.taskvine as vine

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise

desired_tag_order = "7 5 9 6 3 8 2 1".split()
alpha_order = sorted(desired_tag_order)
done_order = []

q = vine.Manager(port=0)
with open(port_file, "w") as f:
    print("Writing port {port} to file {file}".format(port=q.port, file=port_file))
    f.write(str(q.port))
print(vine.__file__)

for tag in alpha_order:
    t = vine.Task("/bin/echo hello tag {}".format(tag))
    t.set_tag(tag)
    q.submit(t)

for tag in desired_tag_order:
    while not q.empty():
        t = q.wait_for_tag(tag, 10)
        if t:
            done_order.append(t.tag)
            break

print("desired order: {}".format(desired_tag_order))
print("returned order: {}".format(done_order))

correct_order = all(map(lambda pair: pair[0] == pair[1], zip(desired_tag_order, done_order)))
if not correct_order or (len(done_order) != len(desired_tag_order)):
    raise Exception("Incorrect order")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
