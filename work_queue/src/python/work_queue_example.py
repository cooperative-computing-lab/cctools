#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program is a very simple example of how to use Work Queue.
# It accepts a list of files on the command line.
# Each file is compressed with gzip and returned to the user.

from work_queue import *

import os
import sys

def find_executable(executable):
    for directory in os.environ['PATH'].split(':'):
        path = os.path.join(directory, executable)
        if os.path.exists(path):
            return path
    return None

port = WORK_QUEUE_DEFAULT_PORT

if len(sys.argv) < 2:
    print "work_queue_example <file1> [file2] [file3] ..."
    print "Each file given on the command line will be compressed using a remote worker."
    sys.exit(1)

try:
    q = WorkQueue(port)
except:
    print "Instantiation of Work Queue failed!" 
    sys.exit(1)

print "listening on port %d..." % q.port

gzip_path = find_executable("gzip")

for i in range(1, len(sys.argv)):
    infile = "%s" % sys.argv[i] 
    outfile = "%s.gz" % sys.argv[i]
    command = "./gzip < %s > %s" % (infile, outfile)
    
    t = Task(command)
    
    t.specify_file(gzip_path, "gzip", WORK_QUEUE_INPUT, cache=True)
    t.specify_file(infile, infile, WORK_QUEUE_INPUT, cache=False)
    t.specify_file(outfile, outfile, WORK_QUEUE_OUTPUT, cache=False)
    
    taskid = q.submit(t)

    print "submitted task (id# %d): %s" % (taskid, t.command)

print "waiting for tasks to complete..."

while not q.empty():
    t = q.wait(5)
    if t:
        print "task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status)
    #task object will be garbage collected by Python automatically when it goes out of scope

print "all tasks complete!"

#work queue object will be garbage collected by Python automatically when it goes out of scope
sys.exit(0)
