#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example program shows the behavior of the VINE_WATCH flag.

# If a task produces output to a file incrementally as it runs,
# it can be helpful to see that output piece by piece as it
# is produced. By simply adding the VINE_WATCH flag to the output
# of the program, taskvine will periodically check for output
# and return it to the manager while each task runs.  When the
# task completes, any remaining output is fetched.

# This example runs several instances of the task named
# vine_example_watch_trickle.sh, which gradually produces output
# every few seconds.  While running the manager program, open
# up another terminal, and observe that files output.0, output.1,
# etc are gradually produced throughout the run.

# Originally written in C (vine_example_watch.c)

from taskvine import *

import os
import sys
import errno

if __name__ == '__main__':
    try:
        m = Manager(port = VINE_DEFAULT_PORT)
    except IOError as e:
        print("couldn't create manager:",e.errno)
        sys.exit(1)
    print("listening on port",m.port)
    
    m.enable_debug_log("manager.log")
    
    for i in range(10):
        output = "output."+str(i)
        t = Task("./vine_example_watch_trickle.sh > output")
        t.add_input_file("vine_example_watch_trickle.sh", "vine_example_watch_trickle.sh", flags=VINE_CACHE)
        t.add_output_file(output, "output", flags=VINE_WATCH)
        t.set_cores(1)
        m.submit(t)
        
    print("Waiting for tasks to complete...")
    
    while not m.empty():
        t = m.wait(5)
        if t:
            r = t.result
            id = t.id
            
            if r==VINE_RESULT_SUCCESS:
                print("task",id,"output:",t.std_output)
            else:
                print("task",id,"failed:",r.result_string)
            del t
            
    print("All tasks complete!")
    
    del m
    
    sys.exit(0)