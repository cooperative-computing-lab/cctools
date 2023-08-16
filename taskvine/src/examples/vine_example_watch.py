#!/usr/bin/env python

# This example program shows the behavior of the watch parameter when adding an
# output file to a task.

# If a task produces output to a file incrementally as it runs,
# it can be helpful to see that output piece by piece as it
# is produced. By simply adding watch=True to the output
# of the program, taskvine will periodically check for output
# and return it to the manager while each task runs.  When the
# task completes, any remaining output is fetched.

# This example runs several instances of the task named
# trickle.sh, which gradually produces output
# every few seconds.  While running the manager program, open
# up another terminal, and observe that files output.0, output.1,
# etc are gradually produced throughout the run.

import ndcctools.taskvine as vine
import sys

script = """
#!/bin/sh
# This is a simple example of a program that gradually
# produces output over time.  It just logs the current
# time every second for 30 seconds.

hostname 

for n in $(seq 1 30)
do
	sleep 1 
	date
done

echo "done!"
""";


if __name__ == "__main__":
    m = vine.Manager()
    print("listening on port", m.port)

    script = m.declare_buffer(script,cache=True)
    
    n = 3
    for i in range(n):
        t = vine.Task("./trickle.sh > output")

        t.add_input(script, "trickle.sh")

        output = m.declare_file(f"output.{i}")
        t.add_output(output, "output", watch=True)

        t.set_cores(1)

        m.submit(t)

    print("Waiting for tasks to complete...")

    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task {t.id} result: {t.std_output}")
            elif t.completed():
                print(f"task {t.id} completed with an executin error, exit code {t.exit_code}")
            else:
                print(f"task {t.id} failed with status {t.result}")

        # print to the console the contents of the files being watched
        for i in range(n):
            try:
                with open(f"output.{i}") as f:
                    print(f"output.{i}:\n{f.readlines()}\n")
            except IOError:
                pass

    print("All tasks complete!")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
