#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example program shows the behavior of the watch parameter when adding an
# output file to a task.

# If a task produces output to a file incrementally as it runs,
# it can be helpful to see that output piece by piece as it
# is produced. By simply adding watch=True to the output
# of the program, taskvine will periodically check for output
# and return it to the manager while each task runs.  When the
# task completes, any remaining output is fetched.

# This example runs several instances of the task named
# vine_example_watch_trickle.sh, which gradually produces output
# every few seconds.  While running the manager program, open
# up another terminal, and observe that files output.0, output.1,
# etc are gradually produced throughout the run.

import taskvine as vine
import sys

if __name__ == "__main__":
    m = vine.Manager()
    print("listening on port", m.port)

    n = 3
    for i in range(n):
        t = vine.Task("./vine_example_watch_trickle.sh > output")

        input_script = m.declare_file("vine_example_watch_trickle.sh")
        t.add_input(input_script, "vine_example_watch_trickle.sh", cache=True)

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
                print(f"task {t.id} failed with status {t.result_string}")

        # print to the console the contents of the files being watched
        for i in range(n):
            try:
                with open(f"output.{i}") as f:
                    print(f"output.{i}:\n{f.readlines()}\n")
            except IOError:
                pass

    print("All tasks complete!")
