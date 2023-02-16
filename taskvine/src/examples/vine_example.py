#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program is a very simple example of how to use taskvine.
# It accepts a list of files on the command line.
# Each file is compressed with gzip and returned to the user.

import taskvine as vine

import os
import sys

# Main program
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("vine_example <file1> [file2] [file3] ...")
        print(
            "Each file given on the command line will be compressed using a remote worker."
        )
        sys.exit(1)

    # Usually, we can execute the gzip utility by simply typing its name at a
    # terminal. However, this is not enough for taskvine; we have to
    # indicate precisely which files need to be transmitted to the workers. We
    # record the location of gzip in 'gzip_path', which is usually found in
    # /bin/gzip or /usr/bin/gzip.

    gzip_path = "/bin/gzip"
    if not os.path.exists(gzip_path):
        gzip_path = "/usr/bin/gzip"
        if not os.path.exists(gzip_path):
            print(
                "gzip was not found. Please modify the gzip_path variable accordingly. To determine the location of gzip, from the terminal type: which gzip (usual locations are /bin/gzip and /usr/bin/gzip)"
            )
            sys.exit(1)

    # We create the tasks queue using the default port. If this port is already
    # been used by another program, you can try setting port = 0 to use an
    # available port.
    q = vine.Manager()
    print("listening on port %d..." % q.port)

    # We create and dispatch a task for each filename given in the argument list
    for i in range(1, len(sys.argv)):
        infile = "%s" % sys.argv[i]
        outfile = "%s.gz" % sys.argv[i]

        # Note that we write ./gzip here, to guarantee that the gzip version we
        # are using is the one being sent to the workers.
        command = "./gzip < %s > %s" % (infile, outfile)

        t = vine.Task(command)

        # gzip is the same across all tasks, so we can cache it in the workers.
        # Note that when adding a file, we have to name its local name
        # (e.g. gzip_path), and its remote name (e.g. "gzip"). Unlike the
        # following line, more often than not these are the same.
        t.add_input_file(gzip_path, "gzip", cache=True)

        # files to be compressed are different across all tasks, so we do not
        # cache them. This is, of course, application specific. Sometimes you may
        # want to cache an output file if is the input of a later task.
        t.add_input_file(infile, infile, cache=False)
        t.add_output_file(outfile, outfile, cache=False)

        # Once all files has been specified, we are ready to submit the task to the queue.
        task_id = q.submit(t)
        print("submitted task (id# %d): %s" % (task_id, t.command))

    print("waiting for tasks to complete...")
    while not q.empty():
        t = q.wait(5)
        if t:
            print(
                "task (id# %d) complete: %s (return code %d)"
                % (t.id, t.command, t.exit_code)
            )
            if t.exit_code != 0:
                # The task failed. Error handling (e.g., resubmit with new parameters, examine logs, etc.) here
                pass
        # task object will be garbage collected by Python automatically when it goes out of scope

    print("all tasks complete!")

    # taskvine object will be garbage collected by Python automatically when it goes out of scope
    sys.exit(0)
