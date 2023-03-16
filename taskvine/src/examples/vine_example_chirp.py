#!/usr/bin/env python

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows how to declare an chirp file so that it can be cached at
# the workers.
# It assumes that chirp is installed where workers are executed. If this is
# not the case, a poncho recipe to construct this environment is also given.
#

import taskvine as vine

import os
import sys

# define a dummy python task that will be apply to each of the files.
def count_lines(chirp_file):
    lines = 0
    with open(chirp_file) as f:
        for line in f:
            lines += 1
    return lines


# construct a starch environment to execute the tasks. Only needed if the chirp
# executables are not available where the workers execute
def create_env(env_name):
    import subprocess
    subprocess.run(["starch", "-x", "chirp_get", "-c", "chirp_get", env_name], check=True)


if __name__ == "__main__":

    try:
        (chirp_server, test_filename) = sys.argv[1:]
    except Exception:
        print(f"Usage: {sys.argv[0]} chirp_server test_filename [auth_ticket_file]")
        print(f"Prints the number of lines of test_filename from chirp_server")
        sys.exit(1)

    env_with_chirp = None
    # uncomment the following lines only if workers don't have chirp available
    #env_with_chirp = "chirp_get.sfx"
    #create_env(env_with_chirp)

    m = vine.Manager()
    print("listening on port", m.port)

    # define the authentication ticket to use.
    ticket_file = None
    #ticket_file = m.declare_file("myticket.ticket", cache=True)

    t = vine.PythonTask(count_lines, "mychirp.file")

    if env_with_chirp:
        sf = m.declare_file("chirp_get.sfx", cache="always")
        env_file = m.declare_starch(sf, cache="always")
    else:
        env_file = None

    chirp_file = m.declare_chirp(chirp_server, test_filename, ticket_file, env=env_file, cache=True)
    t.add_input(chirp_file, "mychirp.file")

    task_id = m.submit(t)
    print("submitted task (id# " + str(task_id) + "): count_lines()")

    print("waiting for tasks to complete...")
    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task {t.id} processed a file with {t.output} lines")
            elif t.completed():
                print(f"task {t.id} completed with an executin error, exit code {t.exit_code}")
            else:
                print(f"task {t.id} failed with status {t.result_string}")

    print("all tasks complete!")
