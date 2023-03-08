#!/usr/bin/env python

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows how to declare an xrootd file so that it can be cached at
# the workers.
# It assumes that uproot is installed where workers are executed. If this is
# not the case, a poncho recipe to construct this environment is:
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

# construct a poncho environment to execute the tasks. only needed if the chirp
# executables are not available where the workers execute
def create_env(env_name):
    import json
    import tempfile
    import subprocess
    py_version = f"{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}"

    if os.path.exists(env_name):
        return

    env = {
            "conda": {
                "channels": ["conda-forge"],
                "dependencies": [f"python={py_version}", "dill", "cctools"]
                }
            }

    with tempfile.NamedTemporaryFile("w", prefix="poncho-spec", encoding="utf8", dir=os.getcwd()) as f:
        json.dump(env, f)
        f.flush()
        subprocess.run(["poncho_package_create", f.name, env_name], check=True)


if __name__ == "__main__":

    try:
        (chirp_server, test_filename) = sys.argv[1:]
    except Exception:
        print(f"Usage: {sys.argv[0]} chirp_server test_filename [auth_ticket_file]")
        print(f"Prints the number of lines of test_filename from chirp_server")
        sys.exit(1)


    env_with_chirp = None
    # uncomment the following lines only if workers don't have chirp available
    env_with_chirp = "chirp_py_env.tar.gz"
    create_env(env_with_chirp)

    m = vine.Manager()
    print("listening on port", m.port)

    # define the authentication ticket to use.
    ticket_file = None
    #ticket_file = m.declare_file("myticket.ticket")

    t = vine.PythonTask(count_lines, "mychirp.file")
    t.add_input(m.declare_chirp(chirp_server, test_filename, ticket_file), "mychirp.file", cache=True)
    t.set_environment(env_with_chirp)

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
