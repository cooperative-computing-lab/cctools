#!/usr/bin/env python

# This example shows how to declare an chirp file
# so that it can be cached at the workers.
# It assumes that chirp is installed where workers are executed. If this is
# not the case, a starch recipe to construct this environment is also given.
#

import ndcctools.taskvine as vine

import argparse
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
    # add the executables chirp, chirp_get, and chirp_put to the env_name starch file.
    # these executables are assumed to be in the current $PATH.
    # by default, the starch file will execute the chirp command.

    if os.path.exists(env_name):
        print(f"reusing existing {env_name} starch file...")
    else:
        print(f"creating {env_name} starch file...")
        subprocess.run(["starch", "-x", "chirp_get", "-x", "chirp_put", "-x", "chirp", "-c", "chirp", env_name], check=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog="vine_example_chirp.py",
            description="TaskVine example on how to declare a file from a chirp server as an input. Prints the number of lines of filename from chirp_server")

    parser.add_argument('chirp_server', action='store', help='the chirp server where the file is located')
    parser.add_argument('filename', action='store', help='the name of the file')
    parser.add_argument('--ticket', action='store', help='optional server authentication ticket', default=None)
    parser.add_argument('--create-env', action='store_true', help='whether to create mini environment to ensure chirp is available at the worker.', default=False)

    args = parser.parse_args()

    # create the chirp environment if needed. This just creates the environment
    # in a local file, but does not registers it with a manager. The
    # environment created comes from a starch file.
    env_filename = None
    if args.create_env:
        env_filename = "chirp_client.sfx"
        create_env(env_filename)

    # create the manager to now listen to connections, and register files and
    # tasks.
    m = vine.Manager()
    print(f"logs in {m.logging_directory}")
    print(f"listening on port {m.port}")

    # declare ticket and env file if needed. These vine files that can
    # be used as input to tasks.
    ticket = None
    if args.ticket:
        ticket = m.declare_file(args.ticket, cache=True)

    env = None
    if env_filename:
        env = m.declare_starch(env_filename, cache=True)

    # declaring the file as coming from chirp server
    chirp_file = m.declare_chirp(args.chirp_server, args.filename, ticket=ticket, env=env, cache=True)

    # create a task from the python function count_lines. The function will
    # operate on the file "mychirp.file", with is the name that the input file
    # will get in the task sandbox when executing remotely.
    t = vine.PythonTask(count_lines, "mychirp.file")

    # the chrip file is added as an input of the task and mapped to
    # mychirp.file when the task executes.
    t.add_input(chirp_file, "mychirp.file")

    task_id = m.submit(t)
    print("submitted task (id# " + str(task_id) + "): count_lines()")
    print("waiting for tasks to complete...")

    print("please create a worker in another terminal. E.g., for a local worker:")
    print(f"vine_worker localhost {m.port}")

    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task {t.id} processed a file with {t.output} lines")
            elif t.completed():
                print(f"task {t.id} completed with an executin error, exit code {t.exit_code}")
            else:
                print(f"task {t.id} failed with status {t.result}")

    print("all tasks complete!")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
