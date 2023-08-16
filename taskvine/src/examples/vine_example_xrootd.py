#!/usr/bin/env python

# This example shows how to declare an xrootd file so that it can be cached at
# the workers.
# It assumes that uproot is installed where workers are executed. If this is
# not the case, a poncho recipe to construct this environment is also given.
#

import ndcctools.taskvine as vine

import argparse
import os
import sys

root_file = "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/ForHiggsTo4Leptons/SMHiggsToZZTo4L.root"

# define a python task that will be apply to each of the files.
def count_events(root_file):
    import uproot
    with uproot.open(root_file) as h:
        return len(h['Events'])


# construct a poncho environment to execute the tasks. Only needed if uproot and
# xrootd are not available where the workers execute
def create_env(env_name):
    import json
    import tempfile
    import subprocess
    py_version = f"{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}"

    if os.path.exists(env_name):
        print(f"reusing existing {env_name} poncho environment...")
    else:
        print(f"creating {env_name} poncho environment...")

    env = {
            "conda": {
                "channels": ["conda-forge"],
                "dependencies": [f"python={py_version}", "cloudpickle", "uproot", "xrootd"]
                }
            }

    with tempfile.NamedTemporaryFile("w", prefix="poncho-spec", encoding="utf8", dir=os.getcwd()) as f:
        json.dump(env, f)
        f.flush()
        subprocess.run(["poncho_package_create", f.name, env_name], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog="vine_example_xrootd.py",
            description="TaskVine example on how to declare a file from xrootd as an input. Prints the number of events in the file.")

    parser.add_argument('root_file', nargs='?', action='store', help='the input xrootd file', default=root_file)
    parser.add_argument('--proxy', action='store', help='optional x509 authentication proxy', default=None)
    parser.add_argument('--create-env', action='store_true', help='whether to create environment to ensure xrootd is available at the worker.', default=False)

    args = parser.parse_args()

    # create the poncho environment with xrootd if needed. This environment
    # ensures that xrootd is available at the execution site.
    # this just creates the environment in a local file, but does not declare
    # it to the manager yet.
    env_filename = None
    if args.create_env:
        env_filename = "xrootd_py_env.tar.gz"
        create_env(env_filename)


    # create the manager to now listen to connections, and register files and
    # tasks.
    m = vine.Manager()
    print(f"logs in {m.logging_directory}")
    print(f"listening on port {m.port}")

    # declare proxy and env file if needed. These vine files that can
    # be used as input to tasks.
    # if a proxy is not give, taskvine will try to find one in the default
    # places (X509_USER_PROXY, or /tmp/x509up_uUID)
    proxy = None
    if args.proxy:
        proxy = m.declare_file(args.proxy, cache=True)

    env = None
    if env_filename:
        env = m.declare_poncho(env_filename, cache=True)

    # declaring the root file, with proxy and env as needed.
    f = m.declare_xrootd(root_file, proxy, env=env, cache=True)

    # create a task from the python function count_events. The function will
    # operate on the file "myroot.file", with is the name that the input file
    # will get in the task sandbox when executing remotely.
    t = vine.PythonTask(count_events, "myroot.file")

    # the xrootd file is added as an input of the task and mapped to
    # myroot.file when the task executes.
    t.add_input(f, "myroot.file")

    # add the python environment to the task if needed. This will wrap the
    # command line so that it executes in the environment defined above.
    if env:
        t.add_environment(env)

    task_id = m.submit(t)

    print("submitted task (id# " + str(task_id) + "): count_events()")
    print("waiting for tasks to complete...")

    print("please create a worker in another terminal. E.g., for a local worker:")
    print(f"vine_worker localhost {m.port}")

    while not m.empty():
        t = m.wait(5)
        if t:
            if t.successful():
                print(f"task {t.id} processed a file with {t.output} events")
            elif t.completed():
                print(f"task {t.id} completed with an executin error, exit code {t.exit_code}")
            else:
                print(f"task {t.id} failed with status {t.result}")

    print("all tasks complete!")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
