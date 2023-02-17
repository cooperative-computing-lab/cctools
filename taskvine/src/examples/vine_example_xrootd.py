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

root_files = [
    "root://eospublic.cern.ch//eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/ForHiggsTo4Leptons/SMHiggsToZZTo4L.root",
]

# define a python task that will be apply to each of the files.
def count_events(root_file):
    import uproot
    with uproot.open(root_file) as h:
        return len(h['Events'])


# construct a poncho environment to execute the tasks only needed if uproot and
# xrootd are not available where the workers execute
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
                "dependencies": [f"python={py_version}", "dill", "uproot", "xrootd"]
                }
            }

    with tempfile.NamedTemporaryFile("w", prefix="poncho-spec", encoding="utf8", dir=os.getcwd()) as f:
        json.dump(env, f)
        f.flush()
        subprocess.run(["poncho_package_create", f.name, env_name], check=True)


if __name__ == "__main__":

    env_with_xrootd = None
    # uncomment the following lines only if workers don't have uproot and xrootd
    # available
    # env_with_xrootd = "xrootd_py_env.tar.gz"
    # create_env(env_with_xrootd)

    m = vine.Manager()
    print("listening on port", m.port)

    # define the authentication file to use.
    # if not give, taskvine will try to find one in the default places
    # (X509_USER_PROXY, or /tmp/x509up_uUID)
    proxy_file = None
    # proxy_file = vine.vine_file_local("myproxy.pem")

    for root_file in root_files:
        t = vine.PythonTask(count_events, "myroot.file")
        t.add_input(vine.FileXrootD(root_file, proxy_file), "myroot.file", cache=True)
        t.set_environment(env_with_xrootd)

        task_id = m.submit(t)
        print("submitted task (id# " + str(task_id) + "): count_events()")
    print("waiting for tasks to complete...")

    while not m.empty():
        t = m.wait(5)
        if t:
            if t.result == vine.VINE_RESULT_SUCCESS:
                print(f"task {t.id} processed a file with {t.output} events")
            else:
                print(f"task {t.id} failed: {t.result_string}")

    print("all tasks complete!")
