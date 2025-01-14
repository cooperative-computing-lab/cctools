#!/usr/bin/env python3

# This test exercises the (rather complex) coupled failure mode
# of libraries and tasks together.  If a user submits a library
# with a bunch of function calls, but the library keeps crashing,
# then the following should eventually occur:

# 1 - The library template will be removed, and no more library
#     instances will be deployed.
# 2 - The function calls depending on that library should return
#     to the user with error LIBRARY_MISSING.

import ndcctools.taskvine as vine
import argparse
import os
import sys

def func( x ) :
    sys.exit(127)
    return x*x

def main():
    parser = argparse.ArgumentParser("Test for taskvine python serverless failure.")
    parser.add_argument("port_file", help="File to write the port the queue is using.")

    args = parser.parse_args()

    q = vine.Manager(port=0)
    q.tune("watch-library-logfiles", 1)  # get logs back from failed libraries
    q.tune("transient-error-interval",2) # retry failed libraries quickly
    q.tune("max-library-retries", 2)     # retry failed libraries only twice
    print(f"TaskVine manager listening on port {q.port}")

    with open(args.port_file, "w") as f:
        print("Writing port {port} to file {file}".format(port=q.port, file=args.port_file))
        f.write(str(q.port))

    print("Creating library from packages and functions...")
    libtask = q.create_library_from_functions("bad-library",func,add_env=False,exec_mode="direct")
    q.install_library(libtask)

    print("Submitting function call tasks...")
    for n in range(0,10):
        task = vine.FunctionCall("bad-library","func",n)
        q.submit(task)

    while not q.empty():
        t = q.wait(5)
        if t:
            print(f"task {t.id} completed with result {t.result} output {t.output}")

if __name__ == '__main__':
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
