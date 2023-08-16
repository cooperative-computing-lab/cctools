#!/usr/bin/env python3

# Example on how to execute python code with a taskvine task.
# The class PythonTask allows users to execute python functions as taskvine
# commands. Functions and their arguments are pickled to a file and executed
# utilizing a wrapper script to execut the function. the output of the executed
# function is then written to a file as an output file and read when neccesary
# allowing the user to get the result as a python variable during runtime and
# manipulated later.

# A PythonTask object is created as `p_task = PyTask.PyTask(func, args)` where
# `func` is the name of the function and args are the arguments needed to
# execute the function. PythonTask can be submitted to a queue as regular Work
# Queue functions, such as `q.submit(p_task)`.
#
# When a task has completed, the resulting python value can be retrieved by calling
# the output method, such as: `x  = t.output` where t is the task retuned by
# `t = q.wait()`.
#
# By default, the task will run assuming that the worker is executing inside an
# appropiate  python environment. If this is not the case, an environment file
# can be specified with: `t.add_environment("env.tar.gz")`, in which
# env.tar.gz is created with the poncho_package_create tool, and has at least a python
# installation, with cloudpickle.
#
# A minimal conda environment 'env.tar.gz' can be created with:
#
# poncho spec in a json file: spec.json (change python version to your current python installation)
"""
{
    "conda": {
        "channels": ["conda-forge"],
        "dependencies": ["python=3.X", "cloudpickle"]
    }
}
"""
# poncho_package_create spec.json env.tar.gz


import ndcctools.taskvine as vine
import argparse

def divide(dividend, divisor):
    import math
    return dividend/math.sqrt(divisor)

def main():
    parser = argparse.ArgumentParser(
        prog="vine_example_blast.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--disable-peer-transfers",
        action="store_true",
        help="disable transfers among workers.",
        default=False,
    )

    q = vine.Manager(9123)

    args = parser.parse_args()

    if args.disable_peer_transfers:
        q.disable_peer_transfers()

    env_file = None
    # if python environment is missing, create an environment as explained
    # above,  and uncomment next line.
    # env_file = m.declare_poncho("env.tar.gz", cache=True)
    env_file = q.declare_poncho("output.tar.gz", cache=True)

    for i in range(1, 16):
        p_task = vine.PythonTask(divide, 1, i**2)
        p_task.add_environment(env_file)

        q.submit(p_task)

    total_sum = 0
    while not q.empty():
        t = q.wait(5)
        if t:
            x = t.output
            if isinstance(x, vine.PythonTaskNoResult):
                print(f"Task {t.id} failed and did not generate a result.")
            else:
                total_sum += x
            print(total_sum)


if __name__ == '__main__':
    main()
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
