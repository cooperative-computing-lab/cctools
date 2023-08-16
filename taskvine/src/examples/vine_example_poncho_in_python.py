#!/usr/bin/env python3

# Example on how to execute python code while creating an poncho package within python.

# A poncho specification can be given by a string or dictionary representation.
# The function package_create.dict_to_env(spec) will create the corresponding 
# poncho package. The package will be cached when cache=True. The default location
# of the poncho cache is `.poncho_cache` but can be modiied by providing a cache path
# like so: cache_path=`<CACHE_PATH>`. If a corresponding poncho package is present in
# the cache, the path to the corresponding package will be returned. Setting the 
# argument force=True will recreate the even if the package is present in the cache.

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


import ndcctools.taskvine as vine
import argparse
from poncho import package_create

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

    env_spec = {"conda": {"channels": ["conda-forge"],"packages": ["python","pip","conda","conda-pack","cloudpickle","xrootd"]},"pip": ["matplotlib"]}
    env_tarball = package_create.dict_to_env(env_spec, cache=True)
    env_file = q.declare_poncho(env_tarball, cache=True)

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
