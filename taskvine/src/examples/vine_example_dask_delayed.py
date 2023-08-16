#!/usr/bin/env python

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows TaskVine executing a dask workflow

import ndcctools.taskvine as vine
import argparse
import getpass

try:
    import dask
except ImportError:
    print("You need dask installed (e.g. conda install -c conda-forge dask) to run this example.")


from operator import add  # use add function in the example graph


# delayed functions we use in the graph
@dask.delayed(pure=True)
def sum_d(args):
    return sum(args)


@dask.delayed(pure=True)
def add_d(*args):
    return add(*args)


# define the graph using delayed functions
z = add_d(1, 2)
w = sum_d([1, 2, z])
v = [sum_d([z, w]), 2]
t = sum_d(v)
expected_result = 11

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="vine_example_dask_delayed.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="""This example shows TaskVine executing a dask workflow.""")
    parser.add_argument(
        "--name",
        nargs="?",
        type=str,
        help="name to assign to the manager.",
        default=f"vine-dask-delayed-{getpass.getuser()}",
    )
    parser.add_argument(
        "--port",
        nargs="?",
        type=int,
        help="port for the manager to listen for connections. If 0, pick any available.",
        default=9123,
    )
    parser.add_argument(
        "--disable-peer-transfers",
        action="store_true",
        help="disable transfers among workers.",
        default=False,
    )

    args = parser.parse_args()

    # define a TaskVine manager that has a get method that executes dask task graphs
    m = vine.DaskVine(port=args.port, ssl=True)
    m.set_name(args.name)
    print(f"Listening for workers at port: {m.port}")

    if args.disable_peer_transfers:
        m.disable_peer_transfers()

    f = vine.Factory(manager=m)
    f.cores = 4
    f.max_workers = 1
    f.min_workers = 1
    with f:
        # use the get method as the scheduler for dask
        with dask.config.set(scheduler=m.get):
            result = t.compute(resources={"cores": 1})  # resources per function call
            print(f"t = {result}")
        print("Terminating workers...", end="")

    print("done!")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
