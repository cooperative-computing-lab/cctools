#!/usr/bin/env python

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows TaskVine executing a manually constructed dask graph.
# See vine_example_dask_delayed.py for an example where the graph
# is constructed by dask.

import ndcctools.taskvine as vine
import argparse
import getpass
import sys

import traceback


from operator import add  # use add function in the example graph
dsk_graph = {
    "x": 1,
    "y": 2,
    "z": (add, "x", "y"),
    "w": (sum, ["x", "y", "z"]),
    "v": [(sum, ["w", "z"]), 2],
    "t": (sum, "v")
}

expected_result = 11

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="vine_example_dask_graph.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="""This example shows TaskVine executing a manually constructed dask graph.
See vine_example_dask_delayed.py for an example where the graph
is constructed by dask.""")
    parser.add_argument(
        "--name",
        nargs="?",
        type=str,
        help="name to assign to the manager.",
        default=f"vine-dask-graph-{getpass.getuser()}",
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

    m = vine.DaskVine(port=args.port, ssl=True)
    m.set_name(args.name)
    print(f"Listening for workers at port: {m.port}")

    if args.disable_peer_transfers:
        m.disable_peer_transfers()

    # checkpoint at even levels when nodes have at least one children
    def checkpoint(dag, key):
        if dag.depth_of(key) % 2 == 0 and len(dag.get_children(key)) > 0:
            print(f"checkpoint for {key}")
            return True
        return False

    f = vine.Factory(manager=m)
    f.cores = 4
    f.max_workers = 1
    f.min_workers = 1
    with f:
        desired_keys = ["t", "w"]
        print(f"dask graph example is:\n{dsk_graph}")
        print(f"desired keys are {desired_keys}")

        try:
            results = m.get(dsk_graph, desired_keys, lazy_transfer=True, checkpoint_fn=checkpoint, resources={"cores": 1})  # 1 core per step
            print({k: v for k, v in zip(desired_keys, results)})
        except Exception:
            traceback.print_exc()

        print("Terminating workers...", end="")

    print("done!")
sys.exit(0)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
