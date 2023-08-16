#!/usr/bin/env python

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows TaskVine executing a dask workflow that uses awkward arrays.

import ndcctools.taskvine as vine
import argparse
import getpass
import sys

try:
    import dask
    import awkward as ak
    import dask_awkward as dak
    import numpy as np
except ImportError:
    print("You need dask, awkward, and numpy installed")
    print("(e.g. conda install -c conda-forge dask dask-awkward numpy) to run this example.")


behavior: dict = {}


@ak.mixin_class(behavior)
class Point:
    def distance(self, other):
        return np.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="vine_example_dask_awk_array.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="""This example shows TaskVine executing a dask workflow that uses awkward arrays""")
    parser.add_argument(
        "--name",
        nargs="?",
        type=str,
        help="name to assign to the manager.",
        default=f"vine-dask-awkward{getpass.getuser()}",
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

    # data arrays
    points1 = ak.Array([
        [{"x": 1.0, "y": 1.1}, {"x": 2.0, "y": 2.2}, {"x": 3, "y": 3.3}],
        [],
        [{"x": 4.0, "y": 4.4}, {"x": 5.0, "y": 5.5}],
        [{"x": 6.0, "y": 6.6}],
        [{"x": 7.0, "y": 7.7}, {"x": 8.0, "y": 8.8}, {"x": 9, "y": 9.9}],
    ])

    points2 = ak.Array([
        [{"x": 0.9, "y": 1.0}, {"x": 2.0, "y": 2.2}, {"x": 2.9, "y": 3.0}],
        [],
        [{"x": 3.9, "y": 4.0}, {"x": 5.0, "y": 5.5}],
        [{"x": 5.9, "y": 6.0}],
        [{"x": 6.9, "y": 7.0}, {"x": 8.0, "y": 8.8}, {"x": 8.9, "y": 9.0}],
    ])

    # construct the dask graph
    array1 = dak.from_awkward(points1, npartitions=3)
    array2 = dak.from_awkward(points2, npartitions=3)

    array1 = dak.with_name(array1, name="Point", behavior=behavior)
    array2 = dak.with_name(array2, name="Point", behavior=behavior)

    distance = array1.distance(array2)

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
        with dask.config.set(scheduler=m.get):
            result = distance.compute(resources={"cores": 1}, resources_mode="max", lazy_transfer=True)
            print(f"distance = {result}")
        print("Terminating workers...", end="")
    print("done!")
sys.exit(0)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
