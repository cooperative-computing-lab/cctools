# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from . import cvine

import os

def get_c_constant(constant):
    """ Returns a TaskVine C constant value from a string. E.g.:
        "result_success" -> VINE_RESULT_SUCCESS
    """
    constant = f"VINE_{constant.upper()}"
    return getattr(cvine, constant)


def rmsummary_snapshots(self):
    """ Converts snapshot array from C to a python list """
    if self.snapshots_count < 1:
        return None

    snapshots = []
    for i in range(0, self.snapshots_count):
        snapshot = cvine.rmsummary_get_snapshot(self, i)
        snapshots.append(snapshot)
    return snapshots


cvine.rmsummary.snapshots = property(rmsummary_snapshots)


def set_port_range(low_port, high_port):
    """ Sets the range for CCTools to look for free ports. """
    if low_port > high_port:
        raise TypeError("low_port {} should be smaller than high_port {}".format(low_port, high_port))
    os.environ["TCP_LOW_PORT"] = str(low_port)
    os.environ["TCP_HIGH_PORT"] = str(high_port)
