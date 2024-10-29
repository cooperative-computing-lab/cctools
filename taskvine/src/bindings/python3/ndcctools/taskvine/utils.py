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


def set_port_range(low_port, high_port):
    """ Sets the range for CCTools to look for free ports. """
    if low_port > high_port:
        raise TypeError("low_port {} should be smaller than high_port {}".format(low_port, high_port))
    os.environ["TCP_LOW_PORT"] = str(low_port)
    os.environ["TCP_HIGH_PORT"] = str(high_port)


# helper function that allows a function call to access a variable from a library's state
def load_variable_from_library(var_name):
    return globals()[var_name]


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
