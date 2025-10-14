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


def set_port_range(port):
    """ Sets the range for CCTools to look for free ports. """
    if isinstance(port, int):
        low_port = port
        high_port = port
    else:
        try:
            low_port, high_port = port
        except Exception:
            raise ValueError("port should be a single integer, or a sequence of two integers")

    if low_port > high_port:
        raise TypeError("high_port {} cannot be smaller than low_port {}".format(high_port, low_port))
    os.environ["TCP_LOW_PORT"] = str(low_port)
    os.environ["TCP_HIGH_PORT"] = str(high_port)


# helper function that allows a function call to access a variable from a library's state
def load_variable_from_library(var_name):
    return globals()[var_name]


# helper function that deletes all files in a directory
def delete_all_files(root_dir):
    if not os.path.exists(root_dir):
        return
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            try:
                os.remove(file_path)
            except FileNotFoundError:
                print(f"Failed to delete file {file_path}")

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
