# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import cctools.taskvine.cvine as cvine


def get_c_constant(constant):
    """ Returns a TaskVine C constant value from a string. E.g.:
        "result_success" -> VINE_RESULT_SUCCESS
    """
    constant = f"VINE_{constant.upper()}"
    return getattr(cvine, constant)
