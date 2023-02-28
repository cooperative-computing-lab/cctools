#!/usr/bin/env python3

# copyright (C) 2021- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import taskvine as vine
import sys

def find_max(nums):
    return max(nums)

numbers = [1,10,10000,100,1000,
        57,90,68,72,45,
        4268,643,985,6543,
        7854,2365,98765,123,
        12,34,56,78,90]

if __name__ == "__main__":
    m = vine.Manager()
    print("listening on port", m.port)

    print("reducing array...")
    max_number = m.tree_reduce(find_max, numbers, chunksize=3)

    print(f"maximum number is {max_number}")

