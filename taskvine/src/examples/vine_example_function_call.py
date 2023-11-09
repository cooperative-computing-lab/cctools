#!/usr/bin/env python

# This example shows how to install a library of functions once
# as a LibraryTask, and then invoke that library remotely by
# using FunctionCall tasks.

import ndcctools.taskvine as vine
import math
import argparse
import numpy as np
from time import sleep as time_sleep
from random import uniform

# The library will consist of the following three functions:

def cube_sqrt(x):
    random_delay = uniform(0.00001, 0.0001)
    time_sleep(random_delay)

    sqrt_value = math.sqrt(x)
    cube_value = np.power(sqrt_value, 3)

    return cube_value

def divide(dividend, divisor):
    return dividend / math.sqrt(divisor)

# Package imports can also be inside of the function
def double(x):
    import math as m
    return m.prod([x, 2])


def main():
    parser = argparse.ArgumentParser(
        prog="vine_example_function_call.py",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--disable-peer-transfers",
        action="store_true",
        help="disable transfers among workers.",
        default=False,
    )

    q = vine.Manager(port=9123)

    print(f"TaskVine manager listening on port {q.port}")

    args = parser.parse_args()

    if args.disable_peer_transfers:
        q.disable_peer_transfers()
    else:
        q.enable_peer_transfers()

    print("Creating library from packages and functions...")

    # This format shows how tocd create package import statements for the library
    imports = {
        'math': '',                        # import math
        'time': '',                        # import time
        'numpy': 'np',                     # import numpy as np
        'random': {'uniform': ''},         # from random import uniform
        'time': {'sleep': 'time_sleep'}    # from time import sleep as time_sleep
    }
    libtask = q.create_library_from_functions('test-library', divide, double, cube_sqrt, imports=imports)
    q.install_library(libtask)

    print("Submitting function call tasks...")
    
    tasks = 100

    for _ in range(0, tasks): 
        s_task = vine.FunctionCall('test-library', 'divide', 2, 2**2)
        q.submit(s_task)
    
        s_task = vine.FunctionCall('test-library', 'double', 3)
        q.submit(s_task)

        s_task = vine.FunctionCall('test-library', 'cube_sqrt', 4)
        q.submit(s_task)

    print("Waiting for results...")

    total_sum = 0
    x = 0
    while not q.empty():
        t = q.wait(5)
        if t:
            x = t.output
            total_sum += x
            print(f"task {t.id} completed with result {x}")

    # Check that we got the right result.
    expected = tasks * ( divide(2, 2**2) + double(3) + cube_sqrt(4))

    print(f"Total:    {total_sum}")
    print(f"Expected: {expected}")

    assert total_sum == expected

if __name__ == '__main__':
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
