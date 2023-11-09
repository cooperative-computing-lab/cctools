#!/usr/bin/env python

# This example shows how to install a library of functions once
# as a LibraryTask, and then invoke that library remotely by
# using FunctionCall tasks.

import ndcctools.taskvine as vine
import argparse
import time
import math as m
from random import randint
from decimal import Decimal as D

# The library will consist of the following two functions:

# This function is only used to show the usage of various import statements
def calculate_with_delay():
    random_int = randint(1, 10)
    sqrt_value = m.sqrt(random_int)
    time.sleep(0.001)
    D(sqrt_value)
    return None

def divide(dividend, divisor):
    return dividend / m.sqrt(divisor)

def double(x):
    return x * 2


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

    # This format shows how to create package import statements in the library
    imports = {
        'time': [],                        # import time
        'math': 'm',                       # import math as m
        'random': ['randint'],             # from random import randint
        'decimal': {'Decimal': 'D'}        # from decimal import Decimal as D
    }
    libtask = q.create_library_from_functions('test-library', divide, double, imports=imports)
    q.install_library(libtask)

    print("Submitting function call tasks...")
    
    tasks = 100

    for _ in range(0, tasks): 
        s_task = vine.FunctionCall('test-library', 'divide', 2, 2**2)
        q.submit(s_task)
    
        s_task = vine.FunctionCall('test-library', 'double', 3)
        q.submit(s_task)

        s_task = vine.FunctionCall('test-library', 'calculate_with_delay')
        q.submit(s_task)

    print("Waiting for results...")

    total_sum = 0
    x = 0
    while not q.empty():
        t = q.wait(5)
        if t:
            x = t.output
            if x == None:
                continue
            total_sum += x
            print(f"task {t.id} completed with result {x}")

    # Check that we got the right result.
    expected = tasks * ( divide(2, 2**2) + double(3) )

    print(f"Total:    {total_sum}")
    print(f"Expected: {expected}")

    assert total_sum == expected

if __name__ == '__main__':
    main()
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
