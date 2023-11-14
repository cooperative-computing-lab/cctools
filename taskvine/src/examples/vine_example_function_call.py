#!/usr/bin/env python

# This example shows how to install a library of functions once
# as a LibraryTask, and then invoke that library remotely by
# using FunctionCall tasks.

import ndcctools.taskvine as vine
import argparse
import math
import math as m

# The library will consist of the following three functions:

def cube(x):
    # import random.uniform as uniform is not a valid Python statement
    # whenever you have a FormImport statement, put is inside of function
    from random import uniform
    from time import sleep as time_sleep

    random_delay = uniform(0.00001, 0.0001)
    time_sleep(random_delay)

    return math.pow(x, 3)

def divide(dividend, divisor):
    # straightfoward usage of preamble import statements
    return dividend / math.sqrt(divisor)

def double(x):
    # straightfoward usage of preamble import statements
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
    import_modules = [("math", "m"), "math"]
    libtask = q.create_library_from_functions('test-library', divide, double, cube, import_modules=import_modules)
    
    libtask.set_cores(1)
    libtask.set_memory(1000)
    libtask.set_disk(1000)

    q.install_library(libtask)

    print("Submitting function call tasks...")
    
    tasks = 100

    for _ in range(0, tasks):
        s_task = vine.FunctionCall('test-library', 'divide', 2, 2**2)
        q.submit(s_task)
    
        s_task = vine.FunctionCall('test-library', 'double', 3)
        q.submit(s_task)

        s_task = vine.FunctionCall('test-library', 'cube', 4)
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
    expected = tasks * (divide(2, 2**2) + double(3) + cube(4))

    print(f"Total:    {total_sum}")
    print(f"Expected: {expected}")

    assert total_sum == expected

if __name__ == '__main__':
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
