#!/usr/bin/env python3

import ndcctools.taskvine as vine
import json
import argparse

def divide(dividend, divisor):
    import math
    return dividend/math.sqrt(divisor)

def double(x):
    return x*2

def main():
    parser = argparse.ArgumentParser("Test for taskvine python bindings.")
    parser.add_argument("port_file", help="File to write the port the queue is using.")

    args = parser.parse_args()

    q = vine.Manager(port=0)

    print(f"TaskVine manager listening on port {q.port}")

    with open(args.port_file, "w") as f:
        print("Writing port {port} to file {file}".format(port=q.port, file=args.port_file))
        f.write(str(q.port))

    print("Creating library from functions...")

    libtask = q.create_library_from_functions('test-library', divide, double, add_env=False)
    q.install_library(libtask)

    print("Submitting function call tasks...")
    
    tasks = 100

    for i in range(0,tasks): 
        s_task = vine.FunctionCall('test-library', 'divide', 2, 2**2)
        q.submit(s_task)
    
        s_task = vine.FunctionCall('test-library', 'double', 3)
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
    expected = tasks * ( divide(2, 2**2) + double(3) )

    print(f"Total:    {total_sum}")
    print(f"Expected: {expected}")

    assert total_sum == expected

if __name__ == '__main__':
    main()

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
