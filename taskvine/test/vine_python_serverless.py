#!/usr/bin/env python3

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
    parser = argparse.ArgumentParser("Test for taskvine python bindings.")
    parser.add_argument("port_file", help="File to write the port the queue is using.")

    args = parser.parse_args()

    q = vine.Manager(port=0)

    print(f"TaskVine manager listening on port {q.port}")

    with open(args.port_file, "w") as f:
        print("Writing port {port} to file {file}".format(port=q.port, file=args.port_file))
        f.write(str(q.port))

    print("Creating library from packages and functions...")

    # This format shows how to create package import statements for the library
    imports = {
        'math': [],                        # import math
        'time': [],                        # import time
        'numpy': 'np',                     # import numpy as np
        'random': ['uniform'],             # from random import uniform
        'time': {'sleep': 'time_sleep'}    # from decimal import Decimal as D
    }
    libtask = q.create_library_from_functions('test-library', divide, double, cube_sqrt, imports=imports)
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
