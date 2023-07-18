#!/usr/bin/env python
import ndcctools.taskvine as vine
import json
import argparse

def divide(dividend, divisor):
    import math
    return dividend/math.sqrt(divisor)

def double(x):
    return x*2

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

    q = vine.Manager(9123)

    args = parser.parse_args()

    if args.disable_peer_transfers:
        q.disable_peer_transfers()
    else:
        q.enable_peer_transfers()

    function_lib = q.create_library_from_functions('test-library', divide, double)
    q.install_library(function_lib)

    s_task = vine.FunctionCall('divide', 'test-library', 2, 2**2)
    q.submit(s_task)
    
    s_task = vine.FunctionCall('double', 'test-library', 3)
    q.submit(s_task)

    total_sum = 0
    x = 0
    while not q.empty():
        t = q.wait(5)
        if t:
            x = json.loads(t.output)['Result']
        total_sum += x
    assert total_sum == divide(2, 2**2) + double(3)

if __name__ == '__main__':
    main()
