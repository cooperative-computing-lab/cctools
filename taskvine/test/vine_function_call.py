#!/usr/bin/env python
import ndcctools.taskvine as vine
import json
import sys

def divide(dividend, divisor):
    import math
    return dividend/math.sqrt(divisor)

def double(x):
    return x*2

def main():
    q = vine.Manager([9123, 9130])

    port_file = None
    try:
        port_file = sys.argv[1]
    except IndexError:
        sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
        raise
    with open(port_file, 'w') as f:
        f.write(str(q.port))

    function_lib = q.create_library_from_functions('test-library', divide, double)
    q.install_library(function_lib)

    s_task = vine.FunctionCall('test-library', 'divide', 2, 2**2)
    q.submit(s_task)
    
    s_task = vine.FunctionCall('test-library', 'double', 3)
    q.submit(s_task)

    total_sum = 0
    x = 0
    while not q.empty():
        t = q.wait(5)
        if t:
            x = t.output
        total_sum += x
    assert total_sum == divide(2, 2**2) + double(3)

if __name__ == '__main__':
    main()
