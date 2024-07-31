#! /usr/bin/env python

import sys
import ndcctools.taskvine as vine

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise


# Define a function to invoke remotely
def my_sum(x, y, negate=False):
    from operator import add, mul

    f = 1
    if negate:
        f = -1
    s = mul(f, add(x, y))
    return s


def main():
    executor = vine.FuturesExecutor(
        port=[9123, 9129], manager_name="vine_matrtix_build_test", factory=False
    )
    print("listening on port {}".format(executor.manager.port))
    with open(port_file, "w") as f:
        f.write(str(executor.manager.port))

    # Submit several tasks for wait function:
    print("submitting tasks...")

    t1 = executor.future_task(my_sum, 7, 4)
    t1.set_cores(1)
    a = executor.submit(t1)

    t2 = executor.future_task(my_sum, a, a)
    t2.set_cores(1)
    b = executor.submit(t2)

    t3 = executor.future_task(my_sum, a, b)
    t3.set_cores(1)
    c = executor.submit(t3)
    
    print("waiting for result...")
    results = vine.futures.wait([a, b, c])
    done = results.done
    not_done = results.not_done
    print(f"results = DONE: {done}\n NOT DONE: not_done")

    # Submit several tasks for as_completed function:
    print("submitting tasks...")

    t1 = executor.future_task(my_sum, 7, 4)
    t1.set_cores(1)
    a = executor.submit(t1)

    t2 = executor.future_task(my_sum, a, a)
    t2.set_cores(1)
    b = executor.submit(t2)

    t3 = executor.future_task(my_sum, a, b)
    t3.set_cores(1)
    c = executor.submit(t3)
    
    print("waiting for result...")
    results = vine.futures.as_completed([a, b, c])
    print(f"results = {results}")

if __name__ == "__main__":
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
