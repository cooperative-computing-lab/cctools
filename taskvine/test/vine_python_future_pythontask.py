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


def failure():
    raise Exception('Expected failure.')


def main():
    executor = vine.FuturesExecutor(
        port=[9123, 9129], manager_name="vine_matrtix_build_test", factory=False
    )
    print("listening on port {}".format(executor.manager.port))
    with open(port_file, "w") as f:
        f.write(str(executor.manager.port))

    # Submit several tasks for execution:
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

    res = t3.output()
    print(f"t3 output = {c.result()}")
    assert c.exception() is None

    a = 7 + 4
    b = a + a
    c = a + b
    assert res == c

    f1 = executor.future_task(failure)
    f1.set_cores(1)
    future = executor.submit(f1)

    try:
        future.result()
    except Exception as e:
        assert str(e) == "Expected failure."
        assert future.exception() == e
    else:
        raise RuntimeError("Future did not raise exception.")


if __name__ == "__main__":
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
