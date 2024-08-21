#! /usr/bin/env python

import sys
import ndcctools.taskvine as vine
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import FIRST_EXCEPTION
from concurrent.futures import ALL_COMPLETED
from concurrent.futures import TimeoutError

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


def my_exception():
    raise Exception("Expected failure.")


def my_timeout():
    import time
    time.sleep(60)


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
    results = list(vine.futures.as_completed([a, b, c]))
    print(f"results = {results}")

    # Test timeouts
    t1 = executor.future_task(my_timeout)
    t1.set_cores(1)
    future = executor.submit(t1)

    try:
        future.result(timeout=1)
    except TimeoutError:
        future.cancel()
        print("timeout raised correctly")
    else:
        raise RuntimeError("TimeoutError was not raised correctly.")

    # # Test error handling with wait
    t1 = executor.future_task(my_sum, 7, 4)
    t1.set_cores(1)
    a = executor.submit(t1)

    t2 = executor.future_task(my_timeout)
    t2.set_cores(1)
    b = executor.submit(t2)

    results = vine.futures.wait([a, b], return_when=FIRST_COMPLETED)
    assert len(results.done) == 1
    assert len(results.not_done) == 1
    assert results.done.pop().result() == 11

    results = vine.futures.wait([a, b], timeout=2, return_when=ALL_COMPLETED)
    assert len(results.done) == 1
    assert len(results.not_done) == 1
    assert results.done.pop().result() == 11

    t3 = executor.future_task(my_exception)
    t3.set_cores(1)
    c = executor.submit(t3)

    results = vine.futures.wait([b, c], return_when=FIRST_EXCEPTION)
    assert len(results.done) == 1
    assert len(results.not_done) == 1
    assert results.done.pop().exception() is not None

    # Cancel the task that is still sleeping
    b.cancel()

    # Test timeouts with as_completed
    t1 = executor.future_task(my_sum, 7, 4)
    t1.set_cores(1)
    a = executor.submit(t1)

    t2 = executor.future_task(my_timeout)
    t2.set_cores(1)
    b = executor.submit(t2)

    iterator = vine.futures.as_completed([a, b], timeout=5)

    # task 1 should complete correctly within the timeout and be yielded first
    a_future = next(iterator)
    assert a_future.result() == 11

    try:
        next(iterator)
    except TimeoutError:
        b.cancel()
        print("as_completed raised timeout correctly")
    else:
        raise RuntimeError("TimeoutError was not raised correctly.")


if __name__ == "__main__":
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
