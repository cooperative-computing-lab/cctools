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
        port=[9123, 9129], manager_name="test-executor", factory=False
    )
    print("listening on port {}".format(executor.port))
    with open(port_file, "w") as f:
        f.write(str(executor.port))

    # Create library task
    print("creating library from functions...")
    libtask = executor.create_library_from_functions(
        "test-library", my_sum, failure, hoisting_modules=None, add_env=False
    )

    # Install library on executor.manager
    executor.install_library(libtask)

    # Submit several tasks for execution
    t1 = executor.future_funcall("test-library", "my_sum", 7, 4)
    a = executor.submit(t1)

    t2 = executor.future_funcall("test-library", "my_sum", a, a)
    b = executor.submit(t2)

    t3 = executor.future_funcall("test-library", "my_sum", b, a)
    c = executor.submit(t3)

    print("waiting for result...")

    res = t3.output()
    print(f"t3 output = {c.result()}")

    a = 7 + 4
    b = a + a
    c = a + b
    assert res == c

    # Test failure handling
    f1 = executor.future_funcall("test-library", "failure")
    future = executor.submit(f1)

    try:
        future.result()
    except Exception as e:
        # FutureFunctionCall wraps the exception string in a
        # FunctionCallNoResult so we check the original error message is
        # present in the raised error.
        assert "Expected failure." in str(e)
        assert future.exception() is not None
    else:
        raise RuntimeError("Future did not raise exception.")


if __name__ == "__main__":
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
