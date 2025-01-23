#! /usr/bin/env python

import sys
import ndcctools.taskvine as vine

port_file = None
try:
    port_file = sys.argv[1]
except IndexError:
    sys.stderr.write("Usage: {} PORTFILE\n".format(sys.argv[0]))
    raise

def main():
    executor = vine.FuturesExecutor(
        port=[9123, 9129], manager_name="vine_hof_test", factory=False
    )

    print("listening on port {}".format(executor.manager.port))
    with open(port_file, "w") as f:
        f.write(str(executor.manager.port))

    nums = list(range(101))

    maximum = executor.reduce(max, nums, fn_arity=2)
    assert maximum.result() == 100

    maximum = executor.reduce(max, nums, fn_arity=25)
    assert maximum.result() == 100

    maximum = executor.reduce(max, nums, fn_arity=1000)
    assert maximum.result() == 100

    maximum = executor.reduce(max, nums, fn_arity=2, chunk_size=50)
    assert maximum.result() == 100

    minimum = executor.reduce(min, nums, fn_arity=2, chunk_size=50)
    assert minimum.result() == 0

    total = executor.reduce(sum, nums, fn_arity=11, chunk_size=13)
    assert total.result() == sum(nums)




if __name__ == "__main__":
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
