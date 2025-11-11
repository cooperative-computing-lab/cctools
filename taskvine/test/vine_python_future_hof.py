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
    executor = vine.FuturesExecutor(port=0,factory=False)

    print("listening on port {}".format(executor.manager.port))
    with open(port_file, "w") as f:
        f.write(str(executor.manager.port))

    nums = list(range(101))

    rows = 3
    mult_table = executor.allpairs(lambda x, y: x*y, range(rows), nums, chunk_size=11).result()
    assert sum(mult_table[1]) == sum(nums)
    assert sum(sum(r) for r in mult_table) == sum(sum(nums) * n for n in range(rows))

    doubles = executor.map(lambda x: 2*x, nums, chunk_size=10).result()
    assert sum(doubles) == sum(nums)*2

    doubles = executor.map(lambda x: 2*x, nums, chunk_size=13).result()
    assert sum(doubles) == sum(nums)*2

    maximum = executor.reduce(max, nums, fn_arity=2).result()
    assert maximum == 100

    maximum = executor.reduce(max, nums, fn_arity=25).result()
    assert maximum == 100

    maximum = executor.reduce(max, nums, fn_arity=1000).result()
    assert maximum == 100

    maximum = executor.reduce(max, nums, fn_arity=2, chunk_size=50).result()
    assert maximum == 100

    minimum = executor.reduce(min, nums, fn_arity=2, chunk_size=50).result()
    assert minimum == 0

    total = executor.reduce(sum, nums, fn_arity=11, chunk_size=13).result()
    assert total == sum(nums)




if __name__ == "__main__":
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
