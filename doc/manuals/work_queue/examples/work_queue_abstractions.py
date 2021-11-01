#!/usr/bin/env python3

# Example of how to use work_queue abstractions
import work_queue as wq

def main():
    # Set up queue
    q = wq.WorkQueue(port=9123)

    # map - similar to Python's own map function, but uses a work_queue worker
    # to complete computation. Returns array with the results from the given function
    # [result] = q.map(func, array)
    # Example: (returns [1, 4, 9, 16])
    results = q.map(lambda x: x*x, [1, 2, 3, 4])
    print(results)

    # pair - similar to map function, but uses the function for every pair between
    # the two arrays. Returns array of results of each pair.
    # [result] = q.pair(func, array1, array2)
    # Example: (returns [1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16])
    results = q.pair(lambda x, y: x*y, [1, 2, 3, 4], [1, 2, 3, 4])
    print(results)

    # treeReduce - combines pairs of values using a given fucntion, and then returns
    # to a single final number after reducing the array.
    # result = q.treeReduce(func, array)
    # Example (even): (returns 24)
    results = q.treeReduce(lambda x, y: x*y, [1, 2, 3, 4])
    print(results)

    # Example (odd): (returns 120)
    results = q.treeReduce(lambda x, y: x*y, [1 ,2, 3, 4, 5])
    print(results)


if __name__ == "__main__":
    main()
