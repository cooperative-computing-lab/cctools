#!/usr/bin/env python3

# Example of how to use taskvine high order functions
import ndcctools.taskvine as vine

def main():
    # Set up queue
    q = vine.Manager(port=9123)

    # map - similar to Python's own map function, but uses a taskvine worker
    # to complete computation. Returns sequence with the results from the given function
    # [result] = q.map(func, sequence)
    # Example: (returns [1, 4, 9, 16])
    results = q.map(lambda x: x*x, [1, 2, 3, 4])
    print(results)

    # pair - similar to map function, but uses the function for every pair between
    # the two sequences. Returns sequence of results of each pair.
    # [result] = q.pair(func, sequence1, sequence2)
    # Example: (returns [1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16])
    results = q.pair(lambda x, y: x*y, [1, 2, 3, 4], [1, 2, 3, 4])
    print(results)

    # tree_reduce - combines pairs of values using a given function, and then returns
    # to a single final number after reducing the sequence.
    # result = q.tree_reduce(func, sequence)
    # Example (even): (returns 24)
    results = q.tree_reduce(lambda x, y: x*y, [1, 2, 3, 4])
    print(results)

    # Example (odd): (returns 120)
    results = q.tree_reduce(lambda x, y: x*y, [1 ,2, 3, 4, 5])
    print(results)


if __name__ == "__main__":
    main()
