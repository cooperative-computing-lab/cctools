#!/usr/bin/env python3

# This example shows how to install a library of functions
# and show that they can be composed.

import ndcctools.taskvine as vine
import argparse


def divide(dividend, divisor):
    return dividend / divisor


def double(x):
    return 2 * x


def composed(x, y):
    return divide(double(x), y)


def main():
    parser = argparse.ArgumentParser("Test for taskvine python bindings.")
    parser.add_argument("port_file", help="File to write the port the queue is using.")

    args = parser.parse_args()

    q = vine.Manager(port=0)
    q.tune("watch-library-logfiles", 1)

    print(f"TaskVine manager listening on port {q.port}")

    with open(args.port_file, "w") as f:
        print(
            "Writing port {port} to file {file}".format(
                port=q.port, file=args.port_file
            )
        )
        f.write(str(q.port))

    print("Creating library from packages and functions...")

    # This format shows how to create package import statements for the library
    fns = [divide, double, composed]

    library = q.create_library_from_functions(
        "test-library",
        *fns,
        add_env=False,
    )

    q.install_library(library)

    print("Submitting function call task...")

    t = vine.FunctionCall("test-library", "composed", 1024, 4)
    q.submit(t)

    expected = composed(1024, 4)
    while not q.empty():
        t = q.wait(5)
        if t:
            r = t.output
            print(f"task {t.id} completed with result {r}. Expected {expected}.")
            assert r == expected


if __name__ == "__main__":
    main()
