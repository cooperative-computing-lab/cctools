#!/usr/bin/env python3

# This example shows how to install a library of functions once
# as a LibraryTask, and then invoke that library remotely by
# using FunctionCall tasks.

import ndcctools.taskvine as vine
import argparse
import math
import json
from ndcctools.taskvine.utils import load_variable_from_library
import os

def exp(x, y=3):
    return {'base_val': x**y}

def cube(x, with_library=False):
    # whenever using FromImport statments, put them inside of functions
    from random import uniform
    from time import sleep as time_sleep

    random_delay = uniform(0.00001, 0.0001)
    time_sleep(random_delay)

    if with_library:
        base_val = load_variable_from_library('base_val')
        return base_val + math.pow(x, 3)
    return math.pow(x, 3)

def divide(dividend, divisor, with_library=False):
    # straightfoward usage of preamble import statements
    if with_library:
        base_val = load_variable_from_library('base_val')
        return base_val + dividend / math.sqrt(divisor)
    return dividend / math.sqrt(divisor)

def double(x, with_library=False):
    import math as m
    # use alias inside of functions
    if with_library:
        base_val = load_variable_from_library('base_val')
        return base_val + m.prod([x,2])
    return m.prod([x, 2])

def func_copy_input_to_output(input_filename, output_filename):
    with open(input_filename, 'r') as f:
        contents = f.read()
    with open(output_filename, 'w') as f:
        f.write(contents)
    return 0

def main():
    parser = argparse.ArgumentParser("Test for taskvine python bindings.")
    parser.add_argument("port_file", help="File to write the port the queue is using.")

    args = parser.parse_args()

    q = vine.Manager(port=0)
    q.tune("watch-library-logfiles", 1)

    print(f"TaskVine manager listening on port {q.port}")

    with open(args.port_file, "w") as f:
        print("Writing port {port} to file {file}".format(port=q.port, file=args.port_file))
        f.write(str(q.port))

    print("Creating library from packages and functions...")

    # This format shows how to create package import statements for the library
    hoisting_modules = [math]

    
    libtask_no_context_direct = q.create_library_from_functions('test-library-no-context-direct', divide, double, cube, hoisting_modules=hoisting_modules, add_env=False, exec_mode='direct')
    
    libtask_no_context_fork = q.create_library_from_functions('test-library-no-context-fork', divide, double, cube, hoisting_modules=hoisting_modules, add_env=False, exec_mode='fork')
    
    libtask_with_context_direct = q.create_library_from_functions('test-library-with-context-direct', divide, double, cube, hoisting_modules=hoisting_modules, add_env=False, exec_mode='direct', library_context_info=[exp, [2], {'y': 3}])
    
    libtask_with_context_fork = q.create_library_from_functions('test-library-with-context-fork', divide, double, cube, hoisting_modules=hoisting_modules, add_env=False, exec_mode='fork', library_context_info=[exp, [2], {'y': 3}])

    # define special functions (1 lambda function and 1 dynamically executed function
    # lambda functions can be specified with a custom name, otherwise it will be assigned a default name by Python as "<lambda>".
    lambda_fn = lambda x : x + 1
    exec("def dyn_fn(x):\n    return x + 2", globals(), globals())
    libtask_with_special_fns = q.create_library_from_functions('test-library-with-special-fns', lambda_fn, dyn_fn, add_env=False, exec_mode='fork')

    # define a function that copies an input file to an output file.
    # this is to test the input/output staging of function calls
    libtask_with_io_fn = q.create_library_from_functions('test-library-with-io-fn', func_copy_input_to_output, add_env=False, exec_mode='fork')

    # Just take default resources for the library, this will cause it to fill the whole worker. 
    # And the number of functions slots will match the number of cores available.

    q.install_library(libtask_no_context_direct)
    q.install_library(libtask_no_context_fork)
    q.install_library(libtask_with_context_direct)
    q.install_library(libtask_with_context_fork)
    q.install_library(libtask_with_special_fns)
    q.install_library(libtask_with_io_fn)
    lib_task_names = ['test-library-no-context-direct',
                      'test-library-no-context-fork',
                      'test-library-with-context-direct',
                      'test-library-with-context-fork',
                      'test-library-with-special-fns',
                      'test-library-with-io-fn']
    print("Submitting function call tasks...")
    
    tasks = 100
    total_sum = 0    
    for lib_name in lib_task_names:
        with_library = False
        if lib_name.find('with-context') != -1:
            with_library=True
        for _ in range(0, tasks):
            if lib_name == 'test-library-with-special-fns':
                s_task = vine.FunctionCall(lib_name, '<lambda>', 1)
                q.submit(s_task)

                s_task = vine.FunctionCall(lib_name, 'dyn_fn', 1)
                q.submit(s_task)
            elif lib_name == 'test-library-with-io-fn':
                input_filename = os.path.basename(__file__) + '.input'
                output_filename = os.path.basename(__file__) + '.output'
                with open(input_filename, 'w') as f:
                    print('Test IO with function calls', file=f)
                s_task = vine.FunctionCall(lib_name, 'func_copy_input_to_output', input_filename, output_filename)
                input_file = q.declare_file(input_filename)
                output_file = q.declare_file(output_filename)
                s_task.add_input(input_file, input_filename)
                s_task.add_output(output_file, output_filename)
                q.submit(s_task)

                # do this test once only
                break
            else:
                s_task = vine.FunctionCall(lib_name, 'divide', 2, 2**2, with_library=with_library)
                q.submit(s_task)
            
                s_task = vine.FunctionCall(lib_name, 'double', 3, with_library=with_library)
                q.submit(s_task)

                s_task = vine.FunctionCall(lib_name, 'cube', 4, with_library=with_library)
                q.submit(s_task)

        while not q.empty():
            t = q.wait(5)
            if t:
                x = t.output
                try:
                    total_sum += x
                except:
                    print(x)
                    raise
                print(f"task {t.id} completed with result {x}")
        print('Done', lib_name)

    # Check that we got the right result.
    no_context_direct_expected = (tasks * (divide(2, 2**2) + double(3) + cube(4)))
    no_context_fork_expected = (tasks * (divide(2, 2**2) + double(3) + cube(4)))
    base_val = exp(2, 3)['base_val']
    with_context_direct_expected = (tasks * (divide(2, 2**2) + double(3) + cube(4) + base_val * 3))
    with_context_fork_expected = (tasks * (divide(2, 2**2) + double(3) + cube(4) + base_val * 3))
    special_fns_expected = (tasks * (lambda_fn(1) + dyn_fn(1)))
    expected = no_context_direct_expected + no_context_fork_expected + with_context_direct_expected + with_context_fork_expected + special_fns_expected

    print(f"Total:    {total_sum}")
    print(f"Expected: {expected}")

    # Check that IO test passed
    with open(input_filename, 'r') as f:
        content_input = f.read()

    with open(output_filename, 'r') as f:
        content_output = f.read()
    
    print(f"Input: {content_input}", end='')
    print(f"Output: {content_output}", end='')

    assert content_input == content_output

    assert total_sum == expected

if __name__ == '__main__':
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
