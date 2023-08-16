#!/usr/bin/env python3

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This shows an example of using Library Tasks and FunctionCall Tasks.
# Gradient descent is an algorithm used to optimize the weights of machine
# learning models and regressions.  Running multiple instances of gradient
# descent can be used to overcome local minima and approach the global minimum
# for the best model possible.

# The manager first creates a Library Task that includes the code for the
# gradient descent algorithm, and installs it on connected workers.  The
# manager then creates FunctionCall Task that run individual instance of
# gradient descent for a randomized model.  The manager runs many of these
# tasks, and returns the set of weights that had the lowest overall error.

import ndcctools.taskvine as vine
import json
import numpy as np
import random
import argparse
import getpass


def batch_gradient_descent(train_data, test_data, number_of_params, max_iterations, min_error, learning_rate):
    """ This is the function we will pack into a Library Task.
        arguments:
        train_data:       List of data to train a model with simple linear regression
                        with polynomial basis functions
        test_data:        List of data to test the fit of the model
        number_of_params: the number of parameters/basis functions in the model
        max_iterations:   maximum number of iterations of weight updates to perform
        min_error:        continue training until the difference in error between
                        iterations is less than this value - or max_iterations is reached.
        learning_rate:    how much to update the weights each iteration

        It returns the optimized set of weights """

    # Note that we import the python modules again inside the function. This is
    # because this function will be executed remotely independent from this
    # current python program.
    import random
    import time
    import numpy as np

    # seed the random number generator to ensure this model has a random set of weights
    random.seed(time.time())

    # convert the training and testing data back into np arrays
    train_data = np.array(train_data)
    test_data = np.array(test_data)

    # compute the phi matrix. each row is a different x_i, and each column is (x_i)^n where n is the column number
    phi = np.matrix([[x[0] ** i for i in range(number_of_params)] for x in train_data])

    # ground truth observed value in the training data
    observed_values = np.matrix([[x[1]] for x in train_data])

    # initial randomized w matrix
    w_initial = np.matrix([[random.random()] for x in range(number_of_params)])

    # calculuate the RMS error of the set of weights passed in the argument and return it
    def calculate_error(w_i):
        return 1 / 2 * (phi @ w_i - observed_values).T @ (phi @ w_i - observed_values)

    # compute the batch gradient descent algorithm
    for i in range(max_iterations):
        # update the next set of weights
        w_next = w_initial - learning_rate * (phi.T @ phi @ w_initial - phi.T @ observed_values)

        # if the error is below our threshold, we are finished!
        if calculate_error(w_initial) - calculate_error(w_next) < min_error:
            break

        # otherwise, get ready to update the weights again
        w_initial = w_next

    # return the final set of weights, and the error
    return [w_next.tolist(), calculate_error(w_next).item()]


def main(name, port, number_of_params, max_iterations, min_error, learning_rate, num_tasks):
    m = vine.Manager(name=name, port=port)

    # enable peer transfers to speed up Library environment delivery
    m.enable_peer_transfers()

    # create Library Task from batch_gradient_descent function, and call it gradient_descent_library
    t = m.create_library_from_functions("gradient_descent_library", batch_gradient_descent)

    # specify resources used by Library Task
    t.set_cores(1)
    t.set_disk(2000)
    t.set_memory(2000)

    # install the Library Task on all workers that will connected to the manager
    m.install_library(t)

    # create our data by sampling 1000 points off a sin curve and adding noise
    # can change the function to perform a regression of different functions,
    # or even input other kinds of data
    x_data = np.linspace(0, 1, 100)
    t_data = np.sin(x_data * 2 * np.pi) + np.random.normal(loc=0, scale=0.1, size=x_data.shape)

    # split data into training and test data
    data = np.column_stack((x_data, t_data))
    train_data = []
    test_data = []
    for i in range(len(data)):
        # place 30% of the data into testing data, and the rest into training data
        if random.randint(1, 10) <= 3:
            test_data.append(list(data[i]))
        else:
            train_data.append(list(data[i]))

    # Create FunctionCall Tasks to run the gradient descent operations
    for i in range(num_tasks):
        # the name of the function to be called is batch_gradient_descent, and
        # the name of the Library that it lives in is gradient_descent_library
        # arguments are train_data, test_data, and iterations control
        t = vine.FunctionCall(
                "batch_gradient_descent",
                "gradient_descent_library",
                train_data, test_data,
                number_of_params, max_iterations, min_error, learning_rate)

        # specify resources used by FunctionCall
        t.set_cores(1)
        t.set_disk(1500)
        t.set_memory(1500)
        m.submit(t)

    # keep track of the best set of weights and the lowest error
    best_weights = []
    best_error = float('inf')

    print(f"TaskVine listening for workers on port {m.port}")

    print("waiting for tasks to complete...")
    while not m.empty():
        t = m.wait(5)
        if t:
            try:
                weights, error = json.loads(t.output)["Result"]
                if error < best_error:
                    best_weights = weights
                    best_error = error
            except Exception:
                print(f"Error reading result of task {t.task_id}")
    print(f"The best weights are: {best_weights}")
    print(f"With an RMS error of {best_error}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="vine_example_gradient_descent.py",
                                     description="""This shows an example of using Library Tasks and FunctionCall Tasks.
Gradient descent is an algorithm used to optimize the weights of machine learning models and regressions.""")
    parser.add_argument('--name', nargs='?', type=str, help='name to assign to the manager.', default=f'vine-bgd-{getpass.getuser()}',)
    parser.add_argument('--port', nargs='?', type=int, help='port for the manager to listen for connections. If 0, pick any available.', default=9123,)
    parser.add_argument('--params', nargs='?', type=int, help='the number of parameters/basis functions in the model', default=100)
    parser.add_argument('--iterations', nargs='?', type=int, help='maximum number of iterations of weight updates to perform', default=100000000)
    parser.add_argument('--error', nargs='?', type=float, help='stop when the fit error is less than this value', default=1e-02)
    parser.add_argument('--rate', nargs='?', type=float, help='how much to update the weights each iteration', default=0.0005)
    parser.add_argument('--tasks', nargs='?', type=int, help='number of tasks to run', default=20)
    args = parser.parse_args()

    main(args.name, args.port, args.params, args.iterations, args.error, args.rate, args.tasks)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
