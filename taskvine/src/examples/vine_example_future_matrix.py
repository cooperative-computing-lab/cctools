#!/usr/bin/env python

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows usage of TaskVine's future Executor
# It performs matrix multiplications on multiple matrices
# creating a tree of matrix multiplication operations.

# FutureTasks are created by calling executor.submit(func, *args, **kwargs)
# The task's result can then be retrieved by calling f.result() from 
# the returned future. Futures themselves can be passed as arguments to other 
# future tasks. 

# Future tasks can also be created by calling executor.task(func, *args, **kwargs)
# Here, modifications can me made to the task such as setting resource allocations
# and adding input and output files.

import ndcctools.taskvine as vine
import cloudpickle
import poncho
import random

futures = []
count = 0

def generate_random_matrix(n):
    matrix = []
    for i in range(n):
        row = []
        for j in range(n):
            row.append(random.uniform(.001, .002))
        matrix.append(row)
    return matrix

def load_matrices(levels):
    matrices = []
    for x in range(2**(levels+1)):
        with open('matrices/matrix-{}'.format(x), 'rb') as f:
            matrix = cloudpickle.load(f)
            matrices.append(matrix)
    return matrices

def write_matrices(levels, n):
    for x in range(2**(levels+1)):
        matrix = generateRandomMatrix(n)
        with open('matrices/matrix-{}'.format(x), 'wb') as f:
            cloudpickle.dump(matrix, f)

def matrix_multiply(a, b):
    result = []
    for i in range(len(a)):
        row = []
        for j in range(len(b[0])):
            row.append(0)
        result.append(row)


    for i in range(len(a)):
        for j in range(len(b[0])):
            for k in range(len(b)):
                result[i][j] += a[i][k] * b[k][j]
    return result

levels = 5
n = 400
matrices = [generate_random_matrix(n) for x in range(2**levels)]
print('Generated Matrices...')

# Here we provide sprecifications for the workers
executor_opts = {"memory": 8000, "disk":8000, "cores":4, "min-workers":5}
executor = vine.Executor(manager_name='vine_matrix_example', batch_type='condor', opts=executor_opts)
executor.manager.enable_peer_transfers()

# Genereate an environment that can be attached to each task. The task will run inside the provided environment.
env_spec = {"conda": {"channels": ["conda-forge"],"packages": ["python","pip","conda","conda-pack","cloudpickle"]}}
env_tarball = poncho.package_create.dict_to_env(env_spec, cache=True)
env_file = executor.manager.declare_poncho(env_tarball, cache=True)

for level in range(levels):
    for x in range(2**(levels - level - 1)):
        if level == 0:
            t = executor.task(matrix_multiply, matrices[x*2], matrices[x*2+1])
            t.set_cores(1)
            t.add_environment(env_file)
            # the future can be created ommiting the task specifications above like so: 
            # f = executor.submit(matrix_multiply, matrices[x*2], matrices[x*2+1])
            f = executor.submit(t)
            futures.append(f)

        else:
            t = executor.task(matrix_multiply, futures[count], futures[count + 1])
            t.set_cores(1)
            t.add_environment(env_file)
            f = executor.submit(t)
            futures.append(f)
            count += 2


f = futures[-1]
print("waiting for result...")
print("RESULT:", f.result())






# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
