#!/usr/bin/env python

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows usage of TaskVine's future Executor
# It performs matrix multiplications on multiple matricies
# creating a tree of matrix multiplication operations.

# FutureTasks are created by calling executor.submit(func, *args, **kwargs)
# The task's result can then be retrieved by calling f.result() from 
# the returned future. futures themselves can be passed as arguments to other 
# future tasks. 

# Future tasks can also be created by calling executor.task(func, *args, **kwargs)
# Here, modifications can me made to the task such as setting resource allocations
# and adding input and output files.

import ndcctools.taskvine as vine
import cloudpickle
import poncho
import random

def generate_random_matrix(n):
    matrix = []
    for i in range(n):
        row = []
        for j in range(n):
            row.append(random.uniform(.001, .002))
        matrix.append(row)
    return matrix

def load_matricies(levels):
    matricies = []
    for x in range(2**(levels+1)):
        with open('matricies/matrix-{}'.format(x), 'rb') as f:
            matrix = cloudpickle.load(f)
            matricies.append(matrix)
    return matricies

def write_matricies(levels, n):
    for x in range(2**(levels+1)):
        matrix = generateRandomMatrix(n)
        with open('matricies/matrix-{}'.format(x), 'wb') as f:
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

futures = []

levels = 3
count = 0
n = 400
matricies = [generate_random_matrix(n) for x in range(2**levels)]
print('Generated Matricies...')

executor = vine.Executor(manager_name='vine_matrix_example', batch_type='condor')
executor.manager.enable_peer_transfers()
executor.set("memory", 8000)
executor.set("disk", 8000)
env_spec = {"conda": {"channels": ["conda-forge"],"packages": ["python","pip","conda","conda-pack","cloudpickle"]}}
env_tarball = poncho.package_create.dict_to_env(env_spec, cache=True)
env_file = executor.manager.declare_poncho(env_tarball, cache=True)

for level in range(levels):
    for x in range(2**(levels - level - 1)):
        if level == 0:
            t = executor.task(matrix_multiply, matricies[x*2], matricies[x*2+1])
            t.set_cores(1)
            t.add_environment(env_file)
            # the future can be created ommiting the task specifications above like so: f = executor.submit(matrix_multiply, matricies[x*2], matricies[x*2+1])
            f = executor.submit(t)
            futures.append(f)

        else:
            t = executor.task(matrix_multiply, futures[count], futures[count + 1])
            t.set_cores(1)
            t.add_environment(env_file)
            f = executor.submit(t)
            futures.append(f)
            count += 2


print("waiting for result...")
f = futures[-1]
print("RESULT:", f.result())






