#!/usr/bin/env python3

# copyright (C) 2021- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# Example on how to execute python code with a Work Queue task.
# The class PythonTask allows users to execute python functions as Work Queue
# commands. Functions and their arguments are pickled to a file and executed
# utilizing a wrapper script to execut the function. the output of the executed
# function is then written to a file as an output file and read when neccesary
# allowing the user to get the result as a python variable during runtime and
# manipulated later.

# A PythonTask object is created as `p_task = PyTask.PyTask(func, args)` where
# `func` is the name of the function and args are the arguments needed to
# execute the function. PythonTask can be submitted to a queue as regular Work
# Queue functions, such as `q.submit(p_task)`.
#
# When a task has completed, the resulting python value can be retrieved by calling
# the output method, such as: `x  = t.output` where t is the task retuned by
# `t = q.wait()`.
#
# By default, the task will run assuming that the worker is executing inside an
# appropiate  python environment. If this is not the case, an environment file
# can be specified with: `t.specify_environment("env.tar.gz")`, in which
# env.tar.gz is created with the conda-pack module, and has at least a python
# installation, the cloudpickle module, and the conda module.
#
# A minimal conda environment 'my-minimal-env.tar.gz' can be created with:
#
# conda create -y -p my-minimal-env python=3.8 cloudpickle conda
# conda install -y -p my-minimal-env -c conda-forge conda-pack
# conda install -y -p my-minimal-env pip and conda install other modules, etc.
# conda run -p my-minimal-env conda-pack


import work_queue as wq

def divide(dividend, divisor):
    import math
    return dividend/math.sqrt(divisor)

def main():
    q = wq.WorkQueue(9123)
    for i in range(1, 16):
        p_task = wq.PythonTask(divide, 1, i**2)

        # if python environment is missing at worker...
        #p_task.specify_environment("env.tar.gz")

        q.submit(p_task)

    sum = 0
    while not q.empty():
        t = q.wait(5)
        if t:
            x = t.output
            if isinstance(x, wq.PythonTaskNoResult):
                print("Task {} failed and did not generate a result.".format(t.id))
            else:
                sum += x
        print(sum)

if __name__ == '__main__':
    main()
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
