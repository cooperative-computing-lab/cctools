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
# manipulated ;ater.

# A PythonTask object is created as `p_task = PyTask.PyTask(func, args)` where
# `func` is the name of the function and args are the arguments needed to
# execute the function. PythonTask can be submitted to a queue as regular Work
# Queue functions, such as `q.submit(p_task)`.
#
# When a has completed, the resulting python value can be retrieved by calling
# the output method, such as: `x  = t.output` where t is the task retuned by
# `t = q.wait()`.


import work_queue as wq

def divide(dividend, divisor):
    import math
    return dividend/math.sqrt(divisor)

def main():
    q = wq.WorkQueue(9123)
    for i in range(1, 16):
        p_task = wq.PythonTask(divide, 1, i**2)
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
