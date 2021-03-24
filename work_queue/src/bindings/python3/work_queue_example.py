#!/usr/bin/env python

# Copyright (c) 2021- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program is a simple example of how to use Work Queue.
# It estimates the value of pi by estimating the ratio of the areas of a circle
# inscribed in a square.
# It accepts two parameters: number of tasks and number of random points to generate per tasks.
# Each task generates its own estimate independently, which then gets combined
# with other tasks as they finish.

import work_queue as wq
import sys
import os


def show_help():
    print("""Estimate pi using random points.

    Usage: work_queue_basic_example.py NUM_TASKS NUM_POINTS
    where:
       NUM_TASKS   Number of tasks to generate.
       NUM_POINTS  Number of random points to generate per task.

Each task generates its own pi etimate by generating NUM_POINTS random points.
These estimates are combined among tasks as tasks finish.""")


def read_arguments():
    try:
        (num_of_tasks, points_per_task) = sys.argv[1:]
    except ValueError:
        show_help()
        print("Error: Incorrect number of arguments")
        exit(1)

    try:
        num_of_tasks = int(num_of_tasks)
        points_per_task = int(points_per_task)
    except ValueError:
        print("Error: Arguments are not integers")
    return (num_of_tasks, points_per_task)


def read_results_file(task):
    result_file = "task_{}.out".format(task.tag)
    try:
        with open(result_file) as f:
            (points_in_square, points_in_circle, pi_estimate) = (float(x) for x in f.readline().split())
            return (points_in_square, points_in_circle)
    except Exception as e:
        print("Error reading file: {}: {}".format(result_file, e))
        return (0, 0)

def cleanup_task(task):
    result_file = "task_{}.out".format(task.tag)
    error_log = "task_{}.err".format(task.tag)

    for filename in [result_file, error_log]:
        try:
            os.remove(filename)
        except Exception as e:
            print("Could not remove task file: {}".format(filename))

def print_error_log(task_tag):
    error_log = "task_{}.err".format(task_tag)
    try:
        with open(error_log) as f:
            print(f.read())
    except IOError:
        pass


(num_of_tasks, points_per_task) = read_arguments()


q = wq.WorkQueue(9123)

for task_tag in range(num_of_tasks):
    output_name = "task_{}.out".format(task_tag)
    error_name  = "task_{}.err".format(task_tag)

    t = wq.Task("./area_circ_sq.py {num_points} {out} 2> {err}".format(num_points=points_per_task, out=output_name, err=error_name))

    t.specify_tag("{}".format(task_tag))

    t.specify_input_file("area_circ_sq.py", cache=True)
    t.specify_output_file(output_name, cache=False)
    t.specify_output_file(error_name, cache=False)

    t.specify_cores(1)
    t.specify_memory(100)
    t.specify_disk(200)

    print("Submitting task {}".format(task_tag))
    q.submit(t)


total_points_in_square = 0
total_points_in_circle = 0

# wait for all tasks to complete
while not q.empty():
    t = q.wait(5)
    if t:
        print("Task {id} has returned.".format(id=t.id))
        if t.result == wq.WORK_QUEUE_RESULT_SUCCESS and t.return_status == 0:
                points_in_square, points_in_circle = read_results_file(t)
                total_points_in_square += points_in_square
                total_points_in_circle += points_in_circle
                print("New estimate: {}".format(float(4 * total_points_in_circle) / total_points_in_square))
        else:
            print("    It could not be completed successfully. Result: {}. Exit code: {}".format(t.result, t.return_status))
            print_error_log(task_tag)
        cleanup_task(t)


print("All task have finished.")

