#! /usr/bin/env python

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# Plot the time spent matching tasks to workers through the information in the performance log

import sys
import matplotlib.pyplot as plt
import os

def read_fields(f, lines_patience = 10):
    for line in f:
        if line[0] != '#':
            lines_patience = lines_patience - 1
        else:
            return line.strip('#\n\r\t ').split()
        if lines_patience < 1:
            break
    sys.stderr.write("Could not find fields. This is probably not a performance log...\n")
    sys.exit(1)


def plot_performance(performance_filename):
    """ 
    This function is used to plot the accumulated schedulling time 
    for all tasks and the separate schedulling time for each task 
    """
    
    print(f"Plotting the performance of task...")
    f = open(performance_filename)
    fields = read_fields(f)
    f.seek(0)

    dispatch_offset = fields.index("tasks_dispatched")
    scheduling_offset = fields.index("time_scheduling")

    num_timestamps = 0
    tasks_dispatched = []
    time_scheduling = []

    for line in f:
        if line[0] == "#":
            continue
        items = line.strip("\n\r\t ").split()
        num_timestamps += 1
        tasks_dispatched.append(int(items[dispatch_offset]))
        time_scheduling.append((items[scheduling_offset]))

    # Get the accmulated time as the number of tasks grow
    accumulated_tasks = []
    accumulated_time = []
    last_task_id = -1
    for i, task_id in enumerate(tasks_dispatched):
        if int(time_scheduling[i]) == 0:
            continue
        if task_id > last_task_id:
            accumulated_tasks.append(task_id)
            accumulated_time.append(int(time_scheduling[i]))
        last_task_id = task_id

    # Draw the first figure: Accumulated Time with Tasks Scheduled
    plt.subplot(1, 2, 1)
    plt.plot(accumulated_tasks, accumulated_time)
    plt.ylim(ymin=0)
    plt.xlabel('Accumulated Tasks')
    plt.ylabel('Accumulated Time (us)')

    # Generate the scheduled time for each task
    scheduled_task = []
    scheduled_time = []
    
    # There are a lot of concurrent tasks in a period, split the execution time evenly among them
    for idx, tid in enumerate(accumulated_tasks):
        if tid != accumulated_tasks[-1]:
            next_tid = accumulated_tasks[idx + 1]
            task_gap = next_tid - tid

            this_time, next_time = accumulated_time[idx], accumulated_time[idx + 1]
            time_gap = next_time - this_time
            time_for_each_task = time_gap / task_gap

            for tid_ in range(tid, next_tid, 1):
                scheduled_task.append(tid_ + 1)
                scheduled_time.append(time_for_each_task)

    # Draw the second figure: Scheduled Time for each Task Number
    plt.subplot(1, 2, 2)
    plt.plot(scheduled_task, scheduled_time)
    plt.ylim(ymin=0)
    plt.xlabel('Scheduled Task')
    plt.ylabel('Scheduled Time (us)')
    plt.tight_layout()
    
    log_dir = os.path.dirname(performance_filename)
    save_png = os.path.join(log_dir, 'schedulling_performance.png')
    plt.savefig(save_png)
    print("Done, png file has been saved in the same log directory")

    return


if __name__ == "__main__":
    performance_filename = sys.argv[1]
    plot_performance(performance_filename)
    