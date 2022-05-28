#!/usr/bin/env python

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Author: Guanchao "Samuel" Huang
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# Packages and Definitions
import re
import sys
import json
import pandas as pd

from matplotlib import pyplot as plt
import bokeh
from bokeh.layouts import layout
from bokeh.models import Div, RangeSlider, Spinner
from bokeh.plotting import figure, show
palette = bokeh.palettes.all_palettes["Viridis"]


# Functions
def usage(status=0):
    print("work_queue_gantt")
    sys.exit(status)


def get_info(line):
    return json.loads(line[line.rfind('{'):])


def parse_tasks(logfile):
    """
    Parse the logfile as a table.
    | taskid | category | wait_time | start_time | end_time | info |
    """
    
    tasks = pd.DataFrame(columns=[
        "taskid", "category", "wait_time", "start_time", "end_time", "info"
    ])
    tasks.set_index("taskid")
    # TODO: switch from nested json to flatten table. 
    with open(logfile, 'r') as log:
        p_taskid    = re.compile(r"^[^#].*TASK (\d+) (WAITING|RUNNING|DONE) ")
        p_info      = re.compile(r" (\{.*\})?$")
        p_category  = re.compile(r"WAITING (\w+)")
        p_time      = re.compile(r"^\d+")
        min_time    = 0

        for line in log:
            m = p_taskid.findall(line)
            if not m:
                continue
            m, = m
            taskid  = int(m[0])
            status  = m[1]
            time    = int(p_time.findall(line)[0][:10])

            if status == "WAITING":
                if not min_time:
                    min_time = time
                tasks.loc[taskid] = {
                    "taskid"        : taskid,
                    "category"      : p_category.findall(line)[0],
                    "wait_time"     : time - min_time,
                    "start_time"    : -1,
                    "end_time"      : -1,
                    "info"          : ""
                }
            elif status == "RUNNING":
                tasks.loc[taskid, "start_time"] = time - min_time
            elif status == "DONE":
                tasks.loc[taskid, "end_time"]   = time - min_time
                tasks.loc[taskid, "info"]       = p_info.findall(line)
   
    print(tasks) 
    return tasks


def get_times(tasks): 
    taskids     = list(tasks.keys())
    wait_times  = [tasks[taskid]["wait_time"]   for taskid in taskids]
    start_times = [tasks[taskid]["start_time"]  for taskid in taskids]
    end_times   = [tasks[taskid]["end_time"]    for taskid in taskids]
    return taskids, wait_times, start_times, end_times    


def plot(tasks, outfile):
    taskids, start_times, end_times  = get_times(tasks)
    durations = [end_times[i] - start_times[i] for i in range(len(tasks))]
    fig, ax = plt.subplots()
    ax.barh(taskids, durations, left=start_times)
    plt.savefig(outfile, format='svg')
    plt.show()
    

def plot_interactive(tasks):
    # taskids, wait_times, start_times, end_times = get_times(tasks)
    # print(start_times)
    # num_tasks = len(taskids)
    
    categories  = tasks["category"].unique() 
    colors      = palette[len(categories)]
    p = figure(title="Tasks Lifetime",
               x_axis_label="time", y_axis_label="task id",
               sizing_mode="stretch_width")
    tasks = tasks.sort_values(by="start_time")
    offset = 0
    for i, category in enumerate(categories):
        # TODO write this as a iterator
        # Plot the bars based on the time they started
        tasks_cat = tasks[tasks["category"] == category]
        p.hbar(y=range(offset, offset + len(tasks_cat)), left=tasks_cat["start_time"], right=tasks_cat["end_time"], color=colors[i])
        offset += len(tasks_cat)
    show(p)


# Main Executions
def main():
    logfile = ""
    outfile = ""
    interactive = False

    arguments = sys.argv[1:]
    while arguments:
        argument = arguments.pop(0);
        if argument[0] == '-':
            if argument == '-h':
                usage(0)
            elif argument == '-i':
                interactive = True
            elif argument == '-o':
                outfile = arguments.pop(0)
            else:
                usage(1)
        logfile = argument

    if not logfile:
        print("Please specify path to the transactions log file.")
        usage(1)
    outfile = logfile + ".svg" if not outfile else outfile
    tasks = parse_tasks(logfile)
    if interactive:
        plot_interactive(tasks)
    else:
        plot(tasks, outfile)
    
if __name__ == '__main__':
    main()

