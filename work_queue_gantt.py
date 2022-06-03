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
        "taskid",
        "category",
        "wait_time",
        "start_time",
        "end_time",
        "info"
    ])
    tasks.set_index("taskid")

    with open(logfile, 'r') as log:
        p_task = re.compile(r"(?P<time>\d+) \s+ "
                            r"(?P<pid>\d+) \s+ TASK \s+ "
                            r"(?P<taskid>\d+) \s+ "
                            r"(?P<state>\S+) \s+ "
                            r"(?P<state_args>.+)", re.X)
        min_time = 0

        for line in log:
            m = p_task.match(line)
            if not m:
                continue
            time   = int(m.group("time"))
            taskid = m.group("taskid")
            state  = m.group("state")

            if state == "WAITING":
                if not min_time:
                    min_time = time
                tasks.loc[taskid] = {
                    "taskid"     : taskid,
                    "category"   : m.group("state_args").split()[0],
                    "wait_time"  : time - min_time,
                    "start_time" : -1,
                    "end_time"   : -1,
                    "info"       : ""
                }
            elif state == "RUNNING":
                tasks.loc[taskid, "start_time"] = time - min_time
            elif state == "DONE":
                tasks.loc[taskid, "end_time"]   = time - min_time
                tasks.loc[taskid, "info"]       = m.group("state_args")

    tasks["order"] = tasks["start_time"].rank(method="first")
    return tasks


def get_times(tasks):
    taskids     = list(tasks.keys())
    wait_times  = [tasks[taskid]["wait_time"]  for taskid in taskids]
    start_times = [tasks[taskid]["start_time"] for taskid in taskids]
    end_times   = [tasks[taskid]["end_time"]   for taskid in taskids]
    return taskids, wait_times, start_times, end_times


def plot(tasks):
    categories = tasks["category"].unique()
    colors     = palette[len(categories)]
    p = figure(
        title="Tasks Lifetime",
        x_axis_label="time", y_axis_label="task id",
        sizing_mode="stretch_width"
    )
    tasks = tasks.sort_values(by="start_time")

    for i, category in enumerate(categories):
        # TODO write this as a iterator
        # Plot the bars based on the time they started
        tasks_cat = tasks[tasks["category"] == category]
        p.hbar(
            y=tasks_cat["order"],
            left=tasks_cat["start_time"], right=tasks_cat["end_time"],
            color=colors[i]
        )
    show(p)


# Main Executions
def main():
    logfile = ""

    arguments = sys.argv[1:]
    while arguments:
        argument = arguments.pop(0)
        if argument[0] == '-':
            if argument == '-h':
                usage(0)
            else:
                usage(1)
        logfile = argument

    if not logfile:
        print("No log file specified")
        usage(1)
    tasks = parse_tasks(logfile)
    plot(tasks)


if __name__ == '__main__':
    main()
