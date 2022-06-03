#!/usr/bin/env python

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Author: Guanchao "Samuel" Huang
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# Packages and Definitions
import re
import sys
import json

import bokeh
from bokeh.layouts import layout
from bokeh.models import ColumnDataSource, CDSView, HoverTool, GroupFilter
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

    tasks = {}
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
                tasks[taskid] = {
                    "taskid"     : taskid,
                    "category"   : m.group("state_args").split()[0],
                    "wait_time"  : time - min_time,
                    "start_time" : -1,
                    "end_time"   : -1,
                    "info"       : ""
                }
            elif state == "RUNNING":
                tasks[taskid]["start_time"] = time - min_time
            elif state == "DONE":
                tasks[taskid]["end_time"] = time - min_time
                tasks[taskid]["info"]     = m.group("state_args")

    taskids = list(tasks.keys())
    tasks = {
        "taskid": taskids,
        **{
            key: [tasks[taskid][key] for taskid in taskids]
            for key in tasks[taskids[0]].keys()
        }
    }

    tasks["order"] = [sorted(tasks["start_time"]).index(i) for i in tasks["start_time"]]
    return tasks


def plot(tasks, outfile):
    # todo outfile
    categories = set(tasks["category"])
    colors     = palette[len(categories)]
    source     = ColumnDataSource(tasks)
    
    p = figure(
        title="Tasks Lifetime",
        x_axis_label="time", y_axis_label="Order of start time",
        sizing_mode="stretch_width"
    )

    for category, color in zip(categories, colors):
        view = CDSView(
            source=source,
            filters=[GroupFilter(column_name="category", group=category)]
        )
        p.hbar(
            y="order",
            left="start_time", right="end_time",
            source=source, view=view,
            color=color
        )

    TOOLTIPS = [
        ("taskid",     "@index"),
        ("category",   "@category"),
        ("wait_time",  "@wait_time"),
        ("start_time", "@start_time"),
        ("end_time",   "@end_time"),
        ("info",       "@info")
    ]
    show(p)


# Main Executions
def main():
    logfile = ""
    outfile = ""

    arguments = sys.argv[1:]
    while arguments:
        argument = arguments.pop(0)
        if argument[0] == '-':
            if argument == "-h":
                usage(0)
            if argument == "-o":
                outfile == arguments.pop(0)
            else:
                usage(1)
        logfile = argument

    if not logfile:
        print("No log file specified")
        usage(1)
    if not outfile:
        outfile = logfile + ".html"

    tasks = parse_tasks(logfile)
    plot(tasks, outfile)


if __name__ == '__main__':
    main()
