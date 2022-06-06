#!/usr/bin/env python

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Author: Guanchao "Samuel" Huang
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# Packages and Definitions
import re
import sys
import json

import bokeh
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
    """

    tasks = {}
    with open(logfile, 'r') as log:
        p_task   = re.compile(r"(?P<time>\d+) \s+ "
                              r"(?P<pid>\d+) \s+ TASK \s+ "
                              r"(?P<taskid>\d+) \s+ "
                              r"(?P<state>\S+) \s+ "
                              r"(?P<state_args>.+)", re.X)
        p_worker = re.compile(r".*RUNNING (?P<worker>\S+)")
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
                tasks[taskid]["worker"] = p_worker.match(line).group("worker")
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

    tasks["order"] = [sorted(tasks["start_time"]).index(i)
                      for i in tasks["start_time"]]

    workers = set(tasks["worker"])
    sorted_times = {worker: sorted([tasks["start_time"][i]
                                    for i in range(len(taskids)) if tasks["worker"][i] == worker])
                    for worker in workers}
    tasks["order_per_worker"] = [
        sorted_times[worker].index(time)
        for worker, time in zip(tasks["worker"], tasks["start_time"])
    ]
    return tasks


def plot(tasks, outfile):
    bokeh.io.output_file(outfile)

    categories = set(tasks["category"])
    workers    = set(tasks["worker"])
    colors     = palette[len(categories)]
    source     = ColumnDataSource(tasks)

    p = figure(
        title="Tasks Lifetime",
        x_axis_label="time", y_axis_label="Order of Task Start Time",
        sizing_mode="stretch_width",
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
        ("taskid", "@taskid"),
        ("category", "@category"),
        ("wait_time", "@wait_time"),
        ("start_time", "@start_time"),
        ("end_time", "@end_time"),
        ("info", "@info")
    ]
    p.add_tools(HoverTool(
        tooltips=TOOLTIPS,
    ))

    show(p)


def plot_worker(tasks, outfile):
    bokeh.io.output_file(outfile)

    categories = set(tasks["category"])
    workers    = set(tasks["worker"])
    colors     = palette[len(categories)]
    source     = ColumnDataSource(tasks)

    figures = {
        worker: figure(
            title=f"Tasks Lifetime for worker {worker}",
            x_axis_label="time",
            y_axis_label="Order of Task Start Time per Worker",
            sizing_mode="stretch_width",
        ) for worker in workers
    }

    TOOLTIPS = [
        ("taskid", "@taskid"),
        ("category", "@category"),
        ("wait_time", "@wait_time"),
        ("start_time", "@start_time"),
        ("end_time", "@end_time"),
        ("info", "@info")
    ]

    for category, color in zip(categories, colors):
        for worker in workers:
            p = figures[worker]
            view = CDSView(
                source=source,
                filters=[
                    GroupFilter(column_name="category", group=category),
                    GroupFilter(column_name="worker",   group=worker)
                ]
            )
            p.hbar(
                y="order_per_worker",
                left="start_time", right="end_time",
                source=source, view=view,
                color=color
            )

            p.add_tools(HoverTool(
                tooltips=TOOLTIPS,
            ))

    show(bokeh.layouts.column(*figures.values()))


# Main Executions
def main():
    logfile      = ""
    outfile      = ""
    worker_view  = False

    arguments = sys.argv[1:]
    while arguments:
        argument = arguments.pop(0)
        if argument[0] == '-':
            if argument == "-h":
                usage(0)
            if argument == "-o":
                outfile = arguments.pop(0)
            elif argument == "-w":
                worker_view = True
            else:
                usage(1)
        logfile = argument

    if not logfile:
        print("No log file specified")
        usage(1)
    if not outfile:
        outfile = logfile + ".html"

    tasks = parse_tasks(logfile)
    if worker_view:
        plot_worker(tasks, outfile)
    else:
        plot(tasks, outfile)


if __name__ == '__main__':
    main()
