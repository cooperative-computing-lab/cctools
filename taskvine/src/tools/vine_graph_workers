#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# Packages and Definitions
import re
import os
import sys

# Bokeh is an interactive data visualizer, it generates portable static HTML
# file. Steamlit accepts Bokeh plot as its component, this information might
# be useful for future developers to implement an interactive dashboard for
# cctools.

try:
    import bokeh
    from bokeh.models import ColumnDataSource, CDSView, HoverTool, GroupFilter, Range1d
    from bokeh.plotting import figure, show
    from bokeh.palettes import Category10
except ImportError:
    print("Bokeh is not installed. Please run the following command to install Bokeh:")
    print("conda install bokeh")
    sys.exit(1)

palette = Category10
# Tooltips used in the interactive hover tool.
TOOLTIPS = [
    ("taskid", "@taskid"),
    ("category", "@category"),
    ("wait_time", "@wait_time"),
    ("start_time", "@start_time"),
    ("end_time", "@end_time"),
    ("info", "@info")
]


# Functions
def usage(status=0, prog=os.path.basename(__file__)):
    print(f"Usage: {prog} [-gtw] [-o outfile] logfile")
    print("Utilize the bokeh package to analyze the taskvine transaction log file interactively.")
    print("""
  -g  Display lifetime of all tasks. (default)
  -t  Display lifetime of tasks per worker.
  -w  Display lifetiem of workers.
  -o  Specify the output file name, default name is <logfile>.html.
""")
    sys.exit(status)


def flatten(data: dict, identifier: str):
    """
    Given a dictionary of data, in which the keys are workers or tasks and the
    values are the corresponding properties, flatten the dictionary, so that
    the keys are the names of the properties and the values are lists of the
    corresponding property values.
    Bokeh ColumnDataSource accepts a dictionary of lists as its data.
    """
    entry_ids = list(data.keys())
    return {
        identifier: entry_ids,
        **{
            key: [data[entry_id][key] for entry_id in entry_ids]
            for key in data[entry_ids[0]].keys()
        }
    }


def parse_tasks(logfile):
    tasks = {}
    with open(logfile, 'r') as log:
        p_task   = re.compile(r"(?P<time>\d+) \s+"
                              r"(?P<pid>\d+) \s+ TASK \s+"
                              r"(?P<taskid>\d+) \s+"
                              r"(?P<state>\S+) \s+"
                              r"(?P<state_args>.+)", re.X)
        p_worker = re.compile(r".*RUNNING (?P<worker>\S+)")
        p_transfer = re.compile(r"(?P<time>\d+) \s+ .* TRANSFER \s+"
                                r"(?P<state>\S+) \s+"
                                r"(?P<taskid>\d+)", re.X)

        min_time = 0

        for line in log:

            # Match for TASK entries
            m = p_task.match(line)
            if m:
                time = int(m.group("time")) / 1e6
                taskid = int(m.group("taskid"))
                state = m.group("state")

                if state == "WAITING":
                    if not min_time:
                        min_time = time
                    tasks[taskid] = {
                        "taskid": taskid,
                        "category": m.group("state_args").split()[0],
                        "wait_time": time - min_time,
                        "start_time": -1,
                        "end_time": -1,
                        "input_time": -1,
                        "output_time": -1,
                        "info": ""
                    }
                elif state == "RUNNING":
                    tasks[taskid]["start_time"] = time - min_time
                    tasks[taskid]["worker"]     = \
                        p_worker.match(line).group("worker")
                elif state == "DONE":
                    tasks[taskid]["end_time"] = time - min_time
                    tasks[taskid]["info"] = m.group("state_args")

                continue

            # Match for TRANSFER entries
            m = p_transfer.match(line)
            if m:
                time   = int(m.group("time"))  / 1e6
                taskid = int(m.group("taskid"))
                state  = m.group("state")
                if state == "INPUT" and tasks[taskid]["input_time"] == -1:
                    tasks[taskid]["input_time"] = time - min_time
                if state == "OUTPUT" and tasks[taskid]["output_time"] == -1:
                    tasks[taskid]["output_time"] = time - min_time

                continue

    tasks = flatten(tasks, "taskid")
    # Sort the tasks by input time.
    tasks["order"] = [sorted(tasks["input_time"]).index(i)
                      for i in tasks["input_time"]]

    return tasks


def parse_workers(logfile):
    # addrport is used to identify a worker.
    p_workers = re.compile(r"(?P<time>\d+) \s+"
                           r"(?P<pid>\d+) \s+ WORKER \s+"
                           r"(?P<worker_id>\S+) \s+"
                           r"(?P<addr>\S+) \s+"
                           r"(?P<state>CONNECTION|DISCONNECTION)", re.X)
    p_running = re.compile(r"(?P<time>\d+) .* TASK .* RUNNING \s+"
                           r"(?P<addr>\S+)", re.X)

    workers  = {}
    min_time = 0
    with open(logfile, 'r') as log:
        for line in log:

            # Match for WORKER entries
            m = p_workers.match(line)
            if m:
                time   = int(m.group("time")) / 1e6
                worker = m.group("addr")
                state  = m.group("state")
                if not min_time:
                    min_time = time
                time -= min_time

                if state == "CONNECTION":
                    workers[worker] = {
                        "arrive": time, "start": -1, "leave": -1
                    }
                elif state == "DISCONNECTION":
                    if worker in workers:
                        workers[worker]["leave"] = time
                continue

            # Match for RUNNING entries
            m = p_running.match(line)
            if m:
                worker = m.group("addr")
                if worker in workers and workers[worker]["start"] == -1:
                    time = int(m.group("time")) / 1e6 - min_time
                    workers[worker]["start"] = time
                continue

    workers = flatten(workers, "addr")
    # Sort the workers by arrival time.
    workers["order"] = [sorted(workers["arrive"]).index(t)
                        for t in workers["arrive"]]

    # For the workers haven't disconnected, use the maximum leave time.
    max_time = max(workers["leave"])
    for i, t in enumerate(workers["leave"]):
        if t == -1:
            workers["leave"][i] = max_time

    # For the workers haven't started, use their leave time.
    for i, t in enumerate(workers["start"]):
        if t == -1:
            workers["start"][i] = workers["leave"][i]

    return workers


def get_binding(tasks):
    """
    Returns the maximum slots existed in each worker and the binding of tasks
    with slots of workers. Slots binding is calculated for the purpose of
    visualization, which is related to, but not exactly the same as CPU cores
    in the workers.
    """

    workers = set(tasks["worker"])
    slots   = {worker: [None] for worker in workers}
    binding = [None] * len(tasks["taskid"])

    # For each task, find an available slot in the worker, so that the tasks
    # don't overlap in the visualization.
    for i in range(len(tasks["taskid"])):
        index      = tasks["order"].index(i)
        worker     = tasks["worker"][index]
        slot       = slots[worker]
        input_time = tasks["input_time"][index]
        end_time   = tasks["end_time"][index]

        bound = False
        for j in range(len(slot)):
            if not slot[j] or slot[j] <= input_time:
                binding[index] = j
                slot[j]        = end_time
                bound          = True
                break
        if not bound:
            binding[index] = len(slot)
            slot.append(end_time)

    return {worker: len(slots[worker]) for worker in workers}, binding


def plot_gantt(tasks, outfile):
    bokeh.io.output_file(outfile)

    categories = set(tasks["category"])
    colors     = palette[len(categories)]
    source     = ColumnDataSource(tasks)

    p = figure(
        title="Tasks Lifetime",
        x_axis_label="time", y_axis_label="Order of Task Start Time",
        sizing_mode="stretch_both",
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

    p.add_tools(HoverTool(tooltips=TOOLTIPS))
    show(p)


def plot_task(tasks, outfile):
    bokeh.io.output_file(outfile)

    slots, binding   = get_binding(tasks)
    tasks["binding"] = binding

    categories = set(tasks["category"])
    workers    = set(tasks["worker"])
    colors     = palette[len(categories)]
    source     = ColumnDataSource(tasks)
    min_time   = min(tasks["input_time"])
    max_time   = max(tasks["end_time"])

    figures = {
        worker: figure(
            title=f"Tasks Lifetime for Worker {worker}",
            x_axis_label="time",
            y_axis_label="Worker Slots",
            x_range=Range1d(min_time, max_time),
        ) for worker in workers
    }

    for category, color in zip(categories, colors):
        for worker in workers:
            p = figures[worker]
            view = CDSView(
                source=source,
                filters=[
                    GroupFilter(column_name="category", group=category),
                    GroupFilter(column_name="worker",   group=worker),
                ]
            )

            for left, right, line, hatch in zip(
                    ["input_time", "start_time",  "output_time"],
                    ["start_time", "output_time", "end_time"],
                    ["black",      "black",       "black"],
                    [None,         '*',           None]
            ):
                p.hbar(
                    y="binding",
                    left=left, right=right,
                    source=source, view=view,
                    fill_color=color, line_color=line, hatch_pattern=hatch,
                    legend_label=category
                )

            p.yaxis.ticker = list(range(slots[worker]))
            p.ygrid.grid_line_alpha = 0.5
            p.ygrid.grid_line_dash = [6, 4]
            p.legend.click_policy = "hide"
            p.add_tools(HoverTool(tooltips=TOOLTIPS))

    show(bokeh.layouts.column(*figures.values(), sizing_mode="stretch_width"))


def plot_worker(workers, outfile):
    # Connected time, Disconnect time, first task start time
    bokeh.io.output_file(outfile)
    source = ColumnDataSource(workers)
    tooltips = [
        ("Worker",                "@addr"),
        ("Connected Time",        "@arrive"),
        ("Disconnected Time",     "@leave"),
        ("First Task Start Time", "@start"),
    ]

    p = figure(
        title="Worker Lifetime",
        x_axis_label="time", y_axis_label="Order of Worker Connect Time",
        sizing_mode="stretch_both",
    )

    for left, right, label, color in zip(
            ["arrive", "start"],
            ["start",  "leave"],
            ["idling", "working"], palette[3]
    ):
        # At least three colors are needed for palette.
        p.hbar(
            y="order", left=left, right=right, source=source,
            color=color, line_color="black",
            legend_label=label
        )

    p.legend.click_policy = "hide"
    p.add_tools(HoverTool(tooltips=tooltips))
    show(p)


# Main Executions
def main():
    logfile = ""
    outfile = ""
    worker_view = False
    task_view   = False
    gantt_view  = True

    arguments = sys.argv[1:]
    while arguments:
        argument = arguments.pop(0)
        if argument[0] == '-':
            if argument == '-t':
                task_view = True
            elif argument == "-g":
                gantt_view = True
            elif argument == "-h":
                usage(0)
            elif argument == "-o":
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

    if task_view:
        tasks = parse_tasks(logfile)
        plot_task(tasks, outfile)
    elif worker_view:
        workers = parse_workers(logfile)
        plot_worker(workers, outfile)
    else:
        tasks = parse_tasks(logfile)
        plot_gantt(tasks, outfile)


if __name__ == '__main__':
    main()
