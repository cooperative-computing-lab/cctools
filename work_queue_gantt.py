#!/usr/bin/env python

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Author: Guanchao "Samuel" Huang
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# Packages and Definitions
import sys
import json
from matplotlib import pyplot as plt
from bokeh.layouts import layout
from bokeh.models import Div, RangeSlider, Spinner
from bokeh.plotting import figure, show



# Functions
def usage(status=0):
    print("work_queue_gantt")
    sys.exit(status)


def get_properties(line):
    return json.loads(line[line.rfind('{'):])


def parse_tasks(logfile):
    """
    Parse the logfile as a dictionary of properties of tasks.
    { name: properties }
    """
    
    tasks = {}
    
    with open(logfile, 'r') as log:
        for line in log:
            if line[0] == '#':
                continue
            
            task_occ = line.find("TASK")
            done_occ = line.find("DONE")
            if task_occ != -1 and done_occ != -1:
                name = line[task_occ:done_occ].split()[1]
                tasks[name] = get_properties(line)
                continue

    return tasks


def get_times(tasks): 
    names = list(tasks.keys())
    start_times = [tasks[name]["start"][0] for name in names]
    min_time = min(start_times)
    start_times = [time - min_time for time in start_times]
    end_times = [tasks[name]["end"][0] - min_time for name in names]
    return names, start_times, end_times    


def plot(tasks, outfile):
    names, start_times, end_times  = get_times(tasks)
    durations = [end_times[i] - start_times[i] for i in range(len(tasks))]
    fig, ax = plt.subplots()
    ax.barh(names, durations, left=start_times)
    plt.savefig(outfile, format='svg')
    plt.show()
    

def plot_interactive(tasks):
    names, start_times, end_times = get_times(tasks)
    p = figure(title="Tasks Lifetime",
               x_axis_label="time", y_axis_label="task id",
               sizing_mode="stretch_width")
    p.hbar(y=range(len(names)), left=start_times, right=end_times)
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

