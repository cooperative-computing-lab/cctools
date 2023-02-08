from collections import Counter
import matplotlib.pyplot as plt
import time
import sys
import argparse
import json

worker_info = {}
worker_of_task = {}

manager_start = -1

def read_log(log, print_stats=False):
    fail_count = 0
    filename = log
    lines = open(log, 'r').read().splitlines()
    for line in lines:
        if line.startswith("#"):
            continue

        try:
            (time, managerpid, subject, target, event, arg) = line.split(maxsplit=5)
        except ValueError:
            continue

        time = int(time)/1000000

        if subject == "MANAGER":
            managerpid = target
            if event == "START":
                global manager_start
                manager_start = time
            continue

        if subject == "WORKER":
            worker_id = target
            if worker_id == "(null)":
                continue

            if event == "CONNECTION":
                worker_address = arg.strip()
                if worker_id not in worker_info:
                    worker_info[worker_id] = {"host":worker_address, "connect time":time, "tasks":{}, "cache_updates":[], "first_task":float('inf'), "resources":[]}
            elif event == "DISCONNECTION":
                reason = arg.strip()
                if reason == "FAILURE":
                    fail_count += 1
                if worker_id in worker_info:
                    worker_info[worker_id]["disconnect time"] = time
            elif event == "RESOURCES":
                worker_info[worker_id]["resources"].append(time)
            elif event == "CACHE_UPDATE":
                (filename, sizeinmb, walltime) = arg.split()
                worker_info[worker_id]["cache_updates"].append([time, float(walltime)/1000000, filename])
            elif event == "TRANSFER":
                (direction, filename, sizeinmb, walltime) = arg.split()
                worker_info[worker_id]["tasks"][task_num]["I_transfer"].append([time, walltime])
            continue

        if subject == "TASK":
            taskid = target
            if event == "RUNNING":
                (worker_id, step, resources) = arg.split(maxsplit=2)
                resources = json.loads(resources)
                worker_info[worker_id]["tasks"][taskid] = {"start":time, "stop":-1, "I_transfer":[]}
                worker_of_task[taskid] = worker_id
                if time < worker_info[worker_id]["first_task"]:
                    worker_info[worker_id]["first_task"] = time
            elif event == "WAITING_RETRIEVAL":
                worker_id = arg.strip()
                worker_info[worker_id]["tasks"][taskid]["stop"] = time
            elif event == "DONE":
                (reason, exit_code, limits, measured) = arg.split(maxsplit=3)
                limits = json.loads(limits)
                measured = json.loads(measured)
                if reason == "SUCCESS":
                    worker_id = worker_of_task[taskid]
                    worker_info[worker_id]["tasks"][taskid]["done"] = time
            continue

    if print_stats:
        ####### WORKER STATS ############
        task_count = []
        time_between_tasks = []
        for worker in worker_info:
            task_count.append(len(worker_info[worker]["tasks"]))
            if len(worker_info[worker]["tasks"]) > 1:
                start_stop = []
                for task in worker_info[worker]["tasks"]:
                    start_stop.append(worker_info[worker]["tasks"][task]["start"])
                    start_stop.append(worker_info[worker]["tasks"][task]["stop"])
                ss_count = 0
                for x in start_stop:
                    if ss_count != 0 and ss_count%2 == 0:
                        start = start_stop[ss_count]
                        stop = start_stop[ss_count - 1]
                        if start != -1 and stop != -1:
                            time_between_tasks.append(start - stop)
                    ss_count += 1

        print(len(time_between_tasks))
        counts = dict(Counter(task_count))
        for x in counts:
            print("Number of workers: {}, Tasks completed {}".format(counts[x], x))

        print("average tasks per worker:", sum(task_count)/len(task_count),
                "number of workers failed:", fail_count,
                "average_time_between_tasks", sum(time_between_tasks)/len(time_between_tasks))
        ###############################

def plot_resource_updates(manager_ref):
    xs = []
    ys = []
    y = 0
    for worker in worker_info:
        y += 1
        for resource_update in worker_info[worker]["resources"]:
            if manager_ref:
                x = resource_update - manager_start
            else:
                x = resource_update - worker_info[worker]["first_task"]
            if x > 0:
                xs.append(x)
                ys.append(y)
    plt.plot(xs , ys, 'm+', label='Resource Updates')

def plot_cache_updates(manager_ref):
    xs = []
    ys = []

    fetch_lefts=[]
    fetch_ys=[]
    fetch_widths=[]

    minitask_lefts=[]
    minitask_ys=[]
    minitask_widths=[]

    y = 0
    for worker in worker_info:
        y += 1
        for cache_update in worker_info[worker]["cache_updates"]:
            #  GETTING THE URL
            if cache_update[2].startswith("url"):
                for task in worker_info[worker]["tasks"]:
                    task_info = worker_info[worker]["tasks"][task]
                    update_time = cache_update[0]
                    if task_info["start"] < update_time and task_info["stop"] > update_time:
                        width = update_time - task_info["start"]
                        if manager_ref:
                            left =  task_info["start"] - manager_start
                        else:
                            left =  task_info["start"] - worker_info[worker]["first_task"]
                        worker_info[worker]["tasks"][task]["start"] = update_time
                        fetch_lefts.append(left)
                        fetch_ys.append(y)
                        fetch_widths.append(width)

            # DOING THE MINITASK
            elif cache_update[2].startswith("task"):
                for task in worker_info[worker]["tasks"]:
                    task_info = worker_info[worker]["tasks"][task]
                    update_time = cache_update[0]
                    if task_info["start"] < update_time and task_info["stop"] > update_time:
                        width = update_time - task_info["start"]
                        if manager_ref:
                            left =  task_info["start"] - manager_start
                        else:
                            left =  task_info["start"] - worker_info[worker]["first_task"]
                        worker_info[worker]["tasks"][task]["start"] = update_time
                        minitask_lefts.append(left)
                        minitask_ys.append(y)
                        minitask_widths.append(width)
            else:
                if manager_ref:
                    x = cache_update[0] - manager_start
                else:
                    x = cache_update[0] - worker_info[worker]["first_task"]
                if x > 0:
                    xs.append(x)
                    ys.append(y)

    plt.plot(xs , ys, '+', color="orange", label='Cache Updates')
    plt.barh(minitask_ys, minitask_widths, left=minitask_lefts, color="red", label='Minitask')
    plt.barh(fetch_ys, fetch_widths, left=fetch_lefts, color="mistyrose", label='Curl URL')

def plot_workers(title='Worker Info', all_info=False, save=None, c_updates=False, flip=False, resources=False, done=False, IT=False, OT=False, xticks=None, yticks=None, manager_ref=False):
    count = 0
    done_xs = []
    IT_lefts = []
    OT_xs = []
    done_ys = []
    IT_ys = []
    OT_ys = []
    IT_widths = []

    ys = []
    ys2 = []
    widths= []
    widths2 = []
    lefts = []
    lefts2 = []
    # PLOT CACHE UPDATES
    if all_info or c_updates:
        plot_cache_updates(manager_ref)
    # PLOT RESOURCES REPORTS
    if all_info or resources:
        plot_resource_updates(manager_ref)
    for worker in worker_info:
        count += 1
        if "tasks" in worker_info[worker]:
            t_count = 1


            for task in worker_info[worker]["tasks"]:
                if "start" not in worker_info[worker]["tasks"][task] or "stop" not in worker_info[worker]["tasks"][task]:
                        break
                if worker_info[worker]["tasks"][task]["stop"] == -1:
                        break

                # DONE MARKERS
                if "done" in worker_info[worker]["tasks"][task]:
                    if manager_ref:
                        x = worker_info[worker]["tasks"][task]["done"] - manager_start
                    else:
                        x = worker_info[worker]["tasks"][task]["done"] - worker_info[worker]["first_task"]
                    done_xs.append(x)
                    done_ys.append(count)

                # TASKS
                if flip:
                    if t_count%2 == 1:
                        widths.append(worker_info[worker]["tasks"][task]["stop"] - worker_info[worker]["tasks"][task]["start"])
                        if manager_ref:
                            lefts.append(worker_info[worker]["tasks"][task]["start"] - manager_start)
                        else:
                            lefts.append(worker_info[worker]["tasks"][task]["start"] - worker_info[worker]["first_task"])
                        ys.append(count)
                    else:
                        widths2.append(worker_info[worker]["tasks"][task]["stop"] - worker_info[worker]["tasks"][task]["start"])
                        if manager_ref:
                            lefts2.append(worker_info[worker]["tasks"][task]["start"] - manager_start)
                        else:
                            lefts2.append(worker_info[worker]["tasks"][task]["start"] - worker_info[worker]["first_task"])
                        ys2.append(count)
                    t_count += 1
                else:
                    widths.append(worker_info[worker]["tasks"][task]["stop"] - worker_info[worker]["tasks"][task]["start"])
                    if manager_ref:
                        lefts.append(worker_info[worker]["tasks"][task]["start"] - manager_start)
                    else:
                        lefts.append(worker_info[worker]["tasks"][task]["start"] - worker_info[worker]["first_task"])
                    ys.append(count)

                # INPUT TRANSFERS
                if all_info or IT:
                    for I_transfer in worker_info[worker]["tasks"][task]["I_transfer"]:
                        if manager_ref:
                            IT_lefts.append(I_transfer[0] - manager_start)
                        else:
                            IT_lefts.append(I_transfer[0] - worker_info[worker]["first_task"])
                        IT_ys.append(count)
                        IT_widths.append(I_transfer[1])

    # PLOT TASKS
    if flip:
        plt.barh(ys, widths, left=lefts, color="blue", label='Tasks')
        plt.barh(ys2, widths2, left=lefts2, color="darkblue", label='Tasks')
    else:
        plt.barh(ys, widths, left=lefts, color='blue', label='Tasks')
    # PLOT DONE MARKERS
    if all_info or done:
        plt.plot(done_xs, done_ys, 'g+', label='Marked Done')
    # PLOT INPUT TRANSFERS
    if all_info or IT:
        plt.barh(IT_ys, IT_widths, left=IT_lefts, color="black", label='Input Transfers')
        # plt.plot(IT_xs , IT_ys, 'k+',label='Input Transfers')
    plt.title(title)
    plt.ylabel("Worker Number")
    plt.xlabel("time")
    plt.tick_params(axis='both', which='major', labelsize=15)
    plt.legend()
    if xticks:
        tick_list = [int(xticks[0])]
        s = tick_list[0]
        for x in range(int(xticks[2])):
            s += int(xticks[1])
            tick_list.append(s)
        plt.xticks(tick_list)
    if yticks:
        tick_list = [int(yticks[0])]
        s = tick_list[0]
        for x in range(int(yticks[2])):
            s += int(yticks[1])
            tick_list.append(s)
        plt.yticks(tick_list)
    if save:
        plt.savefig(save)
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot worker information from transaction lof file.')
    parser.add_argument('log', help='Path to transaction log file')
    parser.add_argument('mode', help='changes mode in which information can be diplayed (-Workers|-Tasks|-Manager)')
    parser.add_argument('title', nargs='?', default ='Worker Info', help='Title of the plot')
    parser.add_argument('save', nargs='?', default = None, help='Save Figure')
    parser.add_argument('-a', action='store_true', help='display all from the transaction log')
    parser.add_argument('-f', action='store_true', help='flip colors between tasks')
    parser.add_argument('-c', action='store_true', help='display cache updates')
    parser.add_argument('-r', action='store_true', help='display resource updates')
    parser.add_argument('-d', action='store_true', help='display done markers')
    parser.add_argument('-i', action='store_true', help='display input transfers')
    parser.add_argument('-o', action='store_true', help='display output transfers')
    parser.add_argument('-s', action='store_true', help='print stats')
    parser.add_argument('-x', nargs=3, help='change scale for x ticks -x <start> <step_size> <steps>')
    parser.add_argument('-y', nargs=3, help='change scale for y ticks -y <start> <step_size> <steps>')
    parser.add_argument('-m', action='store_true', help='use manager start time as plot reference point Default: workers first task')
    args = parser.parse_args()
    read_log(args.log, args.s)
    plot_workers(title=args.title,
                 all_info=args.a,
                 save=args.save,
                 flip=args.f,
                 c_updates=args.c,
                 resources=args.r,
                 done=args.d,
                 IT=args.i,
                 OT=args.o,
                 xticks = args.x,
                 yticks = args.y,
                 manager_ref = args.m)
