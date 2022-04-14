import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys
import os

# config stuff
font = {'family': 'serif',
        'serif': ['Computer Modern Roman'],
        'weight': 'normal',
        'size': 33}
plt.rc('font', **font)
plt.rc('text', usetex=True)
plt.style.use('tableau-colorblind10')

filename = sys.argv[1]
df = pd.read_csv(filename)

dirname = os.path.basename(os.getcwd())

with open("config.csv") as f:
    rtypes = f.readline().strip().split(",")
    maxs = f.readline().strip().split(",")

set_max = True
if rtypes[-1] == "no_box":
    rtypes = rtypes[:-1]
    maxs = maxs[:-1]
    set_max = False

STATS = {r: int(m) for r,m in zip(rtypes, maxs)}

for resource in STATS:
    time = df["time"].to_numpy()
    mem_usage = df[resource].to_numpy()
    allocated_mem_usage = df[f"allocated_{resource}"].to_numpy()
    max_id = max(df["id"].to_numpy())

    prev_time = 0
    current_total = 0
    allocated_total = 0
    allocated_mem_usage_total = np.zeros(max(time))
    for index, (t, m, a) in enumerate(zip(time, mem_usage, allocated_mem_usage)):
        if t != prev_time:
            current_total = 0
            allocated_mem_usage_total[t-1] = allocated_total
            allocated_total = 0
        current_total += m
        allocated_total += a
        mem_usage[index] = current_total
        prev_time = t
    df[resource] = mem_usage.tolist()

    plt.figure(figsize=(8,5))
    plt.style.use('grayscale')
    for i in sorted(range(max_id+1), reverse=True):
        mem_usage = df.loc[df["id"] == i][resource].to_numpy()
        t  = df.loc[df["id"] == i]["time"].to_numpy()
        plt.fill_between(t, mem_usage)

    #
    # plt.scatter(time, mem_usage)
    plt.plot(np.arange(max(time)), np.full(max(time), STATS[resource]))
    if set_max:
        plt.plot(np.arange(max(time)), allocated_mem_usage_total)

    # plt.title(f"{resource} vs time")
    if dirname == "combo" or dirname == "bad":
        plt.xlabel("Time Steps (0.5 sec)")
    plt.ylabel(f"{resource}")
    # plt.tight_layout()

    plt.savefig(f"{dirname}_{resource}.pdf", bbox_inches='tight')



