import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys

filename = sys.argv[1]
df = pd.read_csv(filename)

STATS = {"cpuusage": 800, "memusage": 8000, "jobs": 2000}

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

    plt.figure(figsize=(8,4))
    plt.style.use('grayscale')
    for i in sorted(range(max_id+1), reverse=True):
        mem_usage = df.loc[df["id"] == i][resource].to_numpy()
        t  = df.loc[df["id"] == i]["time"].to_numpy()
        plt.fill_between(t, mem_usage)

    #
    # plt.scatter(time, mem_usage)
    plt.plot(np.arange(max(time)), np.full(max(time), STATS[resource]))
    plt.plot(np.arange(max(time)), allocated_mem_usage_total)

    plt.title(f"{resource} vs time")
    plt.xlabel("Time Steps (0.5 sec)")
    plt.ylabel(f"{resource}")
    plt.tight_layout()

    plt.savefig(f"{resource}.jpg")



