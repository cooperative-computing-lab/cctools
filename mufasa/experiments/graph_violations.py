import matplotlib.pyplot as plt
import sys
import os
import pandas as pd
import numpy as np

# config stuff
font = {'family': 'serif',
        'serif': ['Computer Modern Roman'],
        'weight': 'normal',
        'size': 33}
plt.rc('font', **font)
plt.rc('text', usetex=True)
plt.style.use('tableau-colorblind10')
plt.figure(figsize=(8,5))

for dirname in sys.argv[1:]:
    df = pd.read_csv(f"{dirname}/resources.log")

    with open(f"{dirname}/config.csv") as f:
        rtypes = f.readline().strip().split(",")
        maxs = f.readline().strip().split(",")

    id_violations = set()
    if "no_box" in rtypes:
        rtypes.remove("no_box")

    time = df["time"].to_numpy()
    max_time = max(time)

    STATS = {r: int(m) for r,m in zip(rtypes, maxs)}

    started = []
    prev_started = 0
    for t in range(max_time):
        current_stats = df.loc[df["time"] == t]
        if not len(current_stats["id"].to_numpy()):
            started.append(0)
        else:
            num_started = max(max(current_stats["id"].to_numpy()), prev_started)
            prev_started = max(num_started, prev_started)
            started.append(num_started)

    plt.style.use('grayscale')
    plt.plot(np.arange(0, max_time), started, label=f"{dirname}")

plt.legend()
plt.xlabel("Time Steps (0.5 sec)")
plt.ylabel("Num Started/Restarted")
plt.savefig(f"{sys.argv[1]}_wfs_started.pdf", bbox_inches='tight')

