import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

df = pd.read_csv("resources.log")

time = df["time"].to_numpy()
mem_usage = df["cluster_mem"].to_numpy()
max_id = max(df["id"].to_numpy())

prev_time = -1
current_total = 0
for index, (t, m) in enumerate(zip(time, mem_usage)):
    if t != prev_time:
        current_total = 0
    current_total += m
    mem_usage[index] = current_total
    prev_time = t
df["cluster_mem"] = mem_usage.tolist()

for i in range(max_id):
    mem_usage = df.loc[df["id"] == i]["cluster_mem"].to_numpy()
    t  = df.loc[df["id"] == i]["time"].to_numpy()
    plt.plot(t, mem_usage)

#
# plt.scatter(time, mem_usage)
plt.plot(np.arange(max(time)), np.full(max(time), 80000))

plt.title("Cluster Memory Usage vs Time")
plt.xlabel("Time")
plt.ylabel("Memory (MB)")
plt.tight_layout()

plt.savefig("cluster_mem_usage.jpg")



