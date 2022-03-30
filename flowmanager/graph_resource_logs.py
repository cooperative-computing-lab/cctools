import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

df = pd.read_csv("resources.log")

time = df["time"].to_numpy()
mem_usage = df["cluster_mem"].to_numpy()
allocated_mem_usage = df["allocated_cluster_mem"].to_numpy()
# print(allocated_mem_usage)
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
df["cluster_mem"] = mem_usage.tolist()

plt.figure(figsize=(8,4))
plt.style.use('grayscale')
for i in sorted(range(max_id+1), reverse=True):
    mem_usage = df.loc[df["id"] == i]["cluster_mem"].to_numpy()
    t  = df.loc[df["id"] == i]["time"].to_numpy()
    plt.fill_between(t, mem_usage)

#
# plt.scatter(time, mem_usage)
plt.plot(np.arange(max(time)), np.full(max(time), 80000))
# plt.plot(np.arange(max(time)), allocated_mem_usage_total)

plt.title("Cluster Memory Usage vs Time")
plt.xlabel("Time Steps (0.5 sec)")
plt.ylabel("Memory (MB)")
plt.tight_layout()

plt.savefig("cluster_mem_usage.jpg")



