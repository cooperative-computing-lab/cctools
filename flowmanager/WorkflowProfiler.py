import psutil
import multiprocessing
import os
import time
import subprocess


def get_process_stats(prc):
    try: 
        with prc.oneshot():
            cpu_usage = prc.cpu_percent()
            mem_usage = prc.memory_info().rss
    except psutil.NoSuchProcess:
        cpu_usage = 0
        mem_usage = 0

    return cpu_usage, mem_usage

def get_workflow_stats(workflow_path):
    stats_file = os.path.join(workflow_path, "stats.log")

    if not os.path.exists(stats_file):
        return 0, 0

    prc = subprocess.run(["head", "-n", "1", stats_file], capture_output=True)
    types = prc.stdout.decode().strip().split(' ')

    prc = subprocess.run(["tail", "-n", "1", stats_file], capture_output=True)
    stats = prc.stdout.decode().strip().split(' ')
    cores = int(stats[types.index("total_cores")-1]) # at somepoint stop hardcoding this
    memory = int(stats[types.index("total_memory")-1])

    return cores, memory
            

def profile(pid, workflow_path, interval):
    max_cpu = 0
    max_mem = 0
    max_cluster_cpu = 0
    max_cluster_mem = 0

    try:
        subject_process = psutil.Process(pid)
    except psutile.NoSuchProcess:
        return max_cpu, max_mem

    children = set()
    dead = set()
    while subject_process.is_running() and subject_process.status() != psutil.STATUS_ZOMBIE:
        try:
            cpu_usage, mem_usage = get_process_stats(subject_process)
            cluster_cpu_usage, cluster_mem_usage = get_workflow_stats(workflow_path)
            children.update(subject_process.children(recursive=True))
        except psutil.NoSuchProcess:
            break

        for child in children:
            if not child.is_running():
                dead.add(child)
                continue
            ccpu, cmem = get_process_stats(child)
            cpu_usage += ccpu
            mem_usage += cmem

        children = children.difference(dead)

        max_cpu = max(cpu_usage, max_cpu)
        max_mem = max(mem_usage, max_mem)
        max_cluster_cpu = max(cluster_cpu_usage, max_cluster_cpu)
        max_cluster_mem = max(cluster_mem_usage, max_cluster_mem)

        time.sleep(interval)

    return max_cpu, max_mem // 10**6, max_cluster_cpu, max_cluster_mem
