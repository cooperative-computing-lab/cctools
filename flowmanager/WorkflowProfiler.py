import psutil
import multiprocessing
import time


def get_process_stats(prc):
    try: 
        with prc.oneshot():
            cpu_usage = prc.cpu_percent()
            mem_usage = prc.memory_info().rss
    except psutil.NoSuchProcess:
        cpu_usage = 0
        mem_usage = 0

    return cpu_usage, mem_usage
            

def profile(pid, interval):
    max_cpu = 0
    max_mem = 0

    try:
        subject_process = psutil.Process(pid)
    except psutile.NoSuchProcess:
        return max_cpu, max_mem

    children = set()
    dead = set()
    while subject_process.is_running() and subject_process.status() != psutil.STATUS_ZOMBIE:
        try:
            cpu_usage, mem_usage = get_process_stats(subject_process)
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

        time.sleep(interval)

    return max_cpu, max_mem // 10**6
