# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
import work_queue as wq
import time
import random
import tempfile
import shutil
import os
import sys

def task(x, y):
    # time.sleep(1)
    return x*y

def main():
    NAME = os.getlogin() + "-" + str(random.getrandbits(64))
    with open("input.csv", "r") as f:
        inp = f.read().split(',')
        cores = int(inp[0])
        memory = int(inp[1])
        disk = int(inp[2])
        max_workers = int(inp[3])
        seq_max = int(inp[4])
        print(inp)

    q = wq.WorkQueue(port=0, name=NAME, stats_log="stats.log")

    workers = wq.Factory("condor", NAME)

    workers.cores = cores
    workers.memory = memory
    workers.disk = disk
    workers.max_workers = max_workers
    workers.min_workers = max_workers
    workers.scratch_dir = tempfile.mkdtemp()

    with open("factory.log", "w") as f:
        f.write(f"{max_workers}\n")

    data = []
    count = 0
    while count < (seq_max**4)/2:
        count+=1
        data.append(count)

    seq = list(range(seq_max))

    with workers:
        start = time.time()
        results = q.pair(task, seq, seq)
        stop = time.time()
        print("Elapsed time:", str(stop-start))

    with open("output.txt", "w") as f:
        f.write(str(results))

    # count = 0
    # while count < (seq_max**4)/4:
    #     count+=1
    #     data.pop(0)

    shutil.rmtree(workers.scratch_dir)

if __name__ == "__main__":
    main()
