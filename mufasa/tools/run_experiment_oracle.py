#!/usr/bin/env python
import os
import sys
import shutil
import tempfile
import random
import time

random.seed(7)
count = 0
total = int(sys.argv[1])
inbox_path = os.path.abspath(sys.argv[2])
default_cluster_cores = 4
default_cluster_mem = 500
default_disk = 5000
default_jobs = 200
default_max_seq = 80

tmpdir = tempfile.mkdtemp()
os.chdir(tmpdir)

prob = 0.5**(1/6)
while count < total:
    cluster_cores = default_cluster_cores
    cluster_mem = default_cluster_mem
    disk = default_disk
    jobs = default_jobs
    max_seq = default_max_seq

    with open(f"input.csv", "w") as f:
        f.write(f"{cluster_cores},{cluster_mem},{disk},{jobs},{max_seq}")

    os.system(f"tar zcvf input_{count}.tar.gz input.csv")
    os.system(f"cp input_{count}.tar.gz {inbox_path}")

    count += 1
    # time.sleep(30)

shutil.rmtree(tmpdir)
