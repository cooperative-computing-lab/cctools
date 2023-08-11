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
default_max_seq = 100

tmpdir = tempfile.mkdtemp()
os.chdir(tmpdir)

prob = 0.5**(1/6)
while count < total:
    if random.random() > prob:
        cluster_cores = default_cluster_cores + random.randint(0, 2)
    else:
        cluster_cores = default_cluster_cores - random.randint(0, 2)

    if random.random() > prob:
        cluster_mem = default_cluster_mem + random.randint(0, 500)
    else:
        cluster_mem = default_cluster_mem + random.randint(0, 500)

    disk = default_disk

    if random.random() > prob:
        jobs = default_jobs + random.randint(0, 40)
    else:
        jobs = default_jobs - random.randint(0, 40)

    if random.random() > prob:
        max_seq = default_max_seq + random.randint(0, 20)
    else:
        max_seq = default_max_seq - random.randint(0, 20)

    with open(f"input.csv", "w") as f:
        f.write(f"{cluster_cores},{cluster_mem},{disk},{jobs},{max_seq}")

    os.system(f"tar zcvf input_{count}.tar.gz input.csv")
    os.system(f"cp input_{count}.tar.gz {inbox_path}")

    count += 1
    # time.sleep(30)

shutil.rmtree(tmpdir)
