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
default_cluster_cores = 1
default_cluster_mem = 2000
default_disk = 5000
default_jobs = 10
default_max_seq = 100

tmpdir = tempfile.mkdtemp()
os.chdir(tmpdir)

while count < total:
    cluster_cores = default_cluster_cores #+ random.randint(-2, 2)
    cluster_mem = default_cluster_mem + random.randint(-500, 500)
    disk = default_disk
    jobs = default_jobs #- random.randint(-5, 5)
    max_seq = default_max_seq #- random.randint(-15, 15)

    with open(f"input.csv", "w") as f:
        f.write(f"{cluster_cores},{cluster_mem},{disk},{jobs},{max_seq}")

    os.system(f"tar zcvf input_{count}.tar.gz input.csv")
    os.system(f"cp input_{count}.tar.gz {inbox_path}")

    count += 1
    time.sleep(60)

shutil.rmtree(tmpdir)
