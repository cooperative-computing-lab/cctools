import binascii
import functools
import itertools
import os
import random
import subprocess

from weaver.stack import WeaverNests
from weaver.util import Stash

def nstdir(path):
    return os.path.join(CurrentNest().work_dir, path)

NODES = 25
TASKS = 1
SHARED = []
for i in range(512):
    h = "%08x" % i;
    SHARED.append(h)
UNIQUE = []

consumer = ShellFunction('''
for f; do
    test -e "$f" || exit 1
done
''', cmd_format = "{EXE} {ARG}")
producer = ShellFunction('''
set -e
touch "$1"
shift
while [ "$#" -ge 1 ]; do
    printf "$1" > "$1"
    shift
done
# don't finish too quickly so producers are distributed across all 25 nodes
sleep 30
''', cmd_format = "{EXE} {ARG}")

gen = []

shared = []
for i in range(NODES):
    shared.append(nstdir('sync.%08d' % i))
for f in SHARED:
    path = nstdir(f)
    gen.append(path)
    shared.append(path)

for task in range(TASKS):
    print("compiling task %d" % task)
    inputs = []
    inputs.extend(shared)
    consumer(arguments = inputs, inputs = inputs)

random.shuffle(gen)

def makefiles(i, files):
    sync = nstdir('sync.%08d' % i)
    args = [sync]
    outputs = [sync]
    for f in files:
        args.append(f)
        outputs.append(f)
    producer(arguments = args, outputs = outputs)

for i in range(NODES):
    makefiles(i, gen[i::NODES])

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
