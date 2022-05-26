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

# Thoughts:
# - For shared files: fifo-0,push-async-1 is equivalent to fifo-0,pull-inf

TASKS = 25
SHARED = []
for i in (20, 32, 31, 30, 22, 24, 26, 28):
    SHARED.append({'count': 32, 'prefix': ('%dM-shared' % (2**(i-20))), 'size': functools.partial(lambda i: 2**i, i)})
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
while [ "$#" -ge 3 ]; do
    openssl enc -aes-256-ctr -nosalt -pass pass:"$1" < /dev/zero 2> /dev/null | head -c "$2" > "$3"
    shift
    shift
    shift
done
''', cmd_format = "{EXE} {ARG}")

gen = []

shared = []
for i in range(TASKS):
    shared.append(nstdir('sync.%08d' % i))
for f in SHARED:
    for i in range(f['count']):
        path = nstdir((f['prefix'] + '.%08d') % i)
        gen.append({'path': path, 'size': f['size']})
        shared.append(path)

for task in range(TASKS):
    print("compiling task %d" % task)
    inputs = []
    inputs.extend(shared)
    taskdir = nstdir('task.%08d' % task)
    os.mkdir(taskdir)
    for f in UNIQUE:
        for i in range(f['count']):
            path = os.path.join(taskdir, (f['prefix'] + '.%08d') % i)
            inputs.append(path)
            gen.append({'path': path, 'size': f['size']()})
    consumer(arguments = inputs, inputs = inputs)

random.shuffle(gen)

def makerandoms(i, files):
    sync = nstdir('sync.%08d' % i)
    args = [sync]
    outputs = [sync]
    # create a big file so these don't finish too quickly...
    args.extend((binascii.hexlify(os.urandom(64)), 16*2**30, '__big.%08d' % i))
    for f in files:
        args.extend((binascii.hexlify(os.urandom(64)), f['size'](), f['path']))
        outputs.append(f['path'])
    producer(arguments = args, outputs = outputs)

for i in range(TASKS):
    makerandoms(i, gen[i::TASKS])

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
