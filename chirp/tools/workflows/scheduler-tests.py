import binascii
import itertools
import os
import random
import subprocess

from weaver.stack import WeaverNests
from weaver.util import Stash

UNIQUE_FILES = 1
SHARED_FILES = 0
TASKS = 25

def ufs():
    return 10*2**30
def sfs():
    return 1*2**30

consumer = ShellFunction('sleep 5', cmd_format = "{EXE} {ARG}")
producer = ShellFunction('openssl enc -aes-256-ctr -nosalt -pass pass:"$1" < /dev/zero 2> /dev/null | head -c "$2"')

def makerandom(name, size):
    producer(arguments = [binascii.hexlify(os.urandom(64)), size], outputs = [name])
    return name

shared = []
for i in range(SHARED_FILES):
    input = os.path.join(CurrentNest().work_dir, 'shared.%08d' % i)
    makerandom(input, sfs())
    shared.append(input)

for task in range(TASKS):
    inputs = []
    inputs.extend(shared)
    print("compiling task %d" % task)
    taskdir = os.path.join(CurrentNest().work_dir, 'task.%08d' % task)
    os.mkdir(taskdir)
    for i in range(UNIQUE_FILES):
        inputs.append(makerandom(os.path.join(taskdir, 'unique.%08d' % i), ufs()))
    for i in range(10):
        inputs.append(makerandom(os.path.join(taskdir, 'extra.%08d' % i), 2*2**30))
    consumer(arguments = inputs, inputs = inputs)

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
