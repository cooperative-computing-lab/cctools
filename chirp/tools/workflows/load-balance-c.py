import binascii
import itertools
import os
import random
import subprocess

from weaver.stack import WeaverNests
from weaver.util import Stash

UNIQUE_SMALL_FILES = 16
SHARED_LARGE_FILES = 0
TASKS = 16

def sfs():
    return 1*2**30
def lfs():
    return 1*2**30

consumer = ShellFunction('''
    /bin/cat "$@" > /dev/null
    sleep 120
''', cmd_format = "{EXE} {ARG}")

def makerandom(name, size):
    with open(name, 'wb') as f:
        subprocess.check_call('openssl enc -aes-256-ctr -nosalt -pass pass:%s < /dev/zero 2> /dev/null | head -c %u' % (binascii.hexlify(os.urandom(64)), size), stdout = f, shell = True)

shared = []
for i in range(SHARED_LARGE_FILES):
    input = os.path.join(CurrentNest().work_dir, 'big.%08d' % i)
    makerandom(input, lfs())
    shared.append(input)

for task in range(TASKS):
    inputs = []
    inputs.extend(shared)
    print("compiling task %d" % task)
    taskdir = os.path.join(CurrentNest().work_dir, 'task.%08d' % task)
    os.mkdir(taskdir)
    for i in range(UNIQUE_SMALL_FILES):
        input = os.path.join(taskdir, 'conf.%08d' % i)
        makerandom(input, sfs())
        inputs.append(input)
    consumer(arguments = inputs, inputs = inputs)

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
