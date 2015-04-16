import os
import itertools

from weaver.stack import WeaverNests
from weaver.util import Stash

BYTES=8
assert(BYTES % 2 == 0)
SIZE=128*2**30
MINSPLIT=2**30

#WeaverNests.push(Nest(work_dir = 'sort'))

# XXX Use arguments to pass nest pwd, then cd into it. generate named files (add them to outputs/inputs array)
isort = Function('./intsort', cmd_format = '{EXE} isort output input > stdout 2> stderr')
merge = Function('./intsort', cmd_format = '{EXE} merge output input1 input2 > stdout 2> stderr')
split = Function('./intsort', cmd_format = '{EXE} split input output1 output2 > stdout 2> stderr')

def mergesort(result, data, size):
    assert(size % BYTES == 0)
    half = size//2
    assert(half % BYTES == 0)

    if half <= MINSPLIT:
        isort(inputs = data, outputs = result)
    else:
        lsplit = CurrentNest().stash.next()
        rsplit = CurrentNest().stash.next()
        split(arguments = [half], inputs = [data], outputs = [lsplit, rsplit])

        lsorted = CurrentNest().stash.next()
        mergesort(lsorted, lsplit, half)
        rsorted = CurrentNest().stash.next()
        mergesort(rsorted, rsplit, half)

        merge(outputs = result, inputs = [lsorted, rsorted])

data = os.path.join(CurrentNest().work_dir, 'data')

makerandom = ShellFunction('''
    set -e
    mkdir -p "$(dirname "$2")"
    touch "$2"
    openssl enc -aes-256-ctr -pass pass:"$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64)" -nosalt < /dev/zero 2>/dev/null | head --bytes="$1" > "$2"
''', cmd_format = '{EXE} {ARG} {OUT}')
makerandom(arguments = [SIZE], outputs = data, local = True)

result = os.path.join(CurrentNest().work_dir, 'result')
mergesort(result, data, SIZE)
Function('./intsort', cmd_format = '{EXE} assert {IN}')(inputs = [result])

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
