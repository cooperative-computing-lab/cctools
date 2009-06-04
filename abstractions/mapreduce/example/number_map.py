#!/usr/bin/env python

import sys

for n in sys.stdin:
    sys.stdout.write('%04d\t1\n' % int(n))
