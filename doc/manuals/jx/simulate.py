#! /usr/bin/env python

# Mock simulator. It simply reads the parameter passed with --parameter, and
# prints a message to stdout.

import sys

if len(sys.argv) != 3 or sys.argv[1] != '--parameter':
    sys.stderr.write("""Usage:
        {} --parameter N
where N is a number.
""".format(sys.argv[0]))
    sys.exit(1)

parameter = sys.argv[2]

print('Simulation with parameter {}'.format(parameter))

