#! /usr/bin/env python

import sys
import random
import time

try:
    steps, output_name = int(sys.argv[1]), sys.argv[2]
except (IndexError, ValueError):
    print("""Usage: random_walk.py NUM_OF_STEPS
  where:
    NUM_OF_STEPS  Sets the number of steps to take in the random walk.
""")

current_position=0

with open(output_name, 'w') as fout:
    fout.write("{}\n".format(current_position))
    for step in range(steps):
        coin = random.randint(0,1)

        if coin == 0:
            current_position += 1
        else:
            current_position -= 1

        fout.write("{}\n".format(current_position))

