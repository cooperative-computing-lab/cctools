#! /usr/bin/env python

import sys
import random
import math

try:
    points_total, output_name = int(sys.argv[1]), sys.argv[2]
except (IndexError, ValueError):
    print("""Estimates the value of pi by estimating the ratio of the area of a
circle inscribed in a square. This ratio is estimated by generating random points
inside the square, and checking whether it also lies inside the circle.
    Usage: area_circ_sq.py NUM_OF_POINTS OUTPUT_FILE
  where:
    NUM_OF_POINTS  Total number of random points to generate
    OUTPUT_FILE    Name of file to write the results

  Results format:
    NUM_OF_POINTS NUM_OF_POINTS_IN_CIRCLE PI_ESTIMATE
""")
    sys.exit(1)


# Both square and circle are centered at the origin
# Circle is of radius r, thus square has sides of length 2r.
r = 1

point_count=0
points_inside_circle=0
for point_count in range(points_total):
    # generate random point with x,y coordinates
    x = r * random.uniform(-r, r)
    y = r * random.uniform(-r, r)

    # distance from origin to point
    h = math.sqrt(x*x + y*y)

    if h <= r:
        points_inside_circle += 1

# estimate computed as:
# area_circle = pi * r^2
# area_square = (2r)^2 = 4 * r^2
# areas_ratio = area_circle / area_square = pi/4, from which:
# pi = 4 * areas_ratio
#
# the areas ratio is estimated by the number of random points that fall inside
# the areas.

pi_estimate = float(4 * points_inside_circle) / points_total

with open(output_name, 'w') as fout:
    fout.write("{} {} {}\n".format(points_total, points_inside_circle, pi_estimate))

