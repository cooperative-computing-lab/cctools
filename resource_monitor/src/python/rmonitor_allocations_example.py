# Copyright (C) 2016- The University of Notre Dame This software is distributed
# under the GNU General Public License.  See the file COPYING for details.

from ResourceMonitor import *
import random

# Generate syntetic resource samples according to beta(2, 5)
def beta(start, end, alpha = 2, beta = 5):
    return int((end - start) * random.betavariate(alpha, beta)) + start

# Generate syntetic resource samples according to exp(1.25)
def exponential(start, end, lambd = 1.25):
    return int((end - start) * random.expovariate(lambd)) + start

# Generate syntetic resource samples according to triangular(0.1)
def triangular(start, end, mode = 0.1):
    return int(random.triangular(start, end, start + mode*(end - start)))

if __name__ == '__main__':
    # set seed so that we can compare runs.
    random.seed(42)

    # wall time in seconds
    wall_time   = 1

    # min memory, in MB
    memory_min  = 50

    # max memory, in MB
    memory_max  = 10000

    # number of samples to compute per category
    number_of_tasks = 10000

    # create an empty set of categories
    categories = Categories();

    # generate number_of_tasks memory samples of the category 'example'
    for i in range(number_of_tasks):
        resources = { 'category': 'beta',        'memory': beta(memory_min, memory_max),        'wall_time': wall_time} 
        categories.accumulate_summary(resources)

    for i in range(number_of_tasks):
        resources = { 'category': 'exponential', 'memory': exponential(memory_min, memory_max), 'wall_time': wall_time} 
        categories.accumulate_summary(resources)

    for i in range(number_of_tasks):
        resources = { 'category': 'triangular',  'memory': triangular(memory_min, memory_max),  'wall_time': wall_time} 
        categories.accumulate_summary(resources)

    # print the first allocations found
    for name in categories.category_names():
        fa = categories.first_allocation(mode = 'throughput', category = name)
        print '%-15s: %5d' % (name, fa['memory'])

