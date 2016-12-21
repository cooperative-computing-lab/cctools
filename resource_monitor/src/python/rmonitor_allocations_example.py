# Copyright (C) 2016- The University of Notre Dame This software is distributed
# under the GNU General Public License.  See the file COPYING for details.

from ResourceMonitor import *
import random

# Generate syntetic resource samples according to beta(2, 5)
def beta(start, end, alpha = 2, beta = 5):
    return int((end - start) * random.betavariate(alpha, beta)) + start

if __name__ == '__main__':

    # set seed so that we can compare runs.
    random.seed(42)

    # wall time in seconds
    wall_time   = 1

    # min memory, in MB
    memory_min  = 50

    # max memory, in MB
    memory_max  = 1000

    # memory size of node
    memory_node = 2000

    # number of samples to compute
    number_of_tasks = 10000

    # create an empty set of categories
    categories = Categories();

    # generate number_of_tasks memory samples of the category 'example'
    for i in range(number_of_tasks):
        resources = { 'category': 'example', 'memory': beta(memory_min, memory_max), 'wall_time': wall_time} 
        categories.accumulate_summary(resources)

    # print the first allocations found
    for name in categories.category_names():
        fa = categories.first_allocation(name)
        print '%-10s: %5d' % (name, fa['memory'])

