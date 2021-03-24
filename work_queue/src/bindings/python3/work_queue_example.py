#!/usr/bin/env python

# Copyright (c) 2021- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This program is a simple example of how to use Work Queue.
# It is a toy example of diffusion of particles by Brownian motion.
# It accepts two parameters: number of particles and time steps.
# Each work queue task will simulate the Brownian motion of a particle that can
# move left or right each probability 1/2.

import work_queue as wq

import sys
import math
from collections import defaultdict

def show_help():
    print("""Toy example of diffusion of particles by Brownian motion.

    Usage: work_queue_basic_example.py PARTICLES MAX_STEPS
    where:
       PARTICLES   Number of particles to simulate
       STEPS       Number of steps each particle randomly moves""")


def read_arguments():
    try:
        (total_particles, steps) = sys.argv[1:]
    except ValueError:
        show_help()
        print("Error: Incorrect number of arguments")

    try:
        total_particles = int(total_particles)
        steps = int(steps)
    except ValueError:
        print("Error: Arguments are not integers")
    return (total_particles, steps)


def process_result(particle_index, particles_at_position):
    simulation_result = "particle_{}.out".format(particle_index)
    try:
        with open(simulation_result) as f:
            for (time, position) in enumerate(f):
                position = int(position)
                particles_at_position[time][position] += 1
    except IOError as e:
        print("Error reading file: {}: {}".format(simulation_result, e))

def analyze_results(particles_at_position, total_particles, steps):
    print("time-step\tavg\tsd")
    for step in range(0, steps+1, 10):
        mean = 0
        for (pos, count) in particles_at_position[step].items():
            mean += pos * count
        mean /= total_particles

        var = 0
        for (pos, count) in particles_at_position[step].items():
            var += count * ((pos - mean) ** 2)
        var /= total_particles

        print("{:9d}\t{:.3f}\t{:.3f}".format(step, mean, math.sqrt(var)))


def print_error_log(particle_index):
    error_log = "particle_{}.err".format(particle_index)
    try:
        with open(error_log) as f:
            print(f.read())
    except IOError:
        pass


# particles_at_position[TIME][POSITION] is a mapping that keeps the count of
# particles at a given TIME and POSITION.
particles_at_position = defaultdict(lambda: defaultdict(lambda: 0))

(total_particles, steps) = read_arguments()


q = wq.WorkQueue(9123)


for particle_index in range(total_particles):
    output_name = "particle_{}.out".format(particle_index)
    error_name  = "particle_{}.err".format(particle_index)

    t = wq.Task("./random_walk.py {steps} {out} 2> {err}".format(steps=steps, out=output_name, err=error_name))

    t.specify_tag("{}".format(particle_index))

    t.specify_input_file("random_walk.py", cache=True)
    t.specify_output_file(output_name, cache=False)
    t.specify_output_file(error_name, cache=False)

    t.specify_cores(1)
    t.specify_memory(100)
    t.specify_disk(200)

    q.submit(t)


# wait for all tasks to complete
while not q.empty():
    t = q.wait(5)
    if t:
        print("Task {id} has returned.".format(id=t.id))
        if t.result == wq.WORK_QUEUE_RESULT_SUCCESS and t.return_status == 0:
                particle_index = t.tag
                process_result(particle_index, particles_at_position)
        else:
            print("    It could not be completed successfully. Result: {}. Exit code: {}".format(t.result, t.return_status))
            print_error_log(particle_index)


print("All task have finished.")
print("Computing results:")

analyze_results(particles_at_position, total_particles, steps)


