#!/usr/bin/env python

# This example program produces a mosaic of images, each one transformed
# with a different amount of swirl.

# It demonstrates several features of TaskVine:
#
# - Each task consumes remote data accessed via url, cached and shared
# among all tasks on that machine.
#
# - Each task uses the "convert" program, which may or may not be installed
# on remote machines.  To make the tasks portable, the program "/usr/bin/convert"
# is packaged up into a self-contained archive "convert.sfx" which contains
# the executable and all of its dynamic dependencies.  This allows the
# use of arbitrary workers without regard to their software environment.

import ndcctools.taskvine as vine
import argparse
import os
import sys

# construct a starch environment that ensures convert and montage are available
# when the task executes at the worker.
def create_env(env_name, convert="convert", montage="montage"):
    import subprocess
    # add the executables convert, and montage to the env_name starch file.
    # these executables are assumed to be in the current $PATH if a full path was not specified.
    # by default, the starch file will execute the convert command.

    if os.path.exists(env_name):
        print(f"reusing existing {env_name} starch file...")
    else:
        try:
            print(f"creating {env_name} starch file...")
            subprocess.run(["starch", "-x", "convert", "-x", "montage", "-c", "montage", env_name], check=True)
        except subprocess.CalledProcessError:
            print("could not create environment.")
            print("check that starch is in $PATH, and check that ImageMagick executables are in $PATH,")
            print("or provide their location at the command line.")


# helper function to report final status of a task
def process_result(t):
    if t:
        if t.successful():
            print(f"task {t.id} done: {t.command}")
        elif t.completed():
            print(f"task {t.id} completed with an executin error, exit code {t.exit_code}")
        else:
            print(f"task {t.id} failed with status {t.result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog="vine_example_mosaic.py",
            description="This example TaskVine program produces a mosaic of images, each one transformed with a different amount of swirl. Each task consumes remote data accessed via url, cached and shared among all tasks on that machine. Also each task is executed in a mini environment that ensures that the required executables and libraries are available at the worker.")

    parser.add_argument('--convert', action='store', help='path to the convert ImageMagick executable.', default="convert")
    parser.add_argument('--montage', action='store', help='path to the montage ImageMagick executable.', default="montage")
    args = parser.parse_args()

    env_filename = "convert_montage.sfx"
    create_env(env_filename)

    m = vine.Manager()
    print(f"listening on port {m.port}")

    # declare the environment just created as a starch file.
    # when this env is associated with a task, it will be expanded and wrap its
    # command.
    env = m.declare_starch(env_filename)

    # source image to which all the operations will be applied
    image_file = m.declare_url("https://upload.wikimedia.org/wikipedia/commons/7/74/A-Cat.jpg", cache=True)


    # the image_file will be rotated in steps of 10 degrees by the tasks. The
    # result of these rotations is kept in a vine temporary file. These
    # temporary files stay at the workers for task reuse, and are not
    # transfered back to the manager as outputs.

    # swirl angle -> vine temporary file
    convert_temporary_outputs = {}

    # from 0 to 360, by steps of 10 degrees
    for angle in range(0, 360, 10):
        # the command the task will execute. The convert executable will be
        # provided by the starch environment.
        # note that all tasks will produce as a result a file named output.jpg
        # in their respective sandboxes. It is this file that will be declared
        # as temporary file.
        command = f"convert -swirl {angle} cat.jpg output.jpg"

        t = vine.Task(command)
        t.add_environment(env)

        # add the main source image
        t.add_input(image_file,"cat.jpg")

        # declare the temporary file, associate it with output.jpg, and record
        # it for future use in montage.
        f = m.declare_temp()
        convert_temporary_outputs[angle] = f
        t.add_output(f, "output.jpg")

        # specify that tasks won't use more than one core. This allows the
        # manager to dispatch as many tasks to a worker as its number of cores.
        t.set_cores(1)

        m.submit(t)
        print(f"submitted task {t.id}: {t.command}")

    print("waiting for convert tasks to complete...")
    print("please create a worker in another terminal. E.g., for a local worker:")
    print(f"vine_worker localhost {m.port}")

    while not m.empty():
        t = m.wait(5)
        if t:
            process_result(t)

        # local image name to use for the temporary file when the convert task
        # executes.
        outfile = f"{angle}.cat.jpg"

    print("Combining images into mosaic.jpg...")

    # create a tasks that combines the results of all the convert tasks.
    # each convert temporary output will be mapped to an {angle}.cat.jpg file.
    # the montage executable will be provided by the starch environment
    t = vine.Task("montage `ls *.cat.jpg | sort -n` -tile 6x6 -geometry 128x128+0+0 output_of_montage.jpg")
    t.add_environment(env)

    for (angle, f) in convert_temporary_outputs.items():
        convert_output = f"{angle}.cat.jpg"
        t.add_input(f, convert_output)

    # declare the final image file
    final_montage = m.declare_file("mosaic.jpg")

    # add the final output, mapping mosaic.jpg to the file generated by montage: output_of_montage.jpg
    # note that output_of_montage.jpg could have been named mosaic.jpg, as
    # final_montage refers to the file in the manager's filesystem, not the
    # task sandbox.
    t.add_output(final_montage, "output_of_montage.jpg")
    m.submit(t)

    print("waiting for final task to complete...")
    while not m.empty():
        t = m.wait(5)
        if t:
            process_result(t)
    print("all tasks complete!")


    print("deleting temporary files at the worker.")
    # now that we are done using the temporary files, we can delete them from the workers.
    # in this small example this is not strictly necessary, as these files are
    # deleted from workers once the manager terminates.
    for f in convert_temporary_outputs.values():
        m.remove_file(f)

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
