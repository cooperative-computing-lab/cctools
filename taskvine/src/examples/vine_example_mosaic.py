#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example program produces a mosaic of images, each one transformed
# with a different amount of swirl.

# It demonstrates several features of taskvine:

# - Each task consumes remote data accessed via url, cached and shared
# among all tasks on that machine.

# - Each task uses the "convert" program, which may or may not be installed
# on remote machines.  To make the tasks portable, the program "/usr/bin/convert"
# is packaged up into a self-contained archive "convert.sfx" which contains
# the executable and all of its dynamic dependencies.  This allows the
# use of arbitrary workers without regard to their software environment.

import taskvine as vine
import os
import sys

if __name__ == "__main__":

    try:
        convert = sys.argv[1]
    except IndexError:
        convert = "/usr/bin/convert"

    print(f"Checking that {convert} is installed...")
    if not os.access(convert, os.X_OK):
        print(sys.argv[0], f": {convert} is not installed: this won't work at all.")
        sys.exit(1)

    print(f"Converting {convert} into convert.sfx...")
    if os.system(f"starch -x {convert} -c convert convert.sfx") != 0:
        print(sys.argv[0], ": failed to run starch, is it in your PATH?")
        sys.exit(1)

    try:
        m = vine.Manager()
    except IOError as e:
        print("couldn't create manager:", e.errno)
        sys.exit(1)
    print("listening on port", m.port)

    m.enable_debug_log("manager.log")

    for i in range(0, 360, 10):
        outfile = str(i) + ".cat.jpg"
        command = "./convert.sfx -swirl " + str(i) + " cat.jpg " + str(i) + ".cat.jpg"

        t = vine.Task(command)
        t.add_input_file("convert.sfx", "convert.sfx", cache=True)
        t.add_input_url(
            "https://upload.wikimedia.org/wikipedia/commons/7/74/A-Cat.jpg",
            "cat.jpg",
            cache=True,
        )
        t.add_output_file(outfile, outfile, cache=False)

        t.set_cores(1)

        task_id = m.submit(t)

        print("Submitted task (id# " + str(task_id) + "): " + command)

    print("Waiting for tasks to complete...")

    while not m.empty():
        t = m.wait(5)
        if t:
            r = t.result
            id = t.id

            if r == vine.VINE_RESULT_SUCCESS:
                print("Task " + str(id) + " complete: " + command)
        else:
            print("Task " + str(id) + " falied: " + command)

    print("All tasks complete!")

    print("Combining images into mosaic.jpg...")
    os.system(
        "montage `ls *.cat.jpg | sort -n` -tile 6x6 -geometry 128x128+0+0 mosaic.jpg"
    )

    print("Deleting intermediate images...")
    for i in range(0, 360, 10):
        filename = str(i) + ".cat.jpg"
        os.unlink(filename)
