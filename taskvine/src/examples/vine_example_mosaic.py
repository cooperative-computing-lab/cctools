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
        montage = sys.argv[2]
    except IndexError:
        convert = "/usr/bin/convert"
        montage = "/usr/bin/montage"

    print(f"Checking that {convert} is installed...")
    if not os.access(convert, os.X_OK):
        print(sys.argv[0], f": {convert} is not installed: this won't work at all.")
        sys.exit(1)

    print(f"Converting {convert} into convert.sfx...")
    if os.system(f"starch -x {convert} -c convert convert.sfx") != 0:
        print(sys.argv[0], ": failed to run starch, is it in your PATH?")
        sys.exit(1)

    print(f"Converting {montage} into montage.sfx...")
    if os.system(f"starch -x {montage} -c montage montage.sfx") != 0:
        print(sys.argv[0], ": failed to run starch, is it in your PATH?")
        sys.exit(1)

    m = vine.Manager()
    print("listening on port", m.port)

    temp_files = []
    for i in range(0, 36):
        temp_files.append(vine.FileTemp())

    montage_file = vine.FileLocal("montage.sfx");
    convert_file = vine.FileLocal("convert.sfx");
    image_file = vine.FileURL("https://upload.wikimedia.org/wikipedia/commons/7/74/A-Cat.jpg");
    
    for i in range(0, 36):
        outfile = str(i) + ".cat.jpg"
        command = "./convert.sfx -swirl " + str(i*10) + " cat.jpg output.jpg"

        t = vine.Task(command)
        t.add_input(convert_file, "convert.sfx", cache=True)
        t.add_input(image_file,"cat.jpg",cache=True)
        t.add_output(temp_files[i],"output.jpg",cache=True)

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
                print("Task " + str(id) + " failed: " + command)

    print("Combining images into mosaic.jpg...")

    t = vine.Task("montage `ls *.cat.jpg | sort -n` -tile 6x6 -geometry 128x128+0+0 mosaic.jpg")
    t.add_input(montage_file, "montage.sfx", cache=True)
    for i in range(0, 36):
        t.add_input(temp_files[i],str(i*10)+".cat.jpg",cache=False)
    t.add_output_file("mosaic.jpg", cache=False)

    m.submit(t)
    t = m.wait(vine.VINE_WAIT_FOREVER)

    print("All tasks complete!")
