#!/usr/bin/env python

# This example program demonstrates how to use standard 
# tasks in Work Queue.  The application uses the ImageMagick
# "convert" program to transfrom an image into 100 different variants.

# Advanced Feature:
# To ensure that the "convert" program can operate correctly
# on a remote machine that may not have the necessary dynamic libraries
# installed, we use the "starch" tool to convert the executable
# into a self-contained archive "convert.sfx" that can run anywhere.

# To run this program requires installing both ndcctools and ImageMagick:
# conda install -c conda-forge ndcctools ImageMagick

from work_queue import *

import os
import sys
import shutil

nimages = 100
url = "https://upload.wikimedia.org/wikipedia/commons/5/57/7weeks_old.JPG"

# First, find the "convert" program locally and pack it
# into a self-contained package "convert.sfx" using the "starch" command.

if not os.path.exists("convert.sfx"):
  print("packing convert into a starch archive...")
  fullpath = shutil.which("convert")
  if fullpath is None:
    print("cannot find 'convert' in your PATH!")
    sys.exit(1)
  os.system("starch -x {} -c convert convert.sfx".format(fullpath))

# Next, download an image of a dog from Wikimedia.

if not os.path.exists("dog.jpg"):
  print("downloading {}".format(url))
  os.system("curl {} > {}".format(url,"dog.jpg"))

# Set up a Work Queue listening on the default port.

q = WorkQueue(port = WORK_QUEUE_DEFAULT_PORT)
print("listening on port %d..." % q.port)

# Create a set of tasks that run the convert package.

for i in range(1,nimages):

  outfile = "dog.{}.jpg".format(i)

  # The task will invoke the convert package on the image, and swirl it.
  t = Task("./convert.sfx dog.jpg -swirl {} {}".format(i,outfile))

  # The program and dog.jpg are cacheable inputs, and outfile is an output file.
  t.specify_input_file("convert.sfx",cache=True)
  t.specify_input_file("dog.jpg",cache=True)
  t.specify_output_file(outfile,cache=False)

  t.specify_cores(1)

  taskid = q.submit(t)
  print("submitted task (id# %d): %s" % (taskid, t.command))

print("waiting for tasks to complete...")

# As long as there are more tasks outstanding,
# wait for them and display the result code.

while not q.empty():
  t = q.wait(5)
  if t:
    if t.return_status != 0:
      print("task {} complete".format(t.id))
      nsuccess += 1
    else:
      print("task {} failed with status {}: {}".format(t.id,t.return_status,t.output))
      nfailure +=1 

print("{} tasks succeeded, {} tasks failed".format(nsuccess,nfailure))

sys.exit(nfailure!=0)
