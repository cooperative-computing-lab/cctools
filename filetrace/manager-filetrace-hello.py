#!/usr/bin/env python



from work_queue import *

import os
import sys

if __name__ == '__main__':

  filetrace_path = "./filetrace.py"
  hello_path = "./bin/hello_world"
  if not os.path.exists(hello_path):
    print("hello_world executable not found. please check path.")
    sys.exit(1)
  if not os.path.exists(filetrace_path):
    print("filetrace not found, check path.")
    sys.exit(1)


  try:
      q = WorkQueue(port = WORK_QUEUE_DEFAULT_PORT)
  except:
      print("Instantiation of Work Queue failed!")
      sys.exit(1)

  print("listening on port %d..." % q.port)

  num_of_workers = 10

  for i in range(1, num_of_workers+1):
      # infile = "%s" % sys.argv[i]
      # outfile = "%s.gz" % sys.argv[i]
      ftrace_outfile1 = "filetrace-path-%d.json" % (i)
      ftrace_outfile2 = "filetrace-process-%d.json" % (i)

      command = "./filetrace -n %d -j ./hello_world" % (i) # ,infile, outfile)

      t = Task(command)
      t.specify_cores(1)

      t.specify_file(hello_path, "hello_world", WORK_QUEUE_INPUT, cache=True)
      t.specify_file(filetrace_path, "filetrace", WORK_QUEUE_INPUT, cache=True)

      # t.specify_file(infile, infile, WORK_QUEUE_INPUT, cache=False)
      # t.specify_file(outfile, outfile, WORK_QUEUE_OUTPUT, cache=False)
      t.specify_file(ftrace_outfile1, ftrace_outfile1, WORK_QUEUE_OUTPUT, cache=False)
      t.specify_file(ftrace_outfile2, ftrace_outfile2, WORK_QUEUE_OUTPUT, cache=False)

      taskid = q.submit(t)
      print("submitted task (id# %d): %s" % (taskid, t.command))

  print("waiting for tasks to complete...")
  while not q.empty():
      t = q.wait(5)
      if t:
          print("task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status))
          if t.return_status != 0:
            None

  print("all tasks complete!")

  sys.exit(0)
