#!/usr/bin/env python



from work_queue import *

import os
import sys

if __name__ == '__main__':
  if len(sys.argv) < 2:
    print("work_queue_example <file1> [file2] [file3] ...")
    print("Each file given on the command line will be compressed using a remote worker.")
    sys.exit(1)

  filetrace_path = "./filetrace.py"
  gzip_path = "/bin/gzip"
  if not os.path.exists(gzip_path):
    gzip_path = "/usr/bin/gzip"
    if not os.path.exists(gzip_path):
      print("gzip was not found. Please modify the gzip_path variable accordingly. To determine the location of gzip, from the terminal type: which gzip (usual locations are /bin/gzip and /usr/bin/gzip)")
      sys.exit(1);
  if not os.path.exists(filetrace_path):
      print("filetrace not found, check path.")

  try:
      q = WorkQueue(port = WORK_QUEUE_DEFAULT_PORT)
  except:
      print("Instantiation of Work Queue failed!")
      sys.exit(1)

  print("listening on port %d..." % q.port)

  for i in range(1, len(sys.argv)):
      infile = "%s" % sys.argv[i]
      outfile = "%s.gz" % sys.argv[i]
      ftrace_outfile = "filetrace-%d.json" % (i)

      command = "./filetrace -n %d -j ./gzip < %s > %s" % (i,infile, outfile)

      t = Task(command)

      t.specify_file(gzip_path, "gzip", WORK_QUEUE_INPUT, cache=True)
      t.specify_file(filetrace_path, "filetrace", WORK_QUEUE_INPUT, cache=True)

      t.specify_file(infile, infile, WORK_QUEUE_INPUT, cache=False)
      t.specify_file(outfile, outfile, WORK_QUEUE_OUTPUT, cache=False)
      t.specify_file(ftrace_outfile, ftrace_outfile, WORK_QUEUE_OUTPUT, cache=False)

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