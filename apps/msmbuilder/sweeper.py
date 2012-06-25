# -*- coding: utf-8 -*-
# Copyright (C) 2012- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#
# This program provides a simple api to sweep though a range of parameters
# and store the output in progname/param/
#

from work_queue import *
import sys, os, itertools

class Sweeper:
    """Provides a simple api for a program to sweep through a range of parameters.
        Usage: import sweeper
               x = sweeper.Sweeper()"""
    def __init__(self):
        self.port = WORK_QUEUE_DEFAULT_PORT # 9123
        self.progname = ""                  # the program to sweep with
        self.envpath = ""                   # path to a environment set up script
        self.paramvalues = []               # list of arguments e.g. -l -o 27
        self.command = []                   # list of the full command to be run
        self.sweeps = []                    # list of variables to sweep over
        self.inputlist = []                 # list of input files
        self.outputlist = []                # list of output files

    def addprog(self, progname):
        """The program (sans parameters) to sweep with.
            Usage: x.addprog("echo")
            @param progname The base program that will be run."""
        self.command.append(progname)
        self.progname, sep, tail = progname.partition('.') # remove file extension so we can create a nice directory

    def setenv(self, pathtoenv):
        """Set the path to a env script to set up an environment for the command to run in.
            Usage: x.setenv("env.sh")
            @param pathtoenv The path to a script to run first."""
        self.envpath = pathtoenv

    def addparameter(self, param):
        """Add a parameter to the list of parameters(arguments).
            Usage: x.addparameter("-n")
                   x.addsweep(xrange(1,10,2))
                   x.addparameter("> out")
            @param param The argument to be added."""
        self.command.append(str(param))

    def addinput(self, input):
        """Add a file (or directory) to the input list.
            Usage: x.addinput("infile")
            @param input The file or directory to be included as input."""
        self.inputlist.append(input)

    def addoutput(self, output):
        """Add a file (or directory) to the output list.
            Output will be placed in progname/parameters/
            Usage: a.addoutput("outfile")
            @param output The file to be recieved back from the worker as output."""
        self.outputlist.append(output)

    def addsweep(self, iterlist):
        """Add a sweep to the command.
            Usage: x.addsweep(xrange(1,10,2))
            @param iterlist An iterable object that contains the values to sweep over."""
        self.command.append("%s")
        self.paramvalues.append("%s")
        r = []
        for i in iterlist:
            r.append(i)
        self.sweeps.append(r)

    def sweep(self):
        """Sweep over the command."""
        try:
            self.q = WorkQueue(self.port)
        except:
            print "Work Queue init failed!"
            sys.exit(1)

        print "listening on port %d..." % self.q.port

        for item in itertools.product(*self.sweeps):
            command  = ' '.join(self.command) % (item) # create the command
            paramdir = '_'.join(self.paramvalues) % (item)

            os.system("mkdir -p %s/%s" % (self.progname, paramdir))

            if (self.envpath):
                env = open(self.envpath).read()
            else:
                env = ""

            script = """
                    %(env)s
                    %(command)s
                    """ % {'env': env, 'command': command}
            taskcommand = 'tcsh script.sh'

	    print command
            t = Task(taskcommand)
            t.specify_buffer(script, 'script.sh')

            for input in self.inputlist:
                t.specify_file(input, input, WORK_QUEUE_INPUT, cache=True)
            for output in self.outputlist:
                t.specify_file("%s/%s/%s" % (self.progname, paramdir, output), output, WORK_QUEUE_OUTPUT, cache=False)

            taskid = self.q.submit(t)
            print "submitted task (id# %d): %s" % (taskid, t.command)

        print "waiting for tasks to complete..."

        while not self.q.empty():
            t = self.q.wait(5)
            if t:
                print "task (id# %d) complete %s (return code %d)" % (t.id, t.command, t.return_status)

        print "all tasks complete!"
        sys.exit(0)
