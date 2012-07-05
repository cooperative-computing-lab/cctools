# -*- coding: utf-8 -*-
# Copyright (C) 2012- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#
# This program provides a simple api to sweep though a range of parameters
# and store the output in progname/param/
#

from work_queue import *
import itertools, sys, os
from subprocess import *

class Sweeper:
    """Provides a simple api for a program to sweep through a range of parameters.
        Usage: import sweeper
               x = sweeper.Sweeper()"""
    def __init__(self):
        self.port = WORK_QUEUE_RANDOM_PORT  # random, default is 9123
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

    def addtuple(self, flag, iterlist):
        """Add a flag/sweep pair to the command. This has the same functionality as addparameter() and addsweep() used together.
            Usage: x.addtuple("-n", xrange(1,10,2))
            @param flag The flag to be added to the command.
            @param iterlist An interable list object that contains the values to sweep over."""
        self.command.append(str(flag))
        self.command.append('%s')
        self.paramvalues.append('%s')
        r = []
        for i in iterlist:
            r.append(i)
        self.sweeps.append(r)

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
            commdir = '_'.join(self.command) % (item)
            
            # create progname/commdir, this is where the output will go ex. BuildMSM-sweep/command_with_underscores/Data
            os.system("mkdir -p %s-sweep/%s" % (self.progname, commdir))

            if (self.envpath): # if a env script was specified
                env = open(self.envpath).read()
            else:
                env = ""

            script = """%(env)s%(command)s\n""" % {'env': env, 'command': command} # combine an env script and the command into one script
            fo = open('%s-sweep/%s/%s-script' % (self.progname, commdir, commdir), 'w')
            fo.write(script)
            fo.close
            taskcommand = 'tcsh script.sh' # run with tsch

            t = Task(taskcommand)
            t.specify_buffer(script, 'script.sh')

            for input in self.inputlist:
                t.specify_file(os.path.abspath(input), input, WORK_QUEUE_INPUT, cache=True) # cache the input since it is the same for all commands
            for output in self.outputlist:
                # we want the output on the local machine to go to progname-sweep/params/
                t.specify_file("%s-sweep/%s/%s" % (self.progname, commdir, output), output, WORK_QUEUE_OUTPUT, cache=False) # do not cache the output

            taskid = self.q.submit(t)
            print "submitted task (id# %d): %s" % (taskid, t.command)

        print "waiting for tasks to complete..."

        while not self.q.empty():
            t = self.q.wait(5)
            if t:
                print "task (id# %d) complete %s (return code %d)" % (t.id, t.command, t.return_status)

        print "all tasks complete!"
        print 'output and a copy of the script run by each worker located in %s-sweep' % (self.progname)
        sys.exit(0)

    def myfork (self, sqlscript, host, username, password):
        check_call(['/usr/bin/mysql', '-vvv', '--host='+host, '--user='+username, '--password='+password], stdin=sqlscript)
        """
        pid = os.fork()
        if pid == 0:
            #print('/usr/bin/mysql', ['mysql', '-vvv', '--host=', host, '--user=', username, '--password=', password])
            fd = os.open(sqlscript, 'r')
            os.dup2(fd, 0)
            os.execv('/usr/bin/mysql', ['mysql', '-vvv', '--host='+host, '--user='+username, '--password='+password])
        os.waitpid(pid, 0)
        """
        
        """if pid == 0:
            os.setsid()
            pid = os.fork()
            if pid > 0:
                os._exit(0)
            for i in range(0, 1025):
                os.close(i)
            os.open('/dev/null')
            os.open('FOOBAR', 'w')
            os.open('FOOBAR', 'w')
            os.execv('/usr/bin/mysql', ['mysql', '-vvv', '-h', host, '-u', username, '-p', password])
            os.exit(0)
            """

    def sqldbsubmit(self, host, user):
        """Submit the commands to a MyWorkQueue MySQL database
            @param host The MySQL host.
            @param user The MySQL user."""
        sqlscript = ""
        for item in itertools.product(*self.sweeps):
            #INSERT INTO commands VALUES (command_id, username, personal_id, name, command, status, stdout)
            #INSERT INTO files VALUES (fileid, command_id, local_path, remote_path, type, flags)
            command = ' '.join(self.command) % (item)
            sqlscript += 'INSERT INTO ccltest.commands VALUES (command_id, \'%s\', personal_id, name, \'%s\', 2, stdout);\n' % (user, command)

            # the input for each command generated by the sweeper should be the same
            for input in self.inputlist:
                sqlscript += 'INSERT INTO ccltest.files VALUES (fileid, command_id, \'%s\', \'%s\', 1, 2);\n' % (os.path.abspath(input), input)
            # the output if different for each command
            for output in self.outputlist:
                sqlscript += 'INSERT INTO ccltest.files VALUES (fileid, command_id, \'%s\', \'%s\', 2, 1);\n' % (os.path.abspath(output), output)
            sqlscript += '\n'

        print sqlscript
        fo = open('sqlscript', 'w')
        fo.write(sqlscript)
        fo.close
        #self.myfork(open('sqlscript'), host, user, open('secret/mysql.pwd').read().strip())
        #print 'mysql -vvv -h %s -u %s -p < sqlscript' % (host, user)
        os.system(('mysql -vvv -h %s -u %s -p < sqlscript') % (host, user))
        sys.exit(0)
