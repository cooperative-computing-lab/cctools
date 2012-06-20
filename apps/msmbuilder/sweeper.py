from work_queue import *
import sys, os, itertools

class Sweeper:
    """A parameter sweeper"""
    def __init__(self):
        self.port = WORK_QUEUE_DEFAULT_PORT
        self.progname = ""
        self.envpath = ""
        self.paramvalues = []
        self.command = []
        self.sweeps = []
        self.inputlist = []
        self.outputlist = []

    def addprog(self, progname):
        self.progname = progname
        self.command.append(progname)

    def setenv(self, pathtoenv):
        self.envpath = pathtoenv

    def addparameter(self, param):
        self.command.append(str(param))

    def addinput(self, input):
        self.inputlist.append(input)

    def addoutput(self, output):
        self.outputlist.append(output)

    def addsweep(self, iterlist):
        self.command.append("%s")
        self.paramvalues.append("%s")
        r = []
        for i in iterlist:
            r.append(i)
        self.sweeps.append(r)

    def sweep(self):
        try:
            self.q = WorkQueue(self.port)
        except:
            print "Work Queue init failed!"
            sys.exit(1)

        print "listening on port %d..." % self.q.port

        os.system("mkdir %s" % (self.progname))

        for item in itertools.product(*self.sweeps):
            command  = ' '.join(self.command) % (item)
            paramdir = '_'.join(self.paramvalues) % (item)

            os.system("mkdir %s/%s" % (self.progname, paramdir))

            env = open(self.envpath).read()
            script = """
                    %(env)s
                    %(command)s
                    """ % {'env': env, 'command': command}
            taskcommand = 'bash script.sh'

            t = Task(taskcommand)
            t.specify_buffer(script, 'script.sh')

            for input in self.inputlist:
                t.specify_file(input, input, WORK_QUEUE_INPUT, cache=True)
            for output in self.outputlist:
                t.specify_file("%s/%s/%s" % (self.progname, paramdir, output), output, WORK_QUEUE_OUTPUT, cache=True)

            taskid = self.q.submit(t)
            print "submitted task (id# %d): %s" % (taskid, t.command)

        print "waiting for tasks to complete..."

        while not self.q.empty():
            t = self.q.wait(5)
            if t:
                print "task (id# %d) complete %s (return code %d)" % (t.id, t.command, t.return_status)

        print "all tasks complete!"
        sys.exit(0)
