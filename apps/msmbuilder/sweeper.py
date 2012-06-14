from work_queue import *
import sys
import math
import itertools

class Sweeper:
    """A parameter sweeper"""
    def __init__(self):
        self.port = WORK_QUEUE_DEFAULT_PORT
        self.command = []
        self.sweeps = []
        self.inputlist = []
        self.outputlist = []

    def frange2(self, start, stop, n):
        L = [0.0] * n
        nm1 = n - 1
        nmlinv = 1.0 / nm1
        for i in range(n):
            L[i] = nmlinv * (start*(nm1-i)+stop*i)
        return L

    def frange(self, start, stop = None, step = 1.):
        if stop is None:
            stop, start = start, 0.
        else:
            start = float(start)
        count = int(math.ceil(stop-start)/step)
        return (start+n*step for n in range(count))

    def addprog(self, progname):
        self.command.append(progname)

    def addparameter(self, param):
        self.command.append(str(param))

    def addinput(self, input):
        self.inputlist.append(input)

    def addoutput(self, output):
        self.outputlist.append(output)

    def addsweep(self, start, stop, step):
        self.command.append("%s")
        r = []
        for i in xrange(start, stop+step, step):
            r.append(i)
        self.sweeps.append(r)

    def sweep(self):
        try:
            self.q = WorkQueue(self.port)
        except:
            print "Work Queue init failed!"
            sys.exit(1)

        print "listening on port %d..." % self.q.port

        for item in itertools.product(*self.sweeps):
            command  = ' '.join(self.command) % (item)

            t = Task(command)
            for i in self.inputlist:
                t.specify_file(i, i, WORK_QUEUE_INPUT, cache=True)
            for i in self.outputlist:
                t.specify_file(i, i, WORK_QUEUE_OUTPUT, cache=True)

            taskid = self.q.submit(t)
            print "submitted task (id# %d): %s" % (taskid, t.command)

        print "waiting for tasks to complete..."

        while not self.q.empty():
            t = self.q.wait(5)
            if t:
                print "task (id# %d) complete %s (return code %d)" % (t.id, t.command, t.return_status)

        print "all tasks complete!"
        sys.exit(0)
