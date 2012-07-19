# -*- coding: utf-8 -*-
# Copyright (C) 2012- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#
# This program provides a simple api to sweep though a range of parameters
# and store the output in progname/param/
#

from work_queue import *
import itertools, sys, os, re, hashlib, mmap, _mysql

class Sweeper:
    """Provides a simple api for a program to sweep through a range of parameters.
        Usage: import sweeper
               x = sweeper.Sweeper()"""
    def __init__(self, verbose=0):
        self.vb = verbose                   # turn on/off verbose mode
        self.port = WORK_QUEUE_RANDOM_PORT  # random, default is 9123
        self.progname = ""                  # the program to sweep with
        self.envpath = ""                   # path to a environment set up script
        self.paramvalues = []               # list of arguments e.g. -l -o 27
        self.command = []                   # list of the full command to be run
        self.sweeps = []                    # list of variables to sweep over
        self.inputlist = []                 # list of input files
        self.outputlist = []                # list of output files
        self.keys = []                      # list of keys

    def addprog(self, progname):
        """The program (sans parameters) to sweep with.
            Usage: x.addprog("echo")
            @param progname The base program that will be run."""
        self.command.append(progname)
        # remove the file extension so we can create a nice directory
        self.progname, sep, tail = progname.partition('.')
        if self.vb: print 'added program = %s' % (progname)

    def setenv(self, pathtoenv):
        """Set the path to a env script to set up an environment for the command to run in.
            Usage: x.setenv("env.sh")
            @param pathtoenv The path to a script to run first."""
        self.envpath = pathtoenv
        if self.vb: print 'added environment script = %s' % (pathtoenv)

    def setenvdir(self, pathtoenvdir, caching=2):
        """Specify a directory to be submitted that contains an environment for the worker to run in, a script will still have to set the env up.
            Usage: x.setenvdir('myenvdir')
                   x.setenv('env.sh') # set ups env on worker using myenvdir
            @param pathtoenvdir The path to the directory."""
        # TODO contained in the envdir should be a script that sets up the env, this needs to be added to the env script
        r = []
        r.append(pathtoenvdir)
        r.append(caching)
        self.inputlist.append(r)
        if self.vb: print 'added environment directory = %s caching = %s' % (pathtoenvdir, caching)

    def addtuple(self, flag, iterlist, key=''):
        """Add a flag/sweep pair to the command. This has the same functionality as addparameter() and addsweep() used together.
            Usage: x.addtuple("-n", xrange(1,10,2))
            @param flag The flag to be added to the command.
            @param iterlist An interable list object that contains the values to sweep over."""
        self.command.append(str(flag))
        self.command.append('%s')
        self.paramvalues.append('%s')
        self.sweeps.append(iterlist)
        if self.vb: print 'added tuple = %s sweep = %s' % (flag, iterlist)
        self.keys.append(key)
        if key is not '':
            if self.vb: print 'added key = %s' % (key)

    def addparameter(self, param):
        """Add a parameter to the list of parameters(arguments).
            Usage: x.addparameter("-n")
                   x.addsweep(xrange(1,10,2))
                   x.addparameter("> out")
            @param param The argument to be added."""
        self.command.append(str(param))
        if self.vb: print 'added parameter = %s' % (param)

    def addinput(self, input, caching=2):
        """Add a file (or directory) to the input list.
            Usage: x.addinput("infile")
            @param input The file or directory to be included as input."""
        r = []
        r.append(input)
        r.append(caching)
        r.append(self._checksum(input))
        self.inputlist.append(r)
        if self.vb: print 'added input = %s caching = %s' % (input, caching)

    def addoutput(self, output, caching=1):
        """Add a file (or directory) to the output list.
            Output will be placed in progname/parameters/
            Usage: a.addoutput("outfile")
            @param output The file to be recieved back from the worker as output."""
        r = []
        r.append(output)
        r.append(caching)
        self.outputlist.append(r)
        if self.vb: print 'added output = %s caching = %s' % (output, caching)

    def addsweep(self, iterlist, key=''):
        """Add a sweep to the command.
            Usage: x.addsweep(xrange(1,10,2))
            @param iterlist An iterable object that contains the values to sweep over."""
        self.command.append("%s")
        self.paramvalues.append("%s")
        self.sweeps.append(iterlist)
        if self.vb: print 'added sweep = %s' % (iterlist)
        self.keys.append(key)
        if key is not '':
            if self.vb: print 'added key = %s' % (key)

    def addmetadata(self, key, value, dbconn):
        dbconn.query("""INSERT INTO metadata VALUES (meta_id, \'%s\', \'%s\')""" % (key, value))

    def associatemetadata(self, cid, mid, dbconn):
        dbconn.query("""INSERT INTO cmdmeta VALUES (%d, %d)""" % (cid, mid))

    def sweep(self, interpreter='bash'):
        """Sweep over the command.
            @param interpreter The interpreter to run the script with."""
        try:
            self.q = WorkQueue(self.port)
        except:
            print "Work Queue init failed!"
            sys.exit(1)

        print "listening on port %d..." % self.q.port

        for item in itertools.product(*self.sweeps):
            # create the commad
            command  = ' '.join(self.command) % (item)
            if self.vb: print 'created command = %s' % (command)
            commdir = '_'.join(self.command) % (item)
            # we want to replace illegal file characters with _
            regex = re.compile('[:/" ()<>|?*]|(\\\)')
            commdir = regex.sub('_', commdir)
            
            # create progname/commdir, this is where the output will go ex. BuildMSM-sweep/command_with_underscores/Data
            os.system("mkdir -p %s-sweep/%s" % (self.progname, commdir))
            if self.vb: print 'created directory = %s-sweep/%s' % (self.progname, commdir)

            if (self.envpath): # if a env script was specified
                try:
                    env = open(self.envpath).read()
                except:
                    print 'error: could not open', self.envpath
                    print 'exiting'
                    sys.exit(1)
            else:
                env = ""

            # combine an env script and the command into one
            script = """%(env)s%(command)s\n""" % {'env': env, 'command': command}
            fo = open('%s-sweep/%s/%s-script' 
                     % (self.progname, commdir, commdir), 'w')
            fo.write(script)
            fo.close
            if self.vb: print 'created script = %s-sweep/%s/%s-script' % (self.progname, commdir, commdir)
            taskcommand = '%s script.sh' % (interpreter) # run with bash
            if self.vb: print 'created taskcommand = %s' % (taskcommand)

            t = Task(taskcommand)
            t.specify_buffer(script, 'script.sh')

            for input in self.inputlist:
                if input[1] == 2: # 2 means caching should be true
                    # the input is usually the same for all the commands
                    t.specify_file(os.path.abspath(input[0]), input[0], WORK_QUEUE_INPUT, cache=True)
                else:
                    t.specify_file(os.path.abspath(input[0]), input[0], WORK_QUEUE_INPUT, cache=False)
            for output in self.outputlist:
                # we want the output on the remote machine to go to progname-sweep/params/ on the local machine
                if output[1] == 2: # 2 means caching should be true
                    t.specify_file("%s-sweep/%s/%s" % (self.progname, commdir, output[0]), output[0], WORK_QUEUE_OUTPUT, cache=True)
                else:
                    t.specify_file("%s-sweep/%s/%s" % (self.progname, commdir, output[0]), output[0], WORK_QUEUE_OUTPUT, cache=False)

            taskid = self.q.submit(t)
            print "submitted task (id# %d): %s" % (taskid, t.command)

        print "waiting for tasks to complete..."

        while not self.q.empty():
            t = self.q.wait(5)
            if t:
                print "task (id# %d) complete %s (return code %d)" % (t.id, t.command, t.return_status)

        print "all tasks complete!"
        print 'output and a copy of the script run by each worker located in %s-sweep' % (self.progname)

    def sqldbsubmit(self, host, user,  dbname, pw, interpreter='bash'):
        """Submit the commands to a MyWorkQueue MySQL database
            Usage: x.sqldbsubmit('cvrl-sql.crc.nd.edu', 'ccl', 'ccltest', 'secret/mysql.pwd')
            @param host The MySQL host.
            @param user The MySQL user.
            @param dbname The name of the db
            @param pwfile A file containing the pw for the mysql server.
            @param interpreter The interpreter to run the script"""
        # create the db object
        db = _mysql.connect(host=host, user=user, db=dbname, passwd=pw)
        if self.vb: print 'connected to mysql server = %s user = %s using database = %s' % (host, user, dbname)
        # we want to replace all illegal file characters with _
        regex = re.compile('[:/" ()<>|?*]|(\\\)')
        for item in itertools.product(*self.sweeps):
            command  = ' '.join(self.command) % (item) # create the command
            if self.vb: print 'created command = %s' % (command)
            commdir = '_'.join(self.command) % (item)
            # replace illegal characters with _
            commdir = regex.sub('_', commdir)
            outputdir = '%s-sweep/%s/' % (self.progname, commdir)
            # this is the location of the script on the master
            localpath = os.path.abspath('%s-sweep/%s/%s-script' % (self.progname, commdir, commdir))
            # location of the script on the worker
            remotepath = '%s-script' % (commdir)
            dbcommand = '%s ./' % (interpreter)+remotepath
            if self._checkdup(dbcommand, db): 
                continue

            # create progname-sweep/commdir, this is where the output will go ex. BuildMSM-sweep/command_with_underscores/Data
            os.system("mkdir -p %s-sweep/%s" % (self.progname, commdir))
            if self.vb: print 'created directory = %s-sweep/%s' % (self.progname, commdir)

            if (self.envpath): # if a env script was specified
                try:
                    env = open(self.envpath).read()
                except:
                    print 'error: could not open', self.envpath
                    print 'exiting'
                    sys.exit(1)
            else:
                env = ""

            # combine the env script and the command into one
            script = """%(env)s%(command)s\n""" % {'env': env, 'command': command}
            fo = open(localpath, 'w')
            fo.write(script)
            fo.close()
            if self.vb: print 'created script = %s-sweep/%s/%s-script' % (self.progname, commdir, commdir)

            #INSERT INTO commands VALUES (command_id, username, personal_id, name, command, status, stdout)
            #INSERT INTO files VALUES (fileid, command_id, local_path, remote_path, type, flags, checksum)
            # add the command to the table
            db.query("""INSERT INTO %s.commands VALUES (command_id, \'%s\', personal_id, name, \'%s\', 2, stdout, env)""" 
                    % (dbname, user, dbcommand,))

            #TODO metadata
            for value,key in itertools.izip(item,self.keys):
                if key is not '':
                    self.addmetadata(key,value,db)
                    db.query("""INSERT INTO cmdmeta VALUES ((SELECT MAX(command_id) FROM commands LIMIT 1), (SELECT MAX(meta_id) from metadata LIMIT 1))""")

            
            # add script as input so it is sent to the worker - no caching
            # get the checksum of the input script (localpath)
            db.query("""INSERT INTO %s.files VALUES (fileid, command_id, \'%s\', \'%s\', 1, 1, \'%s\')""" 
                    % (dbname, localpath, remotepath, self._checksum(localpath)))
            # add the input files to the myworkqueue db
            for input in self.inputlist:
		        db.query("""INSERT INTO %s.files VALUES (fileid, command_id, \'%s\', \'%s\', 1, %s, \'%s\')""" 
                        % (dbname, os.path.abspath(input[0]), input[0], input[1], input[2]))
            # add the output files to the myworkqueue db
            for output in self.outputlist:
                db.query("""INSERT INTO %s.files VALUES (fileid, command_id, \'%s\', \'%s\', 2, %s, checksum)""" 
                        % (dbname, os.path.abspath(outputdir+output[0]), output[0], output[1]))
                # each command in the commands table has a unique command_id, this needs to be associated with the correct input/output files
                db.query("""UPDATE %s.files SET files.command_id=(SELECT MAX(command_id) FROM %s.commands WHERE command=\'%s\' LIMIT 1) WHERE files.command_id=0""" 
                        % (dbname, dbname, dbcommand))
    
    def _checksum(self, filename):
        #try:
        #    f = open(localpath)
        #    # this is faster but doesn't seem to work in ND afs space
        #    map = mmap.mmap(f.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
        #    return hashlib.sha1(map).hexdigest()
        #except:
        # slightly slower but always works
        #print filename
        if os.path.isdir(filename) == True:
            # it a dir, we have to hash slighly differently
            SHAhash = hashlib.sha1()
            if not os.path.exists(filename):
                return -1
            try:
                for root, dirs, files, in os.walk(filename):
                    for names in files:
                        filepath = os.path.join(root, names)
                        try:
                            f1 = open(filepath, 'rb')
                        except:
                            f1.close()
                            continue

                        while 1:
                            buf = f1.read(4096)
                            if not buf : break
                            SHAhash.update(hashlib.sha1(buf).hexdigest())
                        f1.close()
            except:
                import traceback
                traceback.print_exc()
                return -2

            return SHAhash.hexdigest()
        else:
            return hashlib.sha1(open(filename, 'rb').read()).hexdigest()

    def _checkdup(self, dbcommand, dbconn):
        dbconn.query("""SELECT command_id FROM commands WHERE command='%s'""" % (dbcommand)) 
        r = dbconn.store_result()
        cid = -1 # we want the command_id to be -1 if we can't find a duplicate
        # fetch_row returns a tuple, we only need the cid
        for tuple in r.fetch_row():
            for i in tuple:
                # if we found a duplicate
                cid = i

        # if we did not find a duplicate
        if cid < 0:
            if self.vb: print 'new command =', dbcommand
            return 0 # new command, input everything

        if self.vb: print 'duplicte command = %s cid = %s' % (dbcommand, cid)
        # find the files associated with this commnand_id
        dbconn.query("""SELECT remote_path,checksum FROM files WHERE command_id='%s'""" % (cid))
        r = dbconn.store_result()
        for file, hash in r.fetch_row(maxrows=0):
            for input in self.inputlist:
                if file in input and hash not in input:
                    if self.vb: print 'file changed', file, hash
                    #dbconn.query("""UPDATE commands SET status=2 WHERE command_id='%s'""" % (cid)) update command to Avaliable
                    return 0 # file changed, insert command anyway
        if self.vb: print 'files identical, doing nothing to command = %s cid = %s (no new command inserted)' % (dbcommand, cid)
        return 1
