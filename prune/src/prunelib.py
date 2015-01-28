#!/usr/bin/env python2.6

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, traceback

import string, random, time, shutil, uuid, hashlib, re, threading

from work_queue import *

import subprocess
from subprocess import Popen, PIPE, call

import prunedb



HOME = os.path.expanduser("~")
CWD = os.getcwd()
ENVID = None
NEXTID = None
transfers = []
hadoop_data = False
concurrency = 0
terminate = False


if hadoop_data:
	cache_folder = '/users/pivie/prune_cache/'
else:
	cache_folder = '/pscratch/pivie/prune_cache/'
	try: 
		os.makedirs(cache_folder)
	except OSError:
		if not os.path.isdir(cache_folder):
			raise

sandbox_prefix = '/tmp/prune_sandbox/'
try: 
	os.makedirs(sandbox_prefix)
except OSError:
	if not os.path.isdir(sandbox_prefix):
		raise

def resetAll():
	if hadoop_data:
		p = subprocess.Popen(['hadoop','fs','-rmr',cache_folder], stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
		(stdout, stderr) = p.communicate()
		p_status = p.wait()
		print 'Data reset'
	else:
		p = subprocess.Popen(['rm','-rf',cache_folder], stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
		(stdout, stderr) = p.communicate()
		p_status = p.wait()
		if (len(stdout)+len(stderr))>0:
			print stdout,stderr
		print 'Data reset'

		try: 
			os.makedirs(cache_folder)
		except OSError:
			if not os.path.isdir(cache_folder):
				raise

	prunedb.database_truncate()
	


if not hadoop_data:
	try: 
		os.makedirs(cache_folder)
	except OSError:
		if not os.path.isdir(cache_folder):
			raise



def initialize(reset):
	global ENVID, NEXTID
	if reset:
		resetAll()
	prunedb.init()


	ENVID = prunedb.var_get('_ENV')
	if not ENVID:
		raise Exception('No Environments!')

def terminate_now():
	terminate = True





def putFile(line, filename, wait=True, pname=None):
	global transfers
	id = prunedb.file_ins(filename)
	cache_filename = cache_folder+str(id)

	if hadoop_data:
		location = 'hadoop'
		cmd = ['hadoop','fs','-copyFromLocal',filename,cache_filename]
	else:
		location = 'local_cache'
		cmd = ['cp',filename,cache_filename]
	p = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
	if wait:
		(stdout, stderr) = p.communicate()
		p_status = p.wait()
		prunedb.copy_ins(id, location, cache_filename)
		wq_check()
	else:
		transfers += [p,line,id,location,cache_filename]
	if pname:
		prunedb.var_set(pname,id)
	return id

def getFile(line, name, filename, wait=True):
	global transfers
	cache_filename = locate(name)
	id = cache_filename.split('/')[-1]
	
	if hadoop_data:
		location = 'hadoop'
		cmd = ['hadoop','fs','-copyToLocal',cache_filename,filename]
	else:
		location = 'local_cache'
		cmd = ['cp',cache_filename,filename]
	p = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
	if wait:
		(stdout, stderr) = p.communicate()
		p_status = p.wait()
		return p_status
	else:
		transfers += [p,line,id,location,cache_filename]
		return 0


# This was was designed to track the progress of non-blocking file transfers.
def transfer_status():
	global transfers
	lines = []
	i = cnt = 0
	while i < len(transfers):
		p = transfers[i]
		if p.poll() is None:
			line = transfers[i+1]
			lines.append(line)
			cnt += 1
		i += 5
	if cnt>0:
		lines[0:0] = [str(cnt)+' pending transfers:']
		lines.append('\n')
	return lines


def locate(name):
	id_str = prunedb.var_get(name)
	if not id_str:
		raise Exception('Not found in database.')
	copies = prunedb.copy_get(id_str)
	if len(copies)>0:
		return copies[0]['filename']
	return None


def isNameDone(name):
	start = time.time()
	id_str = prunedb.var_get(name)
	copies = prunedb.copy_get(id_str)
	if len(copies)==0:
		return False
	return True


def eval(expr, depth=0, expect=[]):
	function = name = None
	lparen = expr.find('(')
	assign = expr.find('=')
	nextpos = min(lparen,assign)
	if nextpos<0:
		nextpos = max(lparen,assign)
	
	if nextpos<=0: # No lparens or assignments
		if (depth==0):
			raise Exception('The expression must assign a name to the results: '+expr)
		args = expr.split(',')
		for a, arg in enumerate(args):
			res = prunedb.var_get(arg.strip())
			if res:
				args[a] = res
			elif len(expect)>a and expect[a].lower()=='file':
				raise Exception('Input file not found: %s'%(arg))
		return args
			
	elif nextpos==lparen: # Function invokation
		if (depth==0):
			raise Exception('You must assign a name to the function result(s).')
		function_name = expr[0:lparen].strip()
		function_id = prunedb.var_get(function_name)
		func = prunedb.function_get(function_id)
		output_ids = []
		in_types = ''
		if not func:

			# This needs to be updated to handle hadoop, etc. at some point.
			with open(cache_folder+str(function_id)) as f:
				out_name_ar = []
				for line in f:
					if line.startswith('#PRUNE_INPUTS'):
						in_types = line[14:-1]
						if in_types == 'File*':
							in_types = 'File'
							for i in range(0,40):
								in_types += ' File'
					elif line.startswith('#PRUNE_OUTPUT'):
						out_name_ar.append(line[14:-1])
			prunedb.function_ins(' '.join(out_name_ar), function_id, in_types)
			func = prunedb.function_get(function_id)


		out_name_ar = func['out_names'].split(' ')
		remain = expr[lparen+1:expr.rfind(')')]
		input_args = eval(remain,depth+1,in_types.split(' '))
		op_id,output_ids = prunedb.op_ins(len(out_name_ar), function_id, input_args, func['in_types'])
		prunedb.run_ins(op_id)
		return output_ids

	else: # Simple assignment
		names = expr[0:assign].strip()
		remain = expr[assign+1:]
		
		res = eval(remain,depth+1)
		args = names.split(',')
		for a, arg in enumerate(args):
			args[a] = arg.strip()
			prunedb.var_set(args[a],res[a])
		return res


def check_transfers():
	global transfers
	i = cnt = 0
	while i < len(transfers):
		p = transfers[i]

		if p.poll() is not None:
			# Use this to get the original line: line = transfers[i+1]
			id = transfers[i+2]
			location = transfers[i+3]
			cache_filename = transfers[i+4]
			prunedb.copy_ins(id, location, cache_filename)
			nextt = transfers[i+5:] if len(transfers)>(i+5) else []
			transfers = transfers[0:i] + nextt
		else:	
			i += 5





def env_name(name):
	global ENVID
	id_str = prunedb.var_get(name)
	prunedb.var_set('_ENV',id_str)
	ENVID = id_str

	

def getRuns(cnt):
	return prunedb.run_get_by_queue('Run',cnt)

def getQueueCounts():
	return prunedb.run_queue_cnts()

def getdb():
	return prunedb

def cmd_log(original,final,op_id=None):
	prunedb.cmd_ins(original,final,op_id)



def hashfile(fname, hasher=hashlib.sha1(), blocksize=65536):
    afile = open(fname, 'rb')
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()







wq = None
wq_task_cnt = 0
def useWQ(name):
	global wq, wq_task_cnt
	try:
		wq = WorkQueue(0)
	except Exception as e:
		raise Exception("Instantiation of Work Queue failed!")

	wq.specify_name(name)
	print "Work Queue master started on port %d with name '%s'..." % (wq.port,name)
	wq.specify_log("wq.log")
	#wq.set_bandwidth_limit('1250000000')
	cctools_debug_config_file("wq.debug")

	return wq_check()


def wq_task(op_id):
	global cache_folder
	op = prunedb.ops_get(op_id)
	ios = prunedb.ios_get(op_id)
	out_ids = []
	needed_files = []
	for io in ios:
		if io['pos']<0:
			out_ids += [io['file_id']]
		elif io['pos']==0:
			function_id = io['file_id']
		else:
			arg = io['file_id']
			res = prunedb.copy_get(arg)
			if len(res)==0:
				return None
			needed_files.append(arg)

	func = prunedb.function_get(function_id)
	out_files = func['out_names'].split(' ')
	
	command = "./prune_cmd %s > stdout.txt 2> stderr.txt" % ( op['in_str'] )
	t = Task(command)
	
	t.specify_file(cache_folder+str(function_id), 'prune_cmd', WORK_QUEUE_INPUT, cache=True)
	t.specify_file(cache_folder+str(function_id)+'.stdout', 'stdout.txt', WORK_QUEUE_OUTPUT, cache=False)
	t.specify_file(cache_folder+str(function_id)+'.stderr', 'stderr.txt', WORK_QUEUE_OUTPUT, cache=False)

	for i, filename in enumerate(out_files):
		t.specify_file(cache_folder+str(out_ids[i]), filename, WORK_QUEUE_OUTPUT, cache=False)
	for i, arg in enumerate(needed_files):
		t.specify_file(cache_folder+str(arg), './'+str(arg), WORK_QUEUE_INPUT, cache=True)
	
	return t

def wq_check():
	global wq, wq_task_cnt
	if wq:
		# Add new tasks first so that they are scheduled while waiting
		left = wq.hungry()
		if left>0:
			runs = getRuns(10000)
			for run in runs:
				if left <= 0:
					break
				t = wq_task(run['op_id'])
				if t:
					task_id = wq.submit(t)
					prunedb.run_upd(run['op_id'],'Running',task_id,'wq')
					wq_task_cnt += 1
					left -= 1

		if wq_task_cnt==0:
			return False
		else:
			# Wait for finished tasks (which also schedules new ones)
			t = wq.wait(1)
			while t: #Once there are no more tasks currently finished, return
				run = prunedb.run_get_by_task_id(t.id)
				op_id = run['op_id']
				ios = prunedb.ios_get(op_id)
				for io in ios:
					if io['pos']<0:
						pathname = cache_folder+str(io['file_id'])
						try:
							file_size = os.stat(pathname).st_size
							chksum = hashfile(pathname)
						except Exception as e:
							file_size = -1
							chksum = '?'
						prunedb.copy_ins(io['file_id'],'local_cache',pathname)
				prunedb.run_upd(run['id'],'Complete',t.return_status,'')
				wq_task_cnt -= 1

				t = wq.wait(1)
		return True
	return False






max_concurrency = 0
local_workers = []
def useLocal(concurrency):
	global local_workers, max_concurrency
	print 'Starting %i local workers'%concurrency
	max_concurrency = concurrency

	left = max_concurrency - len(local_workers)
	runs = getRuns(left)
	for run in runs:
		t = umbrella_task(run['op_id'])
		if t:
			print t
			p = subprocess.Popen(t['cmd'], stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
			local_workers.append( [t,p] )


def file_data_put(path,id):
	if hadoop_data:
		args = ['hadoop','fs','-copyFromLocal',path,cache_folder+str(id)]
		p = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
		(stdout, stderr) = p.communicate()
		p_status = p.wait()
	else:
		shutil.copy( path, cache_folder+id )


def file_data_get(id,path):
	if hadoop_data:
		args = ['hadoop','fs','-copyToLocal',cache_folder+str(id), path]
		p = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
		(stdout, stderr) = p.communicate()
		p_status = p.wait()
	else:
		shutil.copy( cache_folder+str(id), path )


def local_task(op_id):
	global cache_folder
	ios = prunedb.ios_get(op_id)
	in_args = []
	out_ids = []
	for io in ios:
		if io['pos']<0:
			out_ids += [io['file_id']]
		elif io['pos']==0:
			function_id = io['file_id']
		else:
			in_args += [io['file_id']]
	

	func = prunedb.function_get(function_id)
	out_files = func['out_names'].split(' ')
	
	needed_files = []
	input_files = []
	for i,ftype in enumerate(func['in_types'].split(' ')):
		if ftype.lower()=='file':
			input_files += [in_args[i]]
	for i, arg in enumerate(input_files):
		res = prunedb.copy_get(arg)
		if len(res)==0:
			return None
		else:
			needed_files.append(arg)

	command = ['./prune_cmd'] + in_args # Use this to debug: + ['>', 'stdout.txt', '2>', 'stderr.txt']
	
	sandbox_folder = sandbox_prefix+uuid.uuid4().hex+'/'
	if not os.path.isdir(sandbox_folder):
		try:
			os.mkdir(sandbox_folder)
		except OSError:
			if not os.path.isdir(sandbox_folder):
				raise
	
	file_data_get(function_id,sandbox_folder+'prune_cmd')

	copy_out = []
	# Use this to debug:copy_out.append( [sandbox_folder+'stdout.txt', function_id+'.stdout'] )
	# Use this to debug:copy_out.append( [sandbox_folder+'stderr.txt', function_id+'.stderr'] )

	for i, filename in enumerate(out_files):
		copy_out.append( [sandbox_folder+filename,out_ids[i]] )
	for i, arg in enumerate(needed_files):
		file_data_get(arg,sandbox_folder+arg)
	
	return {'sandbox':sandbox_folder, 'cmd':command, 'out':copy_out}

def umbrella_task(op_id):
	global cache_folder
	ios = prunedb.ios_get(op_id)
	in_args = []
	out_ids = []
	for io in ios:
		if io['pos']<0:
			out_ids += [io['file_id']]
		elif io['pos']==0:
			function_id = io['file_id']
		else:
			in_args += [io['file_id']]
	

	func = prunedb.function_get(function_id)
	out_files = func['out_names'].split(' ')
	
	needed_files = []
	input_files = []
	for i,ftype in enumerate(func['in_types'].split(' ')):
		if ftype.lower()=='file':
			input_files += [in_args[i]]
	for i, arg in enumerate(input_files):
		res = prunedb.copy_get(arg)
		if len(res)==0:
			return None
		else:
			needed_files.append(arg)

	command = ['./prune_cmd'] + in_args # Use this to debug:+ ['>', 'stdout.txt', '2>', 'stderr.txt']
	
	sandbox_folder = sandbox_prefix+uuid.uuid4().hex+'/'
	if not os.path.isdir(sandbox_folder):
		try:
			os.mkdir(sandbox_folder)
		except OSError:
			if not os.path.isdir(sandbox_folder):
				raise



	in_str = ''
	for arg in in_args:
		in_str += ','+arg+'='+arg


	file_data_get(ENVID, sandbox_folder+'/package.json')
	shutil.copy('../cctools/bin/umbrella', sandbox_folder+'/umbrella')

	file_data_get(function_id,sandbox_folder+'/prune_cmd')
	os.chmod(sandbox_folder+'/prune_cmd', 0755)
	# This worked at some point: cmd = './umbrella -T local -i "prune_function=%s%s" -c package.json -l ./tmp/ -o ./final_output run "/bin/bash prune_function %s"' \
	# This worked at some point: % (function_id, in_str, arg_str)
	command2 = ['./umbrella','-T','local','-i','prune_function='+function_id+in_str, '-c','package.json', '-l','./tmp/', '-o','./final_output', 'run', '/bin/bash %s'%' '.join(command) ]
	print ' '.join(command2)



	
	file_data_get(function_id,sandbox_folder+'prune_cmd')

	copy_out = []
	# Use this to debug: copy_out.append( [sandbox_folder+'stdout.txt', function_id+'.stdout'] )
	# Use this to debug: copy_out.append( [sandbox_folder+'stderr.txt', function_id+'.stderr'] )

	for i, filename in enumerate(out_files):
		copy_out.append( [sandbox_folder+'final_output/'+filename,out_ids[i]] )
	for i, arg in enumerate(needed_files):
		file_data_get(arg,sandbox_folder+arg)
	
	return {'sandbox':sandbox_folder, 'cmd':command2, 'out':copy_out}


def local_check():
	global local_workers
	w = 0
	while w<len(local_workers):
		(op,t,p) = local_workers[w]
		if p.poll() is not None:
			
			op_id = op['op_id']
			for cp in t['out']:
				(sb_filename,id) = cp
				file_data_put(sb_filename,id)
				pathname = cache_folder+str(id)
				try:
					file_size = os.stat(pathname).st_size
					chksum = hashfile(pathname)
				except Exception as e:
					file_size = -1
					chksum = '?'
				prunedb.copy_ins(id,'local_cache',pathname)
			prunedb.run_upd(op['op_id'],'Complete',p.returncode,'')
			del local_workers[w]
			w -= 1
		w += 1


	left = max_concurrency - len(local_workers)
	runs = getRuns(left)
	for run in runs:
		t = umbrella_task(run['op_id'])
		if t:
			p = subprocess.Popen(t['cmd'], stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd = t['sandbox'], shell=False)
			local_workers.append( [op,t,p] )
			prunedb.run_upd(run['op_id'],'Running',0,'local')







# Use the following code as a reference for getting umbrella to work
'''

def isFile(arg):
	file_objects = prunedb.files_get_by_id(arg)
	if file_objects and file_objects.__len__()>0:
		return True
	return False

def isFileChksum(chksum):
	file_objects = prunedb.files_get_by_chksum(chksum)
	if file_objects and file_objects.__len__()>0:
		return True
	return False




	
def runOps():
	umbrella = False
	global sandbox_prefix, concurrency, max_concurrency
	
	ops = prunedb.queue_get_by_status('Run',max_concurrency-concurrency)
	for op in ops:
		print op


		sandbox_folder = None
		while not sandbox_folder:
			sandbox_folder = sandbox_prefix+uuid.uuid4().hex
			#print 'Creating sandbox in '+sandbox_folder
			if os.path.isdir(sandbox_folder):
				sandbox_folder = None
			else:	
				try:
					os.mkdir(sandbox_folder)
				except OSError:
					if not os.path.isdir(sandbox_folder):
						raise


		
	
		newpid = os.fork()
		if newpid == 0:
			# Forked process executes the function
			run(op['op_id'],sandbox_folder)
		else:
			# Parent process has the pid of the forked process
			prunedb.queue_upd(op['id'],'RunningLocally',sandbox_folder,newpid)
			concurrency += 1

	try:
		(pid,exit_status,res) = os.wait3(os.WNOHANG)
		if pid:
			res2 = prunedb.queue_get_by_pid(pid)
			if res2 and len(res2)>0:
				op = res2[0]
				sandbox_folder = op['sandbox']
				
				ios = prunedb.ios_get(op['id'])
				in_args = []
				out_ids = []
				for io in ios:
					if io['pos']<0:
						out_ids += [io['file_id']]
					elif io['pos']==0:
						function_id = io['file_id']
					else:
						in_args += [io['file_id']]



				if umbrella:
					output_folder = '/final_output'
				else:
					output_folder = ''

				with open(sandbox_folder+'/prune_cmd') as f:
					i = 0
					for line in f.read().splitlines():
						if line.startswith('#PRUNE_'):
							parts = line.split(' ')
							if parts[0] == '#PRUNE_OUTPUT':
								filename = parts[1]

								if not os.path.isfile(sandbox_folder+filename):
									print "Operation #%i completed, but the output file %s does not exist. The sandbox is at: %s! Press any key when the file exists."%(op['id'],filename, sandbox_folder)
									sys.stdin.readline()
									if not os.path.isfile(sandbox_folder+filename):
										raise Exception('No File:',filename)
								if not os.stat(sandbox_folder+filename).st_size:
									print "Operation #%i completed, but the output file %s is empty. The sandbox is at: %s! Press any key when the file is not empty."%(op['id'],filename, sandbox_folder)
									sys.stdin.readline()
									if not os.stat(sandbox_folder+filename).st_size:
										raise Exception('Empty File:',filename)

								id = None
								if len(out_ids)>i:
									id = out_ids[i]
								newFile(sandbox_folder+output_folder+filename,'DATA',0,0,id)

								i += 1
							elif parts[0] == '#PRUNE_OUT_CNT':
								if parts[1][0] == '$':
									out_cnt = in_args[int(parts[1][1:])]
								else:
									out_cnt = int(parts[1])
								for i in range(0,out_cnt):
									id = None
									if len(out_ids)>i:
										id = out_ids[i]
									newFile(sandbox_folder+output_folder+'/prune.output.%i'%(i),'DATA',0,0,id)

						

				#print "Operation #%i completed. Press any key to clean up sandbox at %s!"%(op['id'],sandbox_folder)
				#sys.stdin.readline()
				shutil.rmtree(sandbox_folder)

				prunedb.queue_upd_status(op['id'],'Complete')
				prunedb.run_ins(op['id'],op['last_update'],res.ru_stime,res.ru_utime,exit_status)
				concurrency -= 1


		else:
			# No processes finished at this time
			#print '---', pid,status
			pass
		
	except Exception, e:
		if hasattr(e, 'errno') and e.errno==10:
			# No child processes (this is normal when when all tasks are finished)
			pass
		else:
			print e.message
			print e.__class__.__name__
			traceback.print_exc(e)



def run(op_id, sandbox_folder, use_umbrella=True):
	global ENVID
	ios = prunedb.ios_get(op_id)
	in_args = []
	out_ids = []
	for io in ios:
		if io['pos']<0:
			out_ids += [io['file_id']]
		elif io['pos']==0:
			function_id = io['file_id']
		else:
			in_args += [io['file_id']]
	#print out_ids, function_id, in_args
	if use_umbrella:
		shutil.copy(cache_folder+ENVID, sandbox_folder+'/package.json')
		shutil.copy('./umbrella', sandbox_folder+'/umbrella')

	file_data_get(function_id,sandbox_folder+'/prune_cmd')
	os.chmod(sandbox_folder+'/prune_cmd', 0755)

	in_str = ''
	for arg in in_args:
		if isFile(arg):
			#This filename could conflict with an existing file
			#Need to make sure the file is there too
			#shutil.copy(cache_folder+str(arg), sandbox_folder+arg)
			file_data_get(arg,sandbox_folder+arg)
			in_str += ','+arg+'='+arg


	arg_str = ''
	for in_arg in in_args:
		arg_str += ' '+in_arg
	##cmd = './umbrella -T local -i "prune_function=%s%s" -c package.json -l ./tmp/ -o ./final_output run "/bin/bash prune_function %s"' \
	##% (function_id, in_str, arg_str)
	if use_umbrella:
		args = ['./umbrella','-T','local','-i','prune_function='+function_id+in_str, '-c','package.json', '-l','./tmp/', '-o','./final_output', 'run','/bin/bash prune_cmd'+arg_str]
	else:
		args = ['./prune_cmd'] + in_args
	print 'Running: '+' '.join(args)
	print '...'
	start_time = time.time()
	env = {}
	for key in os.environ:
		env[key] = os.environ[key]

	#p = subprocess.Popen(['chmod','755','prune_cmd'], stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd=sandbox_folder, shell=False)
	#(stdout, stderr) = p.communicate()
	#sys.exit(p.wait())
	p = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd=sandbox_folder, shell=False)
	(stdout, stderr) = p.communicate()
	sys.exit(p.wait())




def cmdFiles(cmd_id):
	print prunedb.ios_get_by_cmd_id(cmd_id)
	return

	cmd_object = prunedb.cmd_get_by_id(chksum)
	if not cmd_object:
		prunedb.cmd_ins(chksum)


'''








