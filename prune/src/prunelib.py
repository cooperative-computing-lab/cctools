#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, traceback

import string, random, time, shutil, uuid, hashlib, re, threading

from work_queue import *

import subprocess
from subprocess import Popen, PIPE, call

import prunedb
import pruneexec


HOME = os.path.expanduser("~")
CWD = os.getcwd()
ENVIRONMENT = None
STORAGE_MODULE = None
transfers = []
concurrency = 0
terminate = False
data_folder = None
sandbox_prefix = None
hadoop_data = None


def truncate():
	global data_folder, sandbox_prefix, hadoop_data
	if hadoop_data:
		p = subprocess.Popen(['hadoop','fs','-rmr',data_folder], stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
		(stdout, stderr) = p.communicate()
		p_status = p.wait()
		print 'Data reset at:%s'%data_folder
	else:
		print data_folder
		p = subprocess.Popen(['rm','-rf',data_folder], stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
		(stdout, stderr) = p.communicate()
		p_status = p.wait()
		if (len(stdout)+len(stderr))>0:
			print stdout,stderr
		print 'Data reset at:%s'%data_folder
	p = subprocess.Popen(['rm','-rf',sandbox_prefix], stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
	(stdout, stderr) = p.communicate()
	p_status = p.wait()
	if (len(stdout)+len(stderr))>0:
		print stdout,stderr
	print 'Sandboxes erased at:%s'%sandbox_prefix
	initialize(data_folder,sandbox_prefix)



def initialize(new_data_folder, new_sandbox_prefix, hadoop=False):
	global ENVIRONMENT, data_folder, sandbox_prefix
	data_folder = new_data_folder
	sandbox_prefix = new_sandbox_prefix
	hadoop_data = hadoop

	if not hadoop:
		try: 
			os.makedirs(data_folder)
		except OSError:
			if not os.path.isdir(data_folder):
				raise
	try: 
		os.makedirs(sandbox_prefix)
	except OSError:
		if not os.path.isdir(sandbox_prefix):
			raise

	exec_script = open(data_folder+'PRUNE_EXECUTOR','w')
	exec_script.write( pruneexec.script_str() )
	exec_script.close()

	ENVIRONMENT = prunedb.var_get('_ENV')
	if not ENVIRONMENT:
		print 'Warning: No Environments defined!'





def terminate_now():
	terminate = True


def storage_pathname(guid):
	global data_folder
	return data_folder+str(guid)




def getDataIDs(filename, guid=None):
	try:
		chksum = hashfile(filename)
		copies = prunedb.copies_get_by_chksum(chksum)
		if len(copies)>0:
			guid = copies[0]['guid']
			return {'guid':guid, 'chksum':chksum, 'filename':filename, 'exists':True, 'repo_id':None}
		if not guid:
			guid = uuid.uuid4()
		return {'guid':guid, 'chksum':chksum, 'filename':filename, 'exists':False, 'repo_id':None}
	except IOError:
		return None
def getDataIDs2(data, guid=None):
	chksum = hashstring(data)
	copies = prunedb.copies_get_by_chksum(chksum)
	if len(copies)>0:
		guid = copies[0]['guid']
		return {'guid':guid, 'chksum':chksum, 'filename':None, 'exists':True, 'repo_id':None}
	if not guid:
		guid = uuid.uuid4()
	return {'guid':guid, 'chksum':chksum, 'filename':None, 'exists':False, 'repo_id':None}

def putMetaData(ids, cmd_id=None):
	global ENVIRONMENT
	guid = ids['guid']
	if ids['pname']:
		prunedb.var_set(ids['pname'], guid, ids['repo_id'])
	op_id = prunedb.op_ins(ENVIRONMENT, cmd_id)
	#prunedb.io_ins(op_id, 'F', function_guid, function_name)
	prunedb.io_ins(op_id, 'I', None, None, ids['repo_id'], ids['filename'], 0)
	prunedb.io_ins(op_id, 'O', guid, ids['pname'], ids['repo_id'], None, 0)
	return op_id
'''
def putData(filename, pname=None, wait=True, cmd_id=None, file_guid=None, repo_id=None):
	global transfers
	chksum = hashfile(filename)
	copies = prunedb.copies_get_by_chksum(chksum)
	if len(copies)>0 and not file_guid: #Use the new file_guid even if there are already copies
		file_guid = copies[0]['guid']
	else:
		if not file_guid:
			file_guid = uuid.uuid4()
		cache_filename = storage_pathname(file_guid)
		if cache_filename!=filename:
			p = transfer_file(filename, cache_filename, wait)
			if p:
				transfers += [p, file_guid, filename, cache_filename]
			else:
				file_meta_put(filename, file_guid)
	if pname:
		prunedb.var_set(pname, file_guid, repo_id)

	op_id = prunedb.op_ins(ENVIRONMENT, cmd_id)
	#prunedb.io_ins(op_id, 'F', function_guid, function_name)
	prunedb.io_ins(op_id, 'I', None, None, None, filename, 0)
	prunedb.io_ins(op_id, 'O', file_guid, pname, repo_id, None, 0)
	#prunedb.run_ins(op_id)

	return True

def file_meta_put(pathname, file_guid):
	global STORAGE_MODULE
	prunedb.copy_ins(file_guid, pathname, STORAGE_MODULE)
'''

def getFile(name, filename, wait=True, repo_id=None):
	global transfers
	while True:
		try:
			copies = locate_copies(name, repo_id)
			for copy in copies:
				restore_file(copy['guid'], filename, wait)
				return True
		except Exception as e:
			print e
		#time.sleep(1)
	return False


def store_file(filename, guid, wait=True, dtype='text', stype=None, storage_module=None):
	if not storage_module:
		if wait:
			cache_filename = storage_pathname(guid)
			#print cache_filename
			if filename:
				shutil.copy2( filename, cache_filename )
			length = os.stat(cache_filename).st_size
			chksum = hashfile(cache_filename)
			prunedb.copy_ins(guid, chksum, length, dtype, stype, storage_module)
			return True
		else:
			print 'LAZY PUT not yet implemented.'
			return False
	elif storage_module=='wq':
		cache_filename = storage_pathname(guid)
		length = os.stat(cache_filename).st_size
		chksum = hashfile(cache_filename)
		prunedb.copy_ins(guid, chksum, length, dtype, stype, storage_module)
		return True

	else:
		print 'Storage module %s not recognized.'%storage_module
		return False

def store_data(data, guid, wait=True, dtype='text', stype=None, storage_module=None):
	if not storage_module:
		if wait:
			cache_filename = storage_pathname(guid)
			print cache_filename
			with open(cache_filename,'w') as f:
				f.write(data.replace('\\n',"\n"))
			length = os.stat(cache_filename).st_size
			chksum = hashfile(cache_filename)
			prunedb.copy_ins(guid, chksum, length, dtype, stype, storage_module)
			return True
		else:
			print 'LAZY PUT not yet implemented.'
			return False
	elif storage_module=='wq':
		cache_filename = storage_pathname(guid)
		length = os.stat(cache_filename).st_size
		chksum = hashfile(cache_filename)
		prunedb.copy_ins(guid, chksum, length, dtype, None, storage_module)
		return True

	else:
		print 'Storage module %s not recognized.'%storage_module
		return False


def restore_file(guid, filename, wait=True, storage_module=None):
	if not storage_module:
		if wait:
			cache_filename = storage_pathname(guid)
			shutil.copy( cache_filename, filename )
			return True
		else:
			print 'Wait not yet implemented.'
			return False
	else:
		print 'Storage module %s not recognized.'%storage_module
		return False


# This was was designed to track the progress of non-blocking file transfers.
def transfers_status():
	global transfers
	lines = []
	i = cnt = 0
	while i < len(transfers):
		p = transfers[i]
		if p.poll() is None:
			line = transfers[i+1]
			lines.append(line)
			cnt += 1
		i += 4
	if cnt>0:
		lines[0:0] = [str(cnt)+' pending transfers:']
		lines.append('\n')
	return lines

def transfers_check():
	global transfers
	i = cnt = 0
	while i < len(transfers):
		p = transfers[i]

		if p.poll() is not None:
			# Use this to get the original line: line = transfers[i+1]
			guid = transfers[i+1]
			source = transfers[i+2]
			destination = transfers[i+3]
			prunedb.copy_ins(id, location, cache_filename)
			nextt = transfers[i+4:] if len(transfers)>(i+4) else []
			transfers = transfers[0:i] + nextt
		else:	
			i += 4



def locate_pathname(name, repo_id=None):
	global data_folder
	copies = locate_copies(name, repo_id)
	if len(copies)>0:
		return storage_pathname(files['guid'])
	return None

def locate_copies(name, repo_id=None):
	guid = prunedb.var_get(name, repo_id)
	if not guid:
		raise Exception('That name is not found in the database: '+name)
	return prunedb.copies_get(guid)


def isNameDone(name, repo_id=None):
	start = time.time()
	guid = prunedb.var_get(name, repo_id)
	copies = prunedb.copy_get(guid)
	if len(copies)==0:
		return False
	return True


def eval(expr, depth=0, cmd_id=None, extra={}):
	global data_folder, ENVIRONMENT
	function = name = None
	lparen = expr.find('(')
	assign = expr.find('=')
	nextpos = min(lparen,assign)
	in_types = extra['in_types'] if 'in_types' in extra else None
	repo_id = extra['repo_id'] if 'repo_id' in extra else None
	if nextpos<0:
		nextpos = max(lparen,assign)
	
	if nextpos<=0: # No lparens or assignments
		if (depth==0):
			print 'Unrecognized expression: '+expr
			print 'Try using caps for PRUNE keywords or provide a name for results with an equal sign.'
			return True
		args = expr.split(',')
		results = []
		for a, arg in enumerate(args):
			arg = arg.strip()
			ar = arg.split('@')
			name = None
			name = ar[0]
			if len(ar)>1:
				repo_name = ar[1]
				repo = prunedb.repo_get_by_name(repo_name)
				if repo:
					repo_id = repo['id']
			if len(ar)>2:
				time = ar[2]
			guid = prunedb.var_get(name,repo_id)
			if not guid and in_types and len(in_types)>a and in_types[a].lower()=='file':
				guid = name
				if 'args' in extra:
					name = extra['args'][a]
				else:
					print 'An argument does not exist'
					break
			if guid:
				results += [[guid,name,repo_id,None]]
			elif len(in_types)>a and in_types[a].lower()=='file':
				raise Exception('Input file not found: %s'%(arg))
			else:
				results += [[None, None, repo_id, arg]]
		return {'arg_list':results}
			
	elif nextpos==lparen: # Function invokation
		if (depth==0):
			raise Exception('You must assign a name to the function result(s).')
		if 'funcname' in extra:
			function_guid = expr[0:lparen].strip()
			function_name = extra['funcname']
		else:
			function_name = expr[0:lparen].strip()
			function_guid = prunedb.var_get(function_name, repo_id)
			if not function_guid:
				raise Exception('A function by the name "%s" could not be found.'%function_name)
		func = prunedb.function_get(function_guid)
		if not func:
			in_types = []
			fout_names = []
			with open( storage_pathname(function_guid) ) as f:
				for line in f:
					if line.startswith('#PRUNE_INPUTS'):
						in_types = line[14:-1].split()
						if in_types == 'File*':
							in_types = ['File']
							for i in range(0,40):
								in_types += ['File']
					elif line.startswith('#PRUNE_OUTPUT'):
						fout_names.append(line[14:-1])
			prunedb.function_ins(' '.join(fout_names), function_guid, ' '.join(in_types))
			func = prunedb.function_get(function_guid)
		else:
			in_types = func['in_types'].split()
			fout_names = func['out_names'].split()

		remain = expr[lparen+1:expr.rfind(')')]
		res = eval(remain, depth+1, cmd_id, dict({'in_types':in_types}.items()+extra.items()) )
		
		op_string = '%s('%function_guid
		for r in res['arg_list']:
			if r[0]:
				op_string += str(r[0])+','
			else:
				op_string += r[3]+','
		op_string = op_string[0:-1] + ')' + str(ENVIRONMENT)
		#op_chksum = hashstring(op_string)
		op_chksum = op_string
		op = prunedb.op_get_by_chksum(op_chksum)
		old_op_id = None
		if op:
			old_op_id = op['id']
			old_ios = prunedb.ios_get(old_op_id)
			print 'A matching operation has already been invoked: '+op_string
			prunedb.run_upd_by_op_id(op['id'], 'Run', -1, '', 'local')

		op_id = prunedb.op_ins(ENVIRONMENT, cmd_id, op_chksum)

		prunedb.io_ins(op_id, 'F', function_guid, function_name, repo_id)
		for i,arg in enumerate(res['arg_list']):
			prunedb.io_ins(op_id, 'I', arg[0], arg[1], arg[2], arg[3], i)
		
		results = []
		for i in range(0,len(fout_names)):
			if old_op_id:
				name = extra['assigns'][i] if len(extra['assigns'])>i else None
				for old_io in old_ios:
					if old_io['pos']==i:
						guid = old_io['file_guid']
				repo_id = None
			elif 'vars' in extra:
				name = extra['vars'][i] if len(extra['vars'])>i else None
				guid = extra['assigns'][i] if len(extra['assigns'])>i else uuid.uuid4() 
				repo_id = extra['repo_id']
			else:
				name = extra['assigns'][i] if len(extra['assigns'])>i else None
				prunedb.var_get
				guid = uuid.uuid4()
				repo_id = None
			results += [[guid,name,repo_id,None]]
			prunedb.io_ins(op_id, 'O', guid, name, repo_id, None, i)
			if name:
				prunedb.var_set(name,guid,repo_id)
		
		if not old_op_id:
			prunedb.run_ins(op_id)
		return results

	elif assign>0: # Simple assignment
		names = expr[0:assign].strip()
		remain = expr[assign+1:]
		
		args = names.split(',')
		for a,arg in enumerate(args):
			args[a] = arg.strip()
		if lparen>0:
			res = eval(remain, depth+1, cmd_id, dict({'assigns':args}.items()+extra.items()) )
			return res
		else:
			res = eval(remain, depth+1, cmd_id, extra)
			for a, arg in enumerate(args):
				prunedb.var_set(args[a], res['guids'][a], repo_id)
			return res

def make_command(ios):
	out_str = ''
	arg_str = ''
	function_name = ''
	for io in ios:
		if io['io_type']=='F':
			function_name += io['name'] if io['name'] else ''
			function_name += '@'+str(io['repo_id']) if io['repo_id'] else ''

		elif io['io_type']=='I':
			if io['literal']:
				arg_str += io['literal']
			else:
				arg_str += io['name'] if io['name'] else ''
				arg_str += '@'+str(io['repo_id']) if io['repo_id'] else ''
			arg_str += ', '
			#arg_str += '%s%s '%(io['name'],io['literal'])
		elif io['io_type']=='O':
			out_str += io['name'] if io['name'] else ''
			out_str += '@'+str(io['repo_id']) if io['repo_id'] else ''
			out_str += ', '
	name_line = "%s = %s( %s )"%(out_str[0:-2],function_name,arg_str[0:-2])

	out_str = ''
	arg_str = ''
	function_name = ''
	for io in ios:
		if io['io_type']=='F':
			function_name = str(io['file_guid'])

		elif io['io_type']=='I':
			if io['literal']:
				arg_str += io['literal']
			else:
				arg_str += str(io['file_guid']) if io['file_guid'] else ''
			arg_str += ', '
			#arg_str += '%s%s '%(io['name'],io['literal'])
		elif io['io_type']=='O':
			out_str += str(io['file_guid']) if io['file_guid'] else ''
			out_str += ', '
	preserved_line = "%s = %s( %s )"%(out_str[0:-2],function_name,arg_str[0:-2])
	return ['#'+name_line,preserved_line]


def origin(name, repo_id=None):
	lines = []
	guid = prunedb.var_get(name, repo_id)
	if not guid:
		raise Exception('An object by the name "%s@%s" could not be found.'%(name,repo_id) )
	file_guids = [guid]
	file_names = [name]
	necessary_files = []
	next_file = 0
	while next_file < len(file_guids):
		guid = file_guids[next_file]
		outs = prunedb.ios_get_by_file_guid(guid,'O')
		for out in outs:
			ios = prunedb.ios_get(out['op_id'])
			function_used = False
			for io in ios:
				if io['io_type']=='F':
					file_guids += [io['file_guid']]
					file_name = io['name']
					file_name += io['repo_id'] if io['repo_id'] else ''
					file_names += [file_name]
					function_used = True
				elif io['io_type']=='I':
					if io['file_guid']:
						file_guids += [io['file_guid']]
						file_name = io['name']
						file_name += io['repo_id'] if io['repo_id'] else ''
						file_names += [file_name]
			if function_used:
				lines += reversed(make_command(ios))
			else:
				new_name = file_names[next_file]



				#new_name += '@'+str(io['repo_id']) if io['repo_id'] else ''
				


				lines += ['PUT %s AS %s'%(guid, new_name)]
				necessary_files += [storage_pathname(guid)]

		next_file += 1
	return lines[::-1], necessary_files
					


def new_guid():
	return uuid.uuid4()

def env_name(name):
	global ENVIRONMENT
	id_str = prunedb.var_get(name)
	prunedb.var_set('_ENV',id_str)
	ENVIRONMENT = id_str

def add_store(fold_filename, unfold_filename):
	global STOREID
	fold_file_id = file_put(fold_filename)
	unfold_file_id = file_put(unfold_filename)
	STOREID = prunedb.store_ins(fold_file_id, unfold_file_id)
	

def getRuns(cnt):
	return prunedb.run_get_by_queue('Run',cnt)

def getQueueCounts():
	return prunedb.run_queue_cnts()

def getdb():
	return prunedb

def cmd_log(original,final,op_id=None):
	prunedb.cmd_ins(original,final,op_id)



def hashfile(fname, blocksize=65536):
    key = hashlib.sha1()
    afile = open(fname, 'rb')
    buf = afile.read(blocksize)
    while len(buf) > 0:
        key.update(buf)
        buf = afile.read(blocksize)
    return key.hexdigest()

def hashstring(str):
    key = hashlib.sha1()
    key.update(str)
    return key.hexdigest()







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
	cctools_debug_flags_set("all")
	cctools_debug_config_file("wq.debug")

	return True
	

def wq_check():
	global wq, wq_task_cnt, data_folder
	if wq:
		# Add new tasks first so that they are scheduled while waiting
		left = wq.hungry()
		#print 'left',left
		if left>0:
			runs = getRuns(10000)
			#print 'returned runs:',len(runs)
			for run in runs:
				#print 'run:',run
				if left <= 0:
					break
				operation = create_operation(run['op_id'],'wq')
				if operation['cmd']:
					t = operation['wq_task']
					task_id = wq.submit(t)
					prunedb.run_upd(run['id'],'Running',task_id,'','wq')
					wq_task_cnt += 1
					left -= 1
					print 'started: #%i, run:%i, op_id:%i   cmd:"%s"'%(wq_task_cnt, run['id'], run['op_id'], operation['cmd'])
				else:
					'No task'

		if wq_task_cnt==0:
			return False
		else:
			# Wait for finished tasks (which also schedules new ones)
			t = wq.wait(5)
			while t: #Once there are no more tasks currently finished, return
				try:
					run = prunedb.run_get_by_task_id(t.id)
					op_id = run['op_id']
					ios = prunedb.ios_get(op_id)
					if t.return_status==0:
						for io in ios:
							if io['io_type']=='O':
								#print 'io:',io
								pathname = storage_pathname(io['file_guid'])
								store_file(pathname, io['file_guid'], True, storage_module='wq')

						print 'complete: run:%i, op_id:%i'%(run['id'],run['op_id'])
						prunedb.run_end(run['id'],t.return_status)
					else:
						for io in ios:
							if io['io_type']=='O':
								try:
									print 'io:',io
									pathname = storage_pathname(io['file_guid'])
									store_file(pathname, io['file_guid'], True, storage_module='wq')
								except:
									pass
						print 'Failed with exit code:',t.return_status
						print 'Resubmit to try again.'
						#print run
						#print ios
						print t.command
						prunedb.run_upd(run['id'],'Failed',0,'','Exit code: %i'%t.return_status)

				except:
					print 'Return status:',t.return_status
					print run
					print ios
					print traceback.format_exc()
					prunedb.run_upd(run['id'],'Failed',0,'',traceback.format_exc())


				wq_task_cnt -= 1
				t = wq.wait(1)
		return True
	#print 'No WQ'
	return False



def create_operation(op_id,framework='local',local_fs=False):
	global sandbox_prefix

	ios = prunedb.ios_get(op_id)
	op = prunedb.op_get(op_id)

	options = ''
	arg_str = ''
	place_files = []
	fetch_files = []
	for io in ios:
		if io['io_type']=='F':
			function_name = io['name']
			function_guid = io['file_guid']
			func = prunedb.function_get(function_guid)
			out_names = func['out_names'].split()
			place_files.append({'src':storage_pathname(function_guid),'dst':function_name})
	
		elif io['io_type']=='I':
			if io['literal']:
				arg = io['literal']
				arg_str += '%s '%(arg)
			else:
				arg = io['name']
				#arg_str += '%s '%(storage_pathname(io['file_guid']))
				arg_str += '%s '%(arg)
				res = prunedb.copies_get(io['file_guid'])
				if len(res)==0:
					#print 'No file', io
					return {'cmd':None}
				place_files.append({'src':storage_pathname(io['file_guid']),'dst':str(arg)})
			

		elif io['io_type']=='O':
			fetch_files.append({'src':out_names[io['pos']],'dst':storage_pathname(io['file_guid']),'guid':io['file_guid']})

	arg_str = arg_str[0:-1]

	copies = prunedb.copies_get(op['env_guid'])
	env_type = ''
	if len(copies)>0:
		copy = copies[0]
		if copy['dtype']=='umbrella':
			env_type = copy['dtype']
			options += ' --umbrella'
		elif copy['dtype']=='targz':
			options += ' --targz ENVIRONMENT'
			env_type = copy['dtype']
			
		
		place_files.append({'src':storage_pathname(op['env_guid']),'dst':'ENVIRONMENT'})

	place_files.append({'src':storage_pathname('PRUNE_EXECUTOR'),'dst':'PRUNE_EXECUTOR'})
	fetch_files.append({'src':'prune_debug.log','dst':storage_pathname(op_id)+'.debug','guid':str(op_id)+'.debug'})

	if local_fs:
		for obj in place_files:
			options += ' --ln %s=%s'%(obj['dst'],obj['src'])

	
	final_cmd = 'chmod 755 PRUNE_RUN; ./PRUNE_RUN'
	run_cmd = "./%s %s"%(function_name, arg_str)
	run_buffer = '#!/bin/bash\npython PRUNE_EXECUTOR%s %s'%(options,run_cmd)
	

	if framework=='wq':
		t = Task('')
		for obj in place_files:
			t.specify_file(obj['src'], obj['dst'], WORK_QUEUE_INPUT, cache=True)
		t.specify_buffer(run_buffer, 'PRUNE_RUN', WORK_QUEUE_INPUT, cache=True)
	
		t.specify_command(final_cmd)
		t.specify_cores(1)

		for obj in fetch_files:
			if obj['src']=='prune_debug.log':
				t.specify_file(obj['dst'], obj['src'], WORK_QUEUE_OUTPUT, cache=False)
			else:
				print obj
				t.specify_file(obj['dst'], obj['src'], WORK_QUEUE_OUTPUT, cache=True)
		return {'cmd':run_cmd,'framework':framework,'wq_task':t}
	
	else:
		sandbox_folder = sandbox_prefix+uuid.uuid4().hex+'/'
		try:
			os.mkdir(sandbox_folder)
		except OSError:
			if not os.path.isdir(sandbox_folder):
				raise
		if not local_fs:
			for obj in place_files:
				print obj
				shutil.copy2(obj['src'], sandbox_folder+obj['dst'])
		f = open(sandbox_folder+'PRUNE_RUN','w')
		f.write(run_buffer)
		f.close()

		if env_type=='umbrella':
			in_str = ''
			for obj in place_files:
				in_str += ',%s=%s'%(obj['dst'],obj['dst'])
			final_cmd = './umbrella -T local -i PRUNE_RUN=PRUNE_RUN%s -c ENVIRONMENT -l ./tmp/ -o ./final_output run ./PRUNE_RUN'%(in_str)
			print final_cmd
			shutil.copy2('../../../cctools.src/umbrella/src/umbrella', sandbox_folder+'umbrella')

		return {'cmd':run_cmd,'final_cmd':final_cmd,'framework':framework,'sandbox':sandbox_folder,'fetch_files':fetch_files}





max_concurrency = 0
local_workers = []
def useLocal(concurrency):
	global local_workers, max_concurrency
	print 'Allocating %i local workers. Don\'t forget to enter the WORK command to start execution.'%concurrency
	max_concurrency = concurrency
	return True


def local_task(op_id):
	global sandbox_prefix

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

	#command = ['./prune_cmd'] + in_args # Use this to debug: + ['>', 'stdout.txt', '2>', 'stderr.txt']
	command = ['./prune_cmd'] + in_args + ['>', 'stdout.txt', '2>', 'stderr.txt']
	
	sandbox_folder = sandbox_prefix+uuid.uuid4().hex+'/'
	if not os.path.isdir(sandbox_folder):
		try:
			os.mkdir(sandbox_folder)
		except OSError:
			if not os.path.isdir(sandbox_folder):
				raise
	
	file_data_get(function_id,sandbox_folder+'prune_cmd')

	copy_out = []
	# Use this to debug:
	copy_out.append( [sandbox_folder+'stdout.txt', function_id+'.stdout'] )
	# Use this to debug:
	copy_out.append( [sandbox_folder+'stderr.txt', function_id+'.stderr'] )

	for i, filename in enumerate(out_files):
		copy_out.append( [sandbox_folder+filename,out_ids[i]] )
	for i, arg in enumerate(needed_files):
		file_data_get(arg,sandbox_folder+arg)
	
	return {'sandbox':sandbox_folder, 'cmd':command, 'out':copy_out}



def umbrella_task(op_id):
	global data_folder, sandbox_prefix, ENVIRONMENT
	
	sandbox_folder = sandbox_prefix+uuid.uuid4().hex+'/'
	if not os.path.isdir(sandbox_folder):
		try:
			os.mkdir(sandbox_folder)
		except OSError:
			if not os.path.isdir(sandbox_folder):
				raise
	t = {'sandbox':sandbox_folder}

	shutil.copy(storage_pathname(ENVIRONMENT), sandbox_folder+'package.json')

	ios = prunedb.ios_get(op_id)
	arg_str = ''
	in_str = ''
	t['out'] = []
	for io in ios:
		if io['io_type']=='F':
			function_name = io['name']
			function_guid = io['file_guid']
			t['func'] = func = prunedb.function_get(function_guid)
			out_names = func['out_names'].split()
			shutil.copy(storage_pathname(function_guid), sandbox_folder+'prune_cmd')
			os.chmod(sandbox_folder+'prune_cmd', 0755)

	
		elif io['io_type']=='I':
			if io['literal']:
				arg_str += '%s '%(io['literal'])
			else:
				arg = io['name']
				arg_str += '%s '%(io['file_guid'])
				res = prunedb.copies_get(io['file_guid'])
				if len(res)==0:
					shutil.rmtree(sandbox_folder)
					#print 'No file yet:',io
					return None
				shutil.copy(storage_pathname(io['file_guid']), sandbox_folder+str(io['file_guid']) )
				in_str += ','+str(io['file_guid'])+'='+str(io['file_guid'])
		elif io['io_type']=='O':
			t['out'] += [io]
			#print storage_pathname(io['file_guid']), out_names[io['pos']]
	arg_str = arg_str[0:-1]
	command = './prune_cmd',arg_str

	shutil.copy('../../../cctools.src/umbrella/src/umbrella', sandbox_folder+'umbrella')

	t['cmd'] = ['./umbrella','-T','local','-i','prune_cmd=prune_cmd'+in_str, '-c','package.json', '-l','./tmp/', '-o','./final_output', 'run', '%s'%' '.join(command) ]
	
	f = open(sandbox_folder+'cmd.sh','w')
	f.write( ' '.join(t['cmd']) )
	f.close()
	os.chmod(sandbox_folder+'cmd.sh', 0755)
	return t
	


def local_check():
	global local_workers, data_folder
	w = 0

	if max_concurrency<=0:
		return True

	while w<len(local_workers):
		(op_id,operation,p) = local_workers[w]
		if p.poll() is not None:
			print 'Returned: %i (%s) '%(p.returncode,operation['cmd'])
			(stdout, stderr) = p.communicate()
			#print stdout
			#print stderr

			fails = 0
			for obj in operation['fetch_files']:
				try:
					#shutil.copy2(operation['sandbox']+obj['src'],obj['dst'])
					store_file(operation['sandbox']+obj['src'], obj['guid'])
				except:
					fails += 1

			if p.returncode==0 and fails==0:
				prunedb.run_end(operation['run']['id'],p.returncode)
				shutil.rmtree(operation['sandbox'])
			else:
				print 'returncode =', p.returncode, ', fails =',fails
				print traceback.format_exc()
				prunedb.run_upd(operation['run']['id'], 'Failed', p.returncode, '', 'local')
			del local_workers[w]
			w -= 1
		w += 1


	left = max_concurrency - len(local_workers)
	runs = getRuns(left)
	for run in runs:
		operation = create_operation(run['op_id'],'local',False)
		if operation['cmd']:
			operation['run'] = run
			print 'Start:',operation['cmd']
			w += 1
			p = subprocess.Popen(operation['final_cmd'], stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd = operation['sandbox'], shell=True)
			local_workers.append( [run['op_id'],operation,p] )
			#prunedb.run_upd(run['id'],'Running',task_id,'','wq')
			prunedb.run_upd(run['id'],'Running',0,'','local')
	time.sleep(1)

	if w > 0:
		return True
	else:
		return False




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
	global ENVIRONMENT
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
		shutil.copy(cache_folder+ENVIRONMENT, sandbox_folder+'/package.json')
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









