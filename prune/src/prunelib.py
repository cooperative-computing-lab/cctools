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


def storage_pathname(puid):
	global data_folder
	return data_folder+str(puid)


def getPackType(filename):
	p = subprocess.Popen(['file',filename], stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=False)
	(stdout, stderr) = p.communicate()
	p_status = p.wait()
	if p_status==0:
		try:
			if stdout.split()[1]=='gzip':
				return 'gzip'
		except:
			pass
	return ''

def getDataIDs(filename, puid=None):
	try:
		chksum = hashfile(filename)
		copies = prunedb.copies_get_by_chksum(chksum)
		pack = getPackType(filename)
		if len(copies)>0:
			puid = copies[0]['puid']
			return {'puid':puid, 'chksum':chksum, 'filename':filename, 'exists':True, 'pack':pack}
		if not puid:
			puid = uuid.uuid4()
		return {'puid':puid, 'chksum':chksum, 'filename':filename, 'exists':False, 'pack':pack}
	except IOError:
		return None
def getDataIDs2(data, puid=None):
	chksum = hashstring(data)
	copies = prunedb.copies_get_by_chksum(chksum)
	if len(copies)>0:
		puid = copies[0]['puid']
		return {'puid':puid, 'chksum':chksum, 'filename':None, 'exists':True, 'pack':''}
	if not puid:
		puid = uuid.uuid4()
	return {'puid':puid, 'chksum':chksum, 'filename':None, 'exists':False, 'pack':''}

def putMetaData(ids, cmd_id=None):
	global ENVIRONMENT
	puid = ids['puid']
	if ids['pname']:
		prunedb.var_set(ids['pname'], puid)
	op_id = prunedb.op_ins(ENVIRONMENT, cmd_id)
	#prunedb.io_ins(op_id, 'F', function_puid, function_name)
	prunedb.io_ins(op_id, 'I', None, None, ids['filename'], 0)
	prunedb.io_ins(op_id, 'O', puid, ids['pname'], None, 0)
	return op_id


def getFile(name, filename, wait=True):
	global transfers
	while True:
		try:
			copies = locate_copies(name)
			for copy in copies:
				restore_file(copy['puid'], filename, wait)
				return True
		except Exception as e:
			print e
		#time.sleep(1)
	return False


def store_file(filename, puid, wait=True, form='ascii', pack=None, storage_module=None):
	print filename, puid, wait, form, pack, storage_module
	if not storage_module:
		if wait:
			cache_filename = storage_pathname(puid)
			#print cache_filename
			if filename:
				shutil.copy2( filename, cache_filename )
			length = os.stat(cache_filename).st_size
			chksum = hashfile(cache_filename)
			prunedb.copy_ins(puid, chksum, length, form, pack, storage_module)
			return True
		else:
			print 'LAZY PUT not yet implemented.'
			return False
	elif storage_module=='wq':
		cache_filename = storage_pathname(puid)
		length = os.stat(cache_filename).st_size
		chksum = hashfile(cache_filename)
		prunedb.copy_ins(puid, chksum, length, form, pack, storage_module)
		return True

	else:
		print 'Storage module %s not recognized.'%storage_module
		return False

def store_data(data, puid, wait=True, form='ascii', pack=None, storage_module=None):
	if not storage_module:
		if wait:
			cache_filename = storage_pathname(puid)
			print cache_filename
			with open(cache_filename,'w') as f:
				f.write(data.replace('\\n',"\n"))
			length = os.stat(cache_filename).st_size
			chksum = hashfile(cache_filename)
			prunedb.copy_ins(puid, chksum, length, form, pack, storage_module)
			return True
		else:
			print 'LAZY PUT not yet implemented.'
			return False
	elif storage_module=='wq':
		cache_filename = storage_pathname(puid)
		length = os.stat(cache_filename).st_size
		chksum = hashfile(cache_filename)
		prunedb.copy_ins(puid, chksum, length, form, None, storage_module)
		return True

	else:
		print 'Storage module %s not recognized.'%storage_module
		return False


def restore_file(puid, filename, wait=True, storage_module=None):
	if not storage_module:
		if wait:
			cache_filename = storage_pathname(puid)
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
			puid = transfers[i+1]
			source = transfers[i+2]
			destination = transfers[i+3]
			prunedb.copy_ins(id, location, cache_filename)
			nextt = transfers[i+4:] if len(transfers)>(i+4) else []
			transfers = transfers[0:i] + nextt
		else:	
			i += 4



def locate_pathname(name):
	global data_folder
	copies = locate_copies(name)
	if len(copies)>0:
		return storage_pathname(files['puid'])
	return None

def locate_copies(name):
	puid = prunedb.var_get(name)
	if not puid:
		raise Exception('That name is not found in the database: '+name)
	return prunedb.copies_get(puid)


def isNameDone(name):
	start = time.time()
	puid = prunedb.var_get(name)
	copies = prunedb.copy_get(puid)
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
			if len(ar)>2:
				time = ar[2]
			puid = prunedb.var_get(name)
			if not puid and in_types and len(in_types)>a and in_types[a].lower()=='file':
				puid = name
				if 'args' in extra:
					name = extra['args'][a]
				else:
					print 'An argument does not exist'
					break
			if puid:
				results += [[puid,name,None]]
			elif len(in_types)>a and in_types[a].lower()=='file':
				raise Exception('Input file not found: %s'%(arg))
			else:
				results += [[None, None, arg]]
		return {'arg_list':results}
			
	elif nextpos==lparen: # Function invokation
		if (depth==0):
			raise Exception('You must assign a name to the function result(s).')
		if 'funcname' in extra:
			function_puid = expr[0:lparen].strip()
			function_name = extra['funcname']
		else:
			function_name = expr[0:lparen].strip()
			function_puid = prunedb.var_get(function_name)
			if not function_puid:
				raise Exception('A function by the name "%s" could not be found.'%function_name)
		func = prunedb.function_get(function_puid)
		if not func:
			in_types = []
			fout_names = []
			with open( storage_pathname(function_puid) ) as f:
				for line in f:
					if line.startswith('#PRUNE_INPUTS'):
						in_types = line[14:-1].split()
						if in_types == 'File*':
							in_types = ['File']
							for i in range(0,40):
								in_types += ['File']
					elif line.startswith('#PRUNE_OUTPUT'):
						fout_names.append(line[14:-1])
			prunedb.function_ins(' '.join(fout_names), function_puid, ' '.join(in_types))
			func = prunedb.function_get(function_puid)
		else:
			in_types = func['in_types'].split()
			fout_names = func['out_names'].split()

		remain = expr[lparen+1:expr.rfind(')')]
		res = eval(remain, depth+1, cmd_id, dict({'in_types':in_types}.items()+extra.items()) )
		
		op_string = '%s('%function_puid
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

		prunedb.io_ins(op_id, 'F', function_puid, function_name)
		for i,arg in enumerate(res['arg_list']):
			prunedb.io_ins(op_id, 'I', arg[0], arg[1], arg[2], i)
		
		results = []
		for i in range(0,len(fout_names)):
			if old_op_id:
				name = extra['assigns'][i] if len(extra['assigns'])>i else None
				for old_io in old_ios:
					if old_io['pos']==i:
						puid = old_io['file_puid']
			elif 'vars' in extra:
				name = extra['vars'][i] if len(extra['vars'])>i else None
				puid = extra['assigns'][i] if len(extra['assigns'])>i else uuid.uuid4() 
			else:
				name = extra['assigns'][i] if len(extra['assigns'])>i else None
				prunedb.var_get
				puid = uuid.uuid4()
			results += [[puid,name,None]]
			prunedb.io_ins(op_id, 'O', puid, name, None, i)
			if name:
				prunedb.var_set(name,puid)
		
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
				prunedb.var_set(args[a], res['puids'][a])
			return res

def make_command(ios):
	out_str = ''
	arg_str = ''
	function_name = ''
	for io in ios:
		if io['io_type']=='F':
			function_name += io['name'] if io['name'] else ''

		elif io['io_type']=='I':
			if io['literal']:
				arg_str += io['literal']
			else:
				arg_str += io['name'] if io['name'] else ''
			arg_str += ', '
			#arg_str += '%s%s '%(io['name'],io['literal'])
		elif io['io_type']=='O':
			out_str += io['name'] if io['name'] else ''
			out_str += ', '
	name_line = "%s = %s( %s )"%(out_str[0:-2],function_name,arg_str[0:-2])

	out_str = ''
	arg_str = ''
	function_name = ''
	for io in ios:
		if io['io_type']=='F':
			function_name = str(io['file_puid'])

		elif io['io_type']=='I':
			if io['literal']:
				arg_str += io['literal']
			else:
				arg_str += str(io['file_puid']) if io['file_puid'] else ''
			arg_str += ', '
			#arg_str += '%s%s '%(io['name'],io['literal'])
		elif io['io_type']=='O':
			out_str += str(io['file_puid']) if io['file_puid'] else ''
			out_str += ', '
	preserved_line = "%s = %s( %s )"%(out_str[0:-2],function_name,arg_str[0:-2])
	return ['#'+name_line,preserved_line]


def origin(name):
	lines = []
	puid = prunedb.var_get(name)
	if not puid:
		raise Exception('An object by the name "%s" could not be found.'%(name) )
	file_puids = [puid]
	file_names = [name]
	necessary_files = []
	next_file = 0
	while next_file < len(file_puids):
		puid = file_puids[next_file]
		outs = prunedb.ios_get_by_file_puid(puid,'O')
		for out in outs:
			ios = prunedb.ios_get(out['op_id'])
			function_used = False
			for io in ios:
				if io['io_type']=='F':
					file_puids += [io['file_puid']]
					file_name = io['name']
					file_names += [file_name]
					function_used = True
				elif io['io_type']=='I':
					if io['file_puid']:
						file_puids += [io['file_puid']]
						file_name = io['name']
						file_names += [file_name]
			if function_used:
				lines += reversed(make_command(ios))
			else:
				new_name = file_names[next_file]



				#new_name += '@'+str(io['repo_id']) if io['repo_id'] else ''
				


				lines += ['PUT %s AS %s'%(puid, new_name)]
				necessary_files += [storage_pathname(puid)]

		next_file += 1
	return lines[::-1], necessary_files
					


def new_puid():
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





def create_operation(op_id,framework='local',local_fs=False):
	global sandbox_prefix

	ios = prunedb.ios_get(op_id)
	op = prunedb.op_get(op_id)

	options = ''
	arg_str = ''
	place_files = []
	fetch_files = []
	for io in ios:
		print io
		if io['io_type']=='F':
			function_name = io['name']
			function_puid = io['file_puid']
			func = prunedb.function_get(function_puid)
			out_names = func['out_names'].split()
			if io['pack']=='gzip':
				place_files.append({'src':storage_pathname(function_puid),'dst':function_name+'.gz'})
				options += ' -gz %s=%s.gz'%(function_name,function_name)
			else:
				place_files.append({'src':storage_pathname(function_puid),'dst':function_name})
	
		elif io['io_type']=='I':
			if io['literal']:
				arg = io['literal']
				arg_str += '%s '%(arg)
			else:
				arg = io['name']
				arg_str += '%s '%(arg)
				res = prunedb.copies_get(io['file_puid'])
				if len(res)==0:
					#print 'No file', io
					return {'cmd':None}
				if io['pack']=='gzip':
					place_files.append({'src':storage_pathname(io['file_puid']),'dst':str(arg)+'.gz'})
					options += ' -gunzip %s=%s.gz'%(arg,arg)
				else:
					place_files.append({'src':storage_pathname(io['file_puid']),'dst':str(arg)})
			

		elif io['io_type']=='O':
			fetch_files.append({'src':out_names[io['pos']],'dst':storage_pathname(io['file_puid']),'puid':io['file_puid']})

	arg_str = arg_str[0:-1]

	copies = prunedb.copies_get(op['env_puid'])
	env_type = ''
	if len(copies)>0:
		copy = copies[0]
		if copy['form']=='umbrella':
			env_type = copy['form']
			options += ' -umbrella'
		elif copy['form']=='targz':
			options += ' -targz ENVIRONMENT'
			env_type = copy['form']
			
		
		place_files.append({'src':storage_pathname(op['env_puid']),'dst':'ENVIRONMENT'})

	place_files.append({'src':storage_pathname('PRUNE_EXECUTOR'),'dst':'PRUNE_EXECUTOR'})
	fetch_files.append({'src':'prune_debug.log','dst':storage_pathname(op_id)+'.debug','puid':str(op_id)+'.debug'})

	if local_fs:
		for obj in place_files:
			if obj['dst']!='PRUNE_EXECUTOR':
				options += ' -ln %s=%s'%(obj['dst'],obj['src'])

	
	final_cmd = 'chmod 755 PRUNE_RUN; ./PRUNE_RUN'
	run_cmd = "./%s %s"%(function_name, arg_str)
	run_buffer = '#!/bin/bash\npython PRUNE_EXECUTOR%s %s'%(options,run_cmd)
	

	if framework=='wq':
		t = Task('')
		for obj in place_files:
			if not local_fs or obj['dst']=='PRUNE_EXECUTOR':
				t.specify_file(obj['src'], obj['dst'], WORK_QUEUE_INPUT, cache=True)
		t.specify_buffer(run_buffer, 'PRUNE_RUN', WORK_QUEUE_INPUT, cache=True)
	
		t.specify_command(final_cmd)
		t.specify_cores(1)

		for obj in fetch_files:
			if obj['src']=='prune_debug.log':
				t.specify_file(obj['dst'], obj['src'], WORK_QUEUE_OUTPUT, cache=False)
			else:
				t.specify_file(obj['dst'], obj['src'], WORK_QUEUE_OUTPUT, cache=True)
		return {'cmd':run_cmd,'framework':framework,'wq_task':t}
	
	else:
		sandbox_folder = sandbox_prefix+uuid.uuid4().hex+'/'
		try:
			os.mkdir(sandbox_folder)
		except OSError:
			if not os.path.isdir(sandbox_folder):
				raise
		for obj in place_files:
			if not local_fs or obj['dst']=='PRUNE_EXECUTOR':
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





wq = None
wq_task_cnt = 0
def useWQ(name, local_fs=False):
	global wq, wq_task_cnt, wq_local_fs
	wq_local_fs = local_fs
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
	global wq, wq_task_cnt, data_folder, wq_local_fs
	if wq:
		# Add new tasks first so that they are scheduled while waiting
		left = wq.hungry()
		if left>0:
			runs = getRuns(10000)
			for run in runs:
				if left <= 0:
					break
				operation = create_operation(run['op_id'],'wq',wq_local_fs)
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
								pathname = storage_pathname(io['file_puid'])
								store_file(pathname, io['file_puid'], True, storage_module='wq')

						print 'complete: run:%i, op_id:%i'%(run['id'],run['op_id'])
						prunedb.run_end(run['id'],t.return_status)
					else:
						for io in ios:
							if io['io_type']=='O':
								try:
									print 'io:',io
									pathname = storage_pathname(io['file_puid'])
									store_file(pathname, io['file_puid'], True, storage_module='wq')
								except:
									pass
						print 'Failed with exit code:',t.return_status
						print 'Resubmit to try again.'
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
	return False



max_concurrency = 0
local_workers = []
def useLocal(concurrency,local_fs=False):
	global local_workers, max_concurrency, local_local_fs
	print 'Allocating %i local workers. Don\'t forget to enter the WORK command to start execution.'%concurrency
	max_concurrency = concurrency
	local_local_fs = local_fs
	return True


def local_check():
	global local_workers, data_folder, local_local_fs
	w = 0

	if max_concurrency<=0:
		return True

	while w<len(local_workers):
		(op_id,operation,p) = local_workers[w]
		if p.poll() is not None:
			print 'Returned: %i (%s) '%(p.returncode,operation['cmd'])
			(stdout, stderr) = p.communicate()

			fails = 0
			for obj in operation['fetch_files']:
				try:
					store_file(operation['sandbox']+obj['src'], obj['puid'])
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
		operation = create_operation(run['op_id'],'local',local_local_fs)
		if operation['cmd']:
			operation['run'] = run
			print 'Start:',operation['cmd']
			w += 1
			p = subprocess.Popen(operation['final_cmd'], stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd = operation['sandbox'], shell=True)
			local_workers.append( [run['op_id'],operation,p] )
			prunedb.run_upd(run['id'],'Running',0,'','local')
	time.sleep(1)

	if w > 0:
		return True
	else:
		return False






