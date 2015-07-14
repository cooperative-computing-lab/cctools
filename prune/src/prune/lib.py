# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, traceback

import string, random, time, shutil, uuid, hashlib, re, threading

from work_queue import *

from distutils.spawn import find_executable

import subprocess
from subprocess import Popen, PIPE, call

from . import database
from . import wrapper



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
debug_level = None

def debug(*options):
	global debug_level

	if debug_level=='all':
		for opt in options:
			print opt



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




def initialize(new_data_folder, new_sandbox_prefix, debug=None, hadoop=False):
	global data_folder, sandbox_prefix, debug_level
	debug_level = debug
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
	exec_script.write( wrapper.script_str() )
	exec_script.close()

	return get_default_environment()


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
		copies = database.copies_get_by_chksum(chksum)
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
	copies = database.copies_get_by_chksum(chksum)
	if len(copies)>0:
		puid = copies[0]['puid']
		return {'puid':puid, 'chksum':chksum, 'filename':None, 'exists':True, 'pack':''}
	if not puid:
		puid = uuid.uuid4()
	return {'puid':puid, 'chksum':chksum, 'filename':None, 'exists':False, 'pack':''}

def putMetaData(ids, cmd_id=None):
	puid = ids['puid']
	if ids['pname']:
		database.tag_set(ids['pname'], 'B', puid)
	op_id = database.op_ins(None, '', '',display='PUT %s AS %s'%(ids['filename'],ids['pname']))
	#database.io_ins(op_id, 'F', function_puid, function_name)
	database.io_ins(op_id, 'I', None, ids['filename'], 0)
	database.io_ins(op_id, 'O', puid, ids['pname'], 0)
	return op_id



def store_file(filename, puid, wait=True, pack=None, storage_module=None):
	if not storage_module:
		if wait:
			cache_filename = storage_pathname(puid)
			#print cache_filename
			if filename:
				shutil.copy2( filename, cache_filename )
			length = os.stat(cache_filename).st_size
			chksum = hashfile(cache_filename)
			database.copy_ins(puid, chksum, length, pack, storage_module)
			runs = database.run_get_by_wait(puid)
			for run in runs:
				database.run_upd(run['puid'],'Run',wait=None)
			return True
		else:
			print 'LAZY PUT not yet implemented.'
			return False
	elif storage_module=='wq':
		cache_filename = storage_pathname(puid)
		length = os.stat(cache_filename).st_size
		chksum = hashfile(cache_filename)
		database.copy_ins(puid, chksum, length, pack, storage_module)
		runs = database.run_get_by_wait(puid)
		for run in runs:
			database.run_upd(run['puid'],'Run',wait=None)
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
			database.copy_ins(puid, chksum, length, form, pack, storage_module)
			return True
		else:
			print 'LAZY PUT not yet implemented.'
			return False
	elif storage_module=='wq':
		cache_filename = storage_pathname(puid)
		length = os.stat(cache_filename).st_size
		chksum = hashfile(cache_filename)
		database.copy_ins(puid, chksum, length, form, None, storage_module)
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
			database.copy_ins(id, location, cache_filename)
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
	tag = database.tag_get(name)
	puid = tag['puid']
	if not puid:
		raise Exception('That name is not found in the database: '+name)
	return database.copies_get(puid)


def isNameDone(name):
	start = time.time()
	puid = database.tag_get(name)
	copies = database.copy_get(puid)
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
			message = '''
List of available commands:
EVAL (default,optional),  PUT, GET,  USE, WORK,  STATUS,  EXIT, QUIT,  CAT, LS, CUT,  RUN,  RESET
See the manual for more details.
			'''
			print message
			return True
		args = expr.split(',')
		results = []
		for a, arg in enumerate(args):
			arg = arg.strip()
			ar = arg.split('@')
			name = None
			name = ar[0]
			puid = None
			if len(ar)>2:
				time = ar[2]
			tag = database.tag_get(name)
			if tag:
				puid = tag['puid']
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
			elif len(arg)>0:
				results += [[None, arg, None]]
		return {'arg_list':results}

	elif nextpos==lparen: # Function invokation
		if (depth==0):
			raise Exception('You must assign a name to the function result(s).')
		if 'funcname' in extra:
			function_file_puid = expr[0:lparen].strip()
			function_name = extra['funcname']
		else:
			function_name = expr[0:lparen].strip()
			tag = database.tag_get(function_name)
			if not tag:
				raise Exception('A function by the name "%s" could not be found.'%function_name)
			else:
				function_file_puid = tag['puid']
		func = database.function_get_by_file(function_file_puid)
		if not func:
			in_types = []
			fout_names = []
			fout_types = []
			with open( storage_pathname(function_file_puid) ) as f:
				for line in f:
					if line.startswith('#PRUNE_INPUTS'):
						in_types = line[14:-1].split()
						if in_types == 'File*':
							in_types = ['File']
							for i in range(0,40):
								in_types += ['File']
					elif line.startswith('#PRUNE_OUTPUT'):
						ar = line.split()
						fout_names.append(ar[1])
						if len(ar)>2:
							fout_types.append(' '.join(ar[2:]))
						else:
							fout_types.append('')
			database.function_ins(function_file_puid, ' '.join(in_types), ' '.join(fout_names), ','.join(fout_types) )
			func = database.function_get_by_file(function_file_puid)
		else:
			in_types = func['in_types'].split()
			fout_names = func['out_names'].split()
			fout_types = func['out_types'].split()

		remain = expr[lparen+1:expr.rfind(')')]
		res = eval(remain, depth+1, cmd_id, dict({'in_types':in_types}.items()+extra.items()) )

		function_puid = func['puid']
		op_string = '%s('%function_puid
		if len(res['arg_list']) < len(in_types):
			print 'Too few arguments passed to the function.'
			return None
		elif len(res['arg_list']) > len(in_types) and in_types[-1][-1]!='*':
			print 'Too many arguments passed to the function.'
			return None
		else:
			for r in res['arg_list']:
				if r[0]:
					op_string += str(r[0])+','
				else:
					op_string += r[1]+','
		op_string = op_string[0:-1] + ')'
		#op_chksum = hashstring(op_string)
		op_chksum = op_string
		op_env_chksum = op_string + str(ENVIRONMENT)

		op = database.op_get_by_env_chksum(op_env_chksum)
		old_op_id = None
		if op:
			old_op_id = op['puid']
			old_ios = database.ios_get(old_op_id)
			op_display = displayOperation(old_ios)
			all_files_exist = True
			for old_io in old_ios:
				if old_io['io_type']=='O' and old_io['file_puid'] and not os.path.isfile( storage_pathname(old_io['file_puid']) ):
					all_files_exist = False
					debug('Output file not memoized:',old_io)
			if not all_files_exist:
				runs = database.runs_get_by_op_puid(old_op_id)
				in_progress = False
				for run in runs:
					if run['queue'] in ['Run','Running','Waiting']:
						in_progress = True
						break
				if not in_progress:
					debug('Operation not already running so queuing for execution:'+op_display)
					database.run_upd_by_op_puid(op['puid'], 'Run', -1, '', 'local')


		op_id = database.op_ins(function_puid,op_chksum, op_env_chksum, env_puid=ENVIRONMENT)
		database.io_ins(op_id, 'E', ENVIRONMENT, '')

		database.io_ins(op_id, 'F', function_puid, function_name)
		for i,arg in enumerate(res['arg_list']):
			database.io_ins(op_id, 'I', arg[0], arg[1], i)

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
				database.tag_get
				puid = uuid.uuid4()
			results += [[puid,name,None]]
			database.io_ins(op_id, 'O', puid, name, i)
			if name:
				database.tag_set(name,'B',puid)
		new_ios = database.ios_get(op_id)
		op_display = displayOperation(new_ios)
		database.op_upd_display(op_id,op_display)

		if not old_op_id:
			database.run_ins(op_id)
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
				database.tag_set(args[a], 'B', res['puids'][a])
			return res

def displayOperation(ios):
	inputs = outputs = ''
	for io in ios:
		if io['io_type']=='F':
			function_name = io['display']
		elif io['io_type']=='I':
			inputs += io['display']+', '
		elif io['io_type']=='O':
			outputs += io['display']+', '
	return '%s = %s( %s ) '%(outputs[:-2], function_name, inputs[:-2])


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
	puid = database.tag_get(name)
	if not puid:
		raise Exception('An object by the name "%s" could not be found.'%(name) )
	file_puids = [puid]
	file_names = [name]
	necessary_files = []
	next_file = 0
	while next_file < len(file_puids):
		puid = file_puids[next_file]
		outs = database.ios_get_by_file_puid(puid,'O')
		for out in outs:
			ios = database.ios_get(out['op_id'])
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

def get_default_environment():
	global ENVIRONMENT
	env = None
	try:
		env = database.environment_get_last()
		if env:
			answer = raw_input('Would you like to execute operations in the environment used in the last session? [y] ')
			if answer in ['','y','yes','Y','YES']:
				ENVIRONMENT = env['puid']
				return True
			else:
				env = None
		else:
			print 'Prune needs a file that describes the environment that operations should be executed in.'
			print 'Currently the only option is a .tar.gz file that, when unzipped in a sandbox, contains all the libraries and dependencies needed to run the operations in a Linux environment.'


		while not env:
			line = raw_input('What local file should be used as execution environment? [default.env] ')
			if len(line)<=0:
				env_file = 'default.env'
			else:
				env_file = line
			line = raw_input('What type of environment file is it [targz]: ')
			if len(line)<=0:
				env_type = 'targz'
			else:
				env_type = line

			ids = getDataIDs(env_file)
			if ids:
				store_file(env_file, ids['puid'])
				database.environment_ins(ids['puid'],env_type)
				env = database.environment_get_last()
				ENVIRONMENT = env['puid']
				return True
			else:
				print 'Source file does not exist: %s'%(env_file)
	except KeyboardInterrupt:
		print ''
		sys.exit(0)



def add_store(fold_filename, unfold_filename):
	global STOREID
	fold_file_id = file_put(fold_filename)
	unfold_file_id = file_put(unfold_filename)
	STOREID = database.store_ins(fold_file_id, unfold_file_id)


def getRuns(cnt):
	runs = database.run_get_by_queue('Run')
	if len(runs)>cnt:
		runs = runs[:cnt]
	return runs
	#return database.run_get_by_queue('Run',cnt)

def getQueueCounts():
	return database.run_queue_cnts()

def getdb():
	return database

def cmd_log(original,final,op_id=None):
	database.cmd_ins(original,final,op_id)



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





def create_operation(op_id,framework='local',local_fs=False, dry_run=False):
	global sandbox_prefix

	ios = database.ios_get(op_id)
	op = database.op_get(op_id)

	final_output = ''
	options = ''
	arg_str = ''
	env_type = ''
	env_files = []
	place_files = []
	fetch_files = []
	for io in ios:
		if io['io_type']=='F':
			function_name = io['display']
			function_puid = op['function_puid']
			func = database.function_get(function_puid)
			out_names = func['out_names'].split()
			out_types = func['out_types'].split()
			res = database.copies_get(func['file_puid'])
			for copy in res:
				if copy['pack']=='gz':
					place_files.append({'src':storage_pathname(func['file_puid']),'dst':function_name+'.gz'})
					options += ' -gz %s=%s.gz'%(function_name,function_name)
				else:
					place_files.append({'src':storage_pathname(func['file_puid']),'dst':function_name})

		elif io['io_type']=='I':
			arg_str += '%s '%(io['display'])
			if io['file_puid']:
				res = database.copies_get(io['file_puid'])
				if len(res)==0:
					#print 'No file', io
					database.run_upd_by_op_puid(op_id,'Waiting',wait=io['file_puid'])
					return {'cmd':None}
				for copy in res:
					if copy['pack']=='gz':
						place_files.append({'src':storage_pathname(io['file_puid']),'dst':io['display']+'.gz'})
						options += ' -gunzip %s=%s.gz'%(io['display'],io['display'])
					else:
						place_files.append({'src':storage_pathname(io['file_puid']),'dst':io['display']})


		elif io['io_type']=='O':

			if 'gz' in out_types:
				options += ' -gzip %s'%(out_names[io['pos']])
				fetch_files.append({'src':out_names[io['pos']]+'.gz','dst':storage_pathname(io['file_puid']),'puid':io['file_puid'],'pack':'gz','display':io['display']})
			else:
				fetch_files.append({'src':out_names[io['pos']],'dst':storage_pathname(io['file_puid']),'puid':io['file_puid'],'pack':'','display':io['display']})



		elif io['io_type']=='E' and io['file_puid']:
			env = database.environment_get(io['file_puid'])
			env_type = env['env_type']
			copies = database.copies_get(env['file_puid'])
			if len(copies)>0:
				copy = copies[0]
				if env_type=='umbrella':
					options += ' -umbrella'
				elif env_type=='targz':
					options += ' -targz ENVIRONMENT'


				env_files.append({'src':storage_pathname(copy['puid']),'dst':'ENVIRONMENT','puid':copy['puid'],'pack':copy['pack'],'display':io['display']})

	arg_str = arg_str[0:-1]


	env_files.append({'src':storage_pathname('PRUNE_EXECUTOR'),'dst':'PRUNE_EXECUTOR'})
	fetch_files.append({'src':'prune_debug.log','dst':storage_pathname(op_id)+'.pdebug','puid':str(op_id)+'.pdebug','pack':'','puid':str(op_id)+'.pdebug','display':str(op_id)+'.pdebug','env':True})

	if local_fs:
		for obj in place_files:
			if obj['dst']!='PRUNE_EXECUTOR':
				options += ' -ln %s=%s'%(obj['dst'],obj['src'])

	if env_type=='umbrella':
		in_str = ''
		for obj in place_files:
			in_str += ',%s=%s'%(obj['dst'],obj['dst'])
		options += ' -umbrellai '+in_str[1:]
		fetch_files.append({'src':'umbrella.log','dst':storage_pathname(op_id)+'.udebug','puid':str(op_id)+'.udebug','pack':'','puid':str(op_id)+'.udebug','display':str(op_id)+'.udebug','env':True})
		final_output = 'final_output/'



	final_cmd = 'chmod 755 PRUNE_RUN PRUNE_EXECUTOR; ./PRUNE_RUN'
	run_cmd = "./%s %s"%(function_name, arg_str)
	run_buffer = './PRUNE_EXECUTOR%s %s'%(options,run_cmd)
	#print run_buffer

	if env_type=='umbrella':
		umbrella_file = which('umbrella')
		env_files.append({'src':umbrella_file[0],'dst':'UMBRELLA_EXECUTABLE'})


	if dry_run:
		return {'cmd':op['display'],'fetch_files':fetch_files}

	elif framework=='wq':
		t = Task('')
		for obj in env_files:
			t.specify_file(obj['src'], obj['dst'], WORK_QUEUE_INPUT, cache=True)
		for obj in place_files:
			if not local_fs:
				t.specify_file(obj['src'], obj['dst'], WORK_QUEUE_INPUT, cache=True)
		t.specify_buffer(run_buffer, 'PRUNE_RUN', WORK_QUEUE_INPUT, cache=True)

		t.specify_command(final_cmd)
		t.specify_cores(1)

		for obj in fetch_files:
			if 'env' in obj and obj['env']:
				t.specify_file(obj['dst'], obj['src'], WORK_QUEUE_OUTPUT, cache=False)
			else:
				t.specify_file(obj['dst'], final_output+obj['src'], WORK_QUEUE_OUTPUT, cache=True)
		return {'cmd':op['display'],'framework':framework,'wq_task':t,'fetch_files':fetch_files}

	else:
		sandbox_folder = sandbox_prefix+uuid.uuid4().hex+'/'
		try:
			os.mkdir(sandbox_folder)
		except OSError:
			if not os.path.isdir(sandbox_folder):
				raise


		for obj in env_files:
			shutil.copy2(obj['src'], sandbox_folder+obj['dst'])
		for obj in place_files:
			if not local_fs or not dry_run:
				shutil.copy2(obj['src'], sandbox_folder+obj['dst'])


		f = open(sandbox_folder+'PRUNE_RUN','w')
		f.write(run_buffer)
		f.close()
		os.chmod(sandbox_folder+'PRUNE_RUN', 0555)


		return {'cmd':op['display'],'final_cmd':final_cmd,'framework':framework,'sandbox':sandbox_folder,'fetch_files':fetch_files,'final_output':final_output}



def which(name, flags=os.X_OK):
	result = []
	exts = filter(None, os.environ.get('PATHEXT', '').split(os.pathsep))
	path = os.environ.get('PATH', None)
	if path is None:
		return []
	for p in os.environ.get('PATH', '').split(os.pathsep):
		p = os.path.join(p, name)
		if os.access(p, flags):
			result.append(p)
		for e in exts:
			pext = p + e
			if os.access(pext, flags):
				result.append(pext)
	return result

wq = None
wq_task_cnt = 0
def useWQ(name, local_fs=False, debug_level=None):
	global wq, wq_task_cnt, wq_local_fs
	wq_local_fs = local_fs
	if wq:
		wq.shutdown_workers(0)
		qs = database.run_get_by_queue('Running')
		for q in qs:
			run_upd(q['puid'],'Run',None,'')

	try:
		wq = WorkQueue(0)
	except Exception as e:
		raise Exception("Instantiation of Work Queue failed!")

	wq.specify_name(name)
	print "Work Queue master started on port %d with name '%s'..." % (wq.port,name)
	wq.specify_log("wq.log")
	#wq.set_bandwidth_limit('1250000000')
	if debug_level:
		cctools_debug_flags_set(debug_level)
		cctools_debug_config_file("wq.debug")

	return True
def usingWQ():
	global wq
	if wq:
		return True
	else:
		return False
def stopWQ():
	if wq:
		wq.shutdown_workers(0)

wq_ops = {}
def wq_check():
	global wq, wq_task_cnt, data_folder, wq_local_fs, wq_ops
	if wq:
		# Add new tasks first so that they are scheduled while waiting
		left = wq.hungry()
		if left>0:
			runs = getRuns(10000)
			for run in runs:
				if left <= 0:
					break
				operation = create_operation(run['op_puid'],'wq',wq_local_fs)
				if operation['cmd']:
					t = operation['wq_task']
					task_id = wq.submit(t)
					wq_ops[task_id] = operation
					database.run_upd(run['puid'],'Running',task_id,'','wq')
					wq_task_cnt += 1
					left -= 1
					print 'Started: %s  (%s) #%i'%(operation['cmd'], run['puid'], wq_task_cnt)
				else:
					'No task'

		if wq_task_cnt==0:
			return False
		else:
			# Wait for finished tasks (which also schedules new ones)
			t = wq.wait(5)
			while t: #Once there are no more tasks currently finished, return
				try:
					run = database.run_get_by_task_id(t.id)
					op_id = run['op_puid']
					ios = database.ios_get(op_id)
					if t.return_status==0:
						operation = wq_ops[t.id]
						all_files = True
						exec_time = exec_space = 0
						for obj in operation['fetch_files']:
							if os.path.isfile(obj['dst']):
								store_file(obj['dst'],obj['puid'],True,obj['pack'],storage_module='wq')
								if obj['display'].endswith('.pdebug'):
									f = open(obj['dst'])
									for line in f:
										if line.startswith('Execution time: '):
											exec_time = line.split(": ")[1]
										elif line.startswith('Execution space: '):
											exec_space = line.split(": ")[1]
									f.close()
							else:
								print 'Output file not found: %s'%(obj['display'])
								all_files = False
						'''
						print ios
						for io in ios:
							if io['io_type']=='O':
								#print 'io:',io
								pathname = storage_pathname(io['file_puid'])
								if os.path.isfile(pathname):
									store_file(pathname, io['file_puid'], True, storage_module='wq')
								else:
									print 'Output file did not exist: %s'%(io['display'])
									all_files = False
						'''
						if not all_files:
							operation = create_operation(op_id,'local')
							database.run_upd(run['puid'],'Failed',t.return_status)
							print 'Failed: %s  (%s)'%(operation['cmd'], run['puid'])
							print 'Try to execute the operation manually in the sandbox at %s by running chmod 755 PRUNE_RUN; ./PRUNE_RUN'%(operation['sandbox'])
						else:
							print 'Completed: %s  (%s)'%(operation['cmd'], run['puid'])

							database.run_end(run['puid'], cpu_time=exec_time, disk_space=exec_space)



					else:
						print 'Failed with exit code:',t.return_status
						operation = wq_ops[t.id]
						print operation
						for obj in operation['fetch_files']:
							if os.path.isfile(obj['dst']):
								print 'File saved:',obj
								store_file(obj['dst'],obj['puid'],True,obj['pack'],storage_module='wq')
						print 'Resubmit to try again: '+operation['cmd']
						print t.command
						database.run_upd(run['puid'],'Failed',0,'','Exit code: %i'%t.return_status)

				except:
					print 'Return status:',t.return_status
					print traceback.format_exc()
					print run
					if 'ios' in locals():
						print ios
					database.run_upd(run['puid'],'Failed',0,'',traceback.format_exc())

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

def usingLocal():
	global wq
	if max_concurrency<=0:
		return False
	else:
		return True

def local_check():
	global local_workers, data_folder, local_local_fs
	w = 0

	if max_concurrency<=0:
		return True

	while w<len(local_workers):
		(op_id,operation,p) = local_workers[w]
		if p.poll() is not None:
			#print 'Returned: %i (%s) '%(p.returncode,operation['cmd'])
			(stdout, stderr) = p.communicate()

			fails = 0
			exec_time = exec_space = 0
			for obj in operation['fetch_files']:
				try:
					if 'env' in obj and obj['env']:
						store_file(operation['sandbox']+obj['src'], obj['puid'])
					else:
						store_file(operation['sandbox']+operation['final_output']+obj['src'], obj['puid'])
					if obj['display'].endswith('.pdebug'):
						f = open(operation['sandbox']+obj['src'])
						for line in f:
							if line.startswith('Execution time: '):
								exec_time = line.split(": ")[1]
							elif line.startswith('Execution space: '):
								exec_space = line.split(": ")[1]
						f.close()

				except:
					fails += 1

			if p.returncode==0 and fails==0:
				print 'Completed: %s  (%s)'%(operation['cmd'], operation['run']['puid'])
				database.run_end(operation['run']['puid'], cpu_time=exec_time, disk_space=exec_space)
				shutil.rmtree(operation['sandbox'])
			else:
				debug( 'returncode =', p.returncode, ', fails =',fails, traceback.format_exc())
				print 'Failed: %s  (%s)'%(operation['cmd'], operation['run']['puid'])
				database.run_upd(operation['run']['puid'], 'Failed', p.returncode, '', 'local')
			del local_workers[w]
			w -= 1
		w += 1

	left = max_concurrency - len(local_workers)
	runs = getRuns(left)
	for run in runs:
		operation = create_operation(run['op_puid'],'local',local_local_fs)
		if operation['cmd']:
			operation['run'] = run
			print 'Started: %s  (%s)'%(operation['cmd'], run['puid'])
			w += 1
			p = subprocess.Popen(operation['final_cmd'], stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd = operation['sandbox'], shell=True)
			local_workers.append( [run['op_puid'],operation,p] )
			database.run_upd(run['puid'],'Running',0,'','local')
	time.sleep(1)

	if w > 0:
		return True
	else:
		return False
