# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, traceback, os.path, select
import time, threading

import readline, re

import zipfile
import operator

from work_queue import *

from . import lib


database = lib.getdb()

HOME = os.path.expanduser("~")
CWD = os.getcwd()
config_file = HOME+'/.pruneconf'
config_file2 = None
run_filename = run_lines = None
reset_all = False
#debug_level = 'all'
debug_level = None

argi = 1
while argi<len(sys.argv):
	arg = sys.argv[argi]
	if arg in ['-m','--mf']:
		argi += 1
		mf_filename = sys.argv[argi]
	elif arg in ['-p','--plan']:
		argi += 1
		run_filename = sys.argv[argi]
	elif arg in ['-s','--stdin']:
		run_filename = 'stdin'
	elif arg in ['-r','--run']:
		argi += 1
		run_lines = sys.argv[argi].split(';')
	elif arg in ['-w','--cwd']:
		argi += 1
		nwd = sys.argv[argi]
		if nwd[0]=='/':
			os.chdir(nwd)
		else:
			os.chdir(os.getcwd()+'/'+nwd)
		CWD = os.getcwd()
		config_file = HOME+'/.prune.conf'
	elif arg in ['-c','--conf']:
		argi += 1
		config_file2 = sys.argv[argi]
	elif arg in ['-v','--version']:
		cctools_version = '5.0.0 [prune:887c027d-DIRTY]'
		cctools_releasedate = '2015-05-26 11:56:15 -0400'
		print "prune version %s (released %s)"%(cctools_version,cctools_releasedate)
		sys.exit(0)
	elif arg in ['-d','--debug']:
		argi += 1
		debug_level = sys.argv[argi]
	elif arg=='--reset':
		reset_all = True
	elif arg in ['-h','--help']:
		message = '''Use: prune [options]
prune options:
	-f.--file <pathname>	PUT the file in Prune and RUN it.
	-r,--run '<commands>'	RUN the provided string as Prune commands separated by semi-colons.
	-w,--cwd <path>		Directory to use when GETing and PUTing files
	-m,--mf <pathname>	Convert the given Makeflow file into Prune commands and RUN them.
	-c,--conf <pathname>	Specify a configuration file. (default=~/.pruneconf)
	-d,--debug <subsystem>	Enable debugging on worker for this subsystem (try -d all to start).
	-s,--stdin		Read commands from standard input
	-v,--version		Display the version of cctools that is installed.
	-h,--help		Show command line options
	--reset			Truncate the database and delete the data and sandboxes directories.
		'''
		print message
		sys.exit(0)
	else:
		run_filename = arg
	argi += 1
if config_file2:
	config_file = config_file2


def debug(*options):
	global debug_level
	
	if debug_level=='all':
		for opt in options:
			print opt



terminate = False
block = False
hadoop_data = False
temp_ids = []

data_folder = db_pathname = sandbox_prefix = db_version = None
if os.path.isfile(config_file):
	with open(config_file) as f:
		for line in f.readlines():
			(meta_type,value) = line[:-1].split('\t')
			if meta_type=='sandbox':
				sandbox_prefix = value
			elif meta_type=='database':
				db_pathname = value
			elif meta_type=='data_folder':
				data_folder = value
else:
	base_dir = HOME+'/.prunespace/'
	
	print '\nWelcome to Prune!'
	print 'The default config file will be stored at:',config_file
	line = raw_input('Enter a location for data files, database, and sandboxes [%s]: '%(base_dir))
	
	if len(line)>0:
		if line[-1] != '/':
			line = line + '/'
		base_dir = line
	
	data_folder = base_dir+'data/'
	db_pathname = base_dir+'_prune.db'
	sandbox_prefix = base_dir+'sandbox/'



def prompt():
	return 'PRUNE$ '

forever_back = '\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b'
	
def user_interface():
	global block, terminate, forever_back, wait_start
	spaces = '                                        '
	cmds = database.cmds_get()
	for cmd in cmds:
		readline.add_history(cmd['command'])
	while not terminate:
		try:
			line = raw_input(prompt())
			block = True
			try:
				done = process_line(line)
				while not done:
					done = process_line(line)
			except KeyboardInterrupt:
				wait_start = None
				print 'Command cancelled:',line
			block = False
			sys.stdout.write(forever_back+spaces+forever_back)
			sys.stdout.flush()
		except EOFError:
			print '\nPrune exiting for Ctl-d...'
			sys.exit(0)
		except KeyboardInterrupt:
			print '\nPrune exiting for Ctl-c...'
			return
		except Exception:
			print traceback.format_exc()


work_start = None
last_report = None
def process_line(line):
	global block, work_start, last_report, hadoop_data, terminate, debug_level
	cmd_id = database.cmd_ins(line)
	try:
		if len(line)==0 or line[0]=='#':
			return True

		elif line.upper().startswith('EXIT') or line.upper().startswith('QUIT'):
			terminate = True
			return True
		elif line.upper().startswith('RESET'):
			if line=='RESET ALL':
				lib.truncate()
				database.truncate()
				database.initialize(db_pathname, debug_level)
				lib.initialize(data_folder, sandbox_prefix)
			else:
				print 'This command is designed to delete all data and meta-data in Prune. If you are sure this is what you want to do, use "PRUNE ALL" (no lower case letters). '


			return True


		elif line.upper().startswith('HELP'):
			message = '''
List of available commands:
EVAL (default,optional),  PUT, GET,  USE, WORK,  STATUS,  EXIT, QUIT,  CAT, LS, CUT,  RUN,  RESET
See the manual for more details.
			'''
			print message
			return True





		elif line.upper().startswith('PUT'):
			# wait = True
			filename = pname = puid = data = None
			wait = True
			# if line.find('"')>=0:
			# 	data = line[line.find('"')+1:line.rfind('"')]
			# 	line = line[:line.find('"')-1]+line[line.rfind('"')+1:]
			ar = line.split()
			i = 1
			pack = None
			while i < len(ar):
				item = ar[i]
				if item.upper() == 'AS':
					i += 1
					pname = ar[i]
				elif item.upper() == 'GZ':
					pack = 'gz'
				# elif item.upper() == 'UUID' or item.upper() == 'GUID':
				# 	i += 1
				# 	puid = ar[i]
				# elif item.upper() == 'ENV':
				# 	i += 1
				# 	form = ar[i]
				elif not filename:
					filename = item
				elif not pname:
					print 'Bad command: use "PUT <local_namespace_filename> AS <prune_namespace_filename>" '
					return True
				i += 1
			if not pname and filename:
				pname = filename
			if filename:
				ids = lib.getDataIDs(filename)
				if not ids:
					print 'Source file does not exist: %s'%(filename)
					return True
			elif data:
				ids = lib.getDataIDs2(data)
				if not ids:
					print 'Source file does not exist: %s'%(data)
					return True
			ids['pname'] = pname
			ids['pack'] = pack
			if ids['exists']:
				debug('That file already exists with Prune uid %s'%(ids['puid']))
				if ids['pname']:
					database.tag_set(ids['pname'], 'B', ids['puid'])
			elif data:
				lib.store_data(data,ids['puid'],wait)
				op_id = lib.putMetaData(ids, cmd_id)
			elif filename:
				op_id = lib.putMetaData(ids, cmd_id)
				lib.store_file(filename,ids['puid'],wait,pack=ids['pack'])
			return True





		elif line.upper().startswith('GET'):
			wait = True
			filename = expr = None
			ar = line.split(' ')
			i = 1
			while i < len(ar):
				item = ar[i]
				if not expr:
					expr = item
				elif item.upper() == 'AS':
					filename = ar[i+1]
					i += 1
				else:
					print 'Please specify the destination filename with the AS keyword.'
					print 'Ex. "GET <prune_name> AS <local_filename>".'

				#elif item == 'LAZY':
				#	wait = False
				i += 1
			if not filename:
				filename = expr
			res = database.tag_get(expr)
			if not res:
				print '%s cannot be found.'%(filename)
				return True
			try:
				lib.restore_file(res['puid'], filename)
			except IOError:
				print 'That data does not yet exist. You may need to wait for it to be generated.'
			return True
			




		elif line.upper().startswith('ENV'):
			ar = line.split()
			if len(ar)<=2 or len(ar)>3:
				print 'You need to specify the type of environment you want to use. Any file needed for the environment should be first PUT into Prune.'
				print 'Please try "ENV targz <prune_filename>" for a file that simply needs to be untarred and ungzipped in the sandbox to create the environment.'
				#print '      -or- "ENV umbrella <prune_filename>" to use an Umbrella specification for the environment.'
			else:
				(env_type,env_filename) = ar[1:3]
				if env_type not in ['targz']:
					print 'Invalid environment type:',env_type
					return True
				tag = database.tag_get(env_filename)
				if not tag:
					print '%s not found. No environment was set.'%(env_filename)
					return True
				#print tag
				res = database.environment_get_by_file_puid(tag['puid'],env_type)
				if res:
					env_puid = res['puid']
				else:
					env_puid = database.environment_ins(tag['puid'], env_type)
				database.tag_set('DefaultEnvironment','E',env_puid)
				lib.get_default_environment()
			return True



		elif line.upper().startswith('USE'):
			ar = line.split(' ')
			resource_type = ar[1]
			if 'TRY_LOCAL' in line:
				local_fs = True
			else:
				local_fs = False
			if resource_type=='local':
				concurrency = int(ar[2])
				lib.useLocal(concurrency,local_fs)
			elif resource_type=='wq':
				master_name = ar[2]
				lib.useWQ(master_name,local_fs,debug_level=debug_level)
			else:
				print 'Specified resources are not recognized or implemented.'
				print 'Please try "USE local <#>" to use the specified number of processes.'
				print '      -or- "USE wq <master_name>" to start a Work Queue master.'

			return True




		elif line.upper().startswith('WORK'):
			if not work_start:
				work_start = time.time()
				last_report = work_start - 5


			output = ''
			some_in_progress = False
			res = lib.getQueueCounts()
			for r in res:
				output += '%s: %i   '%( r['queue'], r['cnt'] )
				if r['queue']=='Running' or r['queue']=='Run':
					some_in_progress = True
			if ((time.time()-last_report)>15):
				now = time.time()
				minutes = (time.time()-work_start)/60
				seconds = (time.time()-work_start)%60

				print 'Queue sizes...', output, time.strftime("%H:%M:%S"), now, '(%02dm%02ds)'%( minutes, seconds )
				last_report = now

			lib.wq_check()
			lib.local_check()
			
			ar = line.split(' ')
			if len(ar)>1 and ar[1]=='FOR':
				timeout = float(ar[2])
			else:
				timeout = 9999999999.0

			if ( (time.time()-work_start) > timeout ):
				#print 'ws=None'
				work_start = None
				if ar[-1]=='TERMINATE':
					terminate = True
				return True
			elif not (lib.usingWQ() or lib.usingLocal()):
				print 'No resources are being used, so work cannot be done.'
				print 'Please try "USE local <#>" to use the specified number of processes.'
				print '      -or- "USE wq <master_name>" to start a Work Queue master.'
				return True
			elif not some_in_progress:
				minutes = (time.time()-work_start)/60
				seconds = (time.time()-work_start)%60
				print 'PRUNE finished executing after: %02dm%02ds'%( minutes, seconds )
				#print 'DB read, write count:', database.get_query_cnts()

				work_start = None
				if ar[-1]=='TERMINATE':
					terminate = True
				return True
			else:
				#time.sleep(1)
				return False
			


		


		elif line.upper().startswith('RUN'):
			start_time = time.time()
			ar = line.split()
			name = ar[1]
			try:
				res = lib.locate_copies(name)
				cache_filename = lib.storage_pathname(res[0]['puid'])
				with open(cache_filename) as f:
					for line in f.read().splitlines():
						if len(line)>1 and line[0]!='#':
							print line
							done = process_line(line)
							while not done:
								done = process_line(line)
				minutes = (time.time()-start_time)/60
				seconds = (time.time()-start_time)%60
				print 'Total run (with preservation): %02dm%02ds'%( minutes, seconds )
			except TypeError as e:
				print '%s not found:'%(name)
			except Exception as e:
				print 'Something unexpected occurred.'
				debug('Exception on: '+line, traceback.format_exc())
			return True



		elif line.upper().startswith('PRUN'):
			ar = line.split(' ')
			for line in ['PUT %s'%(ar[1]),'RUN %s'%(ar[1])]:
				print line
				done = process_line(line)
				while not done:
					done = process_line(line)
			return True




		elif line.upper().startswith('STATUS'):
			global temp_ids
			ar = line.split()
			if len(ar)<=1:
				res = lib.getQueueCounts()
				output = 'Queue sizes... '
				for r in res:
					output += '%s: %i   '%( r['queue'], r['cnt'] )
				print output
				print 'Enter "STATUS <queue_name>" to see more details, or "STATUS <queue_name> <new_status>" to change the status for an entire queue.'
				return True

			try:
				queue_name = ar[1]
				new_queue_name = None
				if len(ar)>2:
					new_queue_name = ar[2]
				runs = database.run_get_by_queue(queue_name)
				for run in runs:
					if new_queue_name:
						database.run_upd(run['puid'], new_queue_name)
					else:
						if run['op_puid'] not in temp_ids:
							temp_ids.append(run['op_puid'])
						operation = lib.create_operation(run['op_puid'],dry_run=True)
						temp_id = temp_ids.index(run['op_puid'])+1
						print '%i: %s'%(temp_id,operation['cmd'])
			except KeyboardInterrupt:
				print ''
			except:
				debug('Exception on:'+line, traceback.format_exc())
				print 'Error showing status for %s'%(queue_name)

			return True



		elif line.upper().startswith('CUT'):
			ar = line.split()
			if len(ar)==1:
				print 'Use "CUT *" to remove all names, or you can CUT a specific name (no wildcards).'
			elif len(ar)==2 and ar[1]=='*':
				res = database.tags_getAll()
				for r in res:
					database.tag_unset(r['name'])
			else:
				for name in ar[1:]:
					try:
						database.tag_unset(name)
					except:
						debug('Exception on:'+line, traceback.format_exc())
						print '%s could not be CUT'%(name)
			return True


				
		elif line.upper().startswith('LS'):
			ar = line.split()
			keyword = None
			if len(ar)<=1:
				print 'Use "LS *" to show all names, or you can use a keyword filter (no wildcards).'
			elif len(ar)>1 and ar[1]!='*':
					keyword = ar[1]
			res = database.ls()
			for r in res:
				if r['name'][0]=='_':
					continue #This is for _ENV
				if keyword:
					if keyword in r['name']:
						if r['length']:
							print '%s\t-> %i bytes @ %s'%(r['name'],r['length'], pretty_date(r['at']) )
						else:
							print r['name'],'\t(pending)'
				else:
					if r['length']:
						print '%s\t-> %i bytes @ %s'%(r['name'],r['length'], pretty_date(r['at']) )
					else:
						print r['name'],'\t(pending)'
			return True
				


		elif line.upper().startswith('CAT'):
			ar = line.split()
			tag = database.tag_get(ar[1])
			if tag:
				puid = tag['puid']
				pathname = lib.storage_pathname(puid)
				if os.path.isfile(pathname):
					with open(pathname) as f:
						try:
							for line in f:
								sys.stdout.write( line )
						except KeyboardInterrupt:
							print ''

					return True
				else:
					print 'That file does not exist yet. You may need to tell Prune to WORK before it will become available.'
			else:
				print 'Cannot find anything by that name.'
			return True














		elif line.upper().startswith('LOCATE____'):
			ar = line.split(' ')
			name = ar[1]
			now = True if len(ar)>2 and ar[2]=='NOW' else False
			res = lib.locate_pathname(name)
			if res:
				if now:
					peek(forever_back)
				print name,'IS_AT',res
				return True
			elif now:
				return False
			else:
				print 'That data does not exist yet. You could try WAITing for it, or try again later.'
				return True




		elif line.upper().startswith('EXPORT'):
			ar = line.split(' ')
			lines,files = lib.origin(ar[1])
			i = 2
			while i < len(ar):
				item = ar[i]
				if item == 'AS':
					destination = ar[i+1]
					i += 1
				i += 1
			zf = zipfile.ZipFile(destination, mode='w')
			try:
				plan = 'REPO %s\n'%(repo_puid)
				plan += '\n'.join(lines)
				zf.writestr('prune_cmds.plan', plan)
				for pathname in files:
					zf.write(pathname,pathname.split('/')[-1])
			finally:
				zf.close()
			
			return True




		elif line.upper().startswith('MF'):
			ar = line.split(' ')
			filename = ar[1]
			
			original_files = []
			intermediate_files = []
			final_files = []
			functions = {}
			next_func_id = 1
			with open(filename) as f:
				for line in f.read().splitlines():
					if len(line)<=0:
						continue
					ar = line.split(':')
					if len(ar)>1:
						(out_str,in_str) = ar
						outs = out_str.split()
						ins = in_str.split()
					else:
						line = line.strip()
						if line.upper().startswith('LOCAL'):
							line = line[6:]
						function_string = "#!/bin/bash\n\n#PRUNE_INPUT"
						for in_file in ins:
							function_string += " File"
							if in_file in final_files:
								intermediate_files += [in_file]
								final_files.remove(in_file)
							elif in_file not in intermediate_files and in_file not in original_files:
								original_files += [in_file]
						function_string += "\n"
						for out_file in outs:
							function_string += "#PRUNE_OUTPUT %s\n"%(out_file)
							final_files += [out_file]

						function_string += "\n\n"
						for arg_i,in_file in enumerate(ins):
							function_string += "mv ${%i} %s\n"%(arg_i+1,in_file)
							
						function_string += "\n\n"+line+"\n"
						func_id = 'Function%03d'%next_func_id
						next_func_id += 1

						functions[func_id] = ["\\n".join( function_string.splitlines() ),ins,outs]

						ins = outs = []

			for ofile in original_files:
				line = "PUT %s AS %s"%(ofile, ofile)
				print line
				done = process_line(line)
				while not done:
					done = process_line(line)

			sorted_functions = sorted(functions.items(), key=operator.itemgetter(0))
			for func_id,data in sorted_functions:
				(func_script,ins,outs) = data
				line = "PUT AS %s \"%s\""%(func_id,func_script)
				print line
				done = process_line(line)
				while not done:
					done = process_line(line)
				line = "%s = %s(%s)\n"%( ', '.join(outs), func_id, ', '.join(ins) )
				print line
				done = process_line(line)
				while not done:
					done = process_line(line)

			line = 'USE wq prune2'
			done = process_line(line)
			while not done:
				done = process_line(line)
			line = 'WORK FINISH'
			done = process_line(line)
			while not done:
				done = process_line(line)

			#print 'Remember to assign compute resource and perform WORK to compute the results. Then, use the commands below to get the final files and store them in the working directory.'
			for ffile in final_files:
				line = "GET %s AS %s"%(ffile, ffile)
				print line
				done = process_line(line)
				while not done:
					done = process_line(line)

			return True




		elif line.upper().startswith('IMPORT'):
			ar = line.split(' ')
			filename = ar[1]
			i = 2
			while i < len(ar):
				item = ar[i]
				if item == 'AS':
					destination = ar[i+1]
					i += 1
				elif item == 'FROM':
					repo_name = ar[i+1]
					i += 1
				i += 1

			#zf = zipfile.ZipFile(destination, mode='w', compression=zipfile.ZIP_DEFLATED)
			zf = zipfile.ZipFile(filename, mode='r')
			try:
				plan = zf.read('prune_cmds.plan')
				for line in plan.splitlines(True):
					print line
					if line.upper().startswith('PUT'):
						ar = line.split()
						puid = ar[1]
						pname = ar[3]
						pathname = lib.storage_pathname(puid)
						f = open(pathname,'w')
						f.write(zf.read(puid))
						f.close()
						
						ids = lib.getDataIDs(filename,puid)
						ids['pname'] = pname
						print ids

						if lib.store_file(None, puid, pack=ids['pack']):
							op_id = lib.putMetaData(ids, cmd_id)
						else:
							print 'There was a problem with the command'
					elif line.upper().startswith('#'):
						extra = {}
						matchObj = re.match( r'#([^=]+)=([^\(]*)\(([^/)]*)', line, re.M|re.I)
						extra['vars'] = matchObj.group(1).split(',')
						extra['funcname'] = matchObj.group(2).strip()
						extra['args'] = matchObj.group(3).split(',')
						for i, assign in enumerate(extra['vars']):
							extra['vars'][i] = assign.strip()
						for i, arg in enumerate(extra['args']):
							extra['args'][i] = arg.strip()
						print 'Skipping names for now:',line
						pass
					else:
						res = lib.eval(line, 0, cmd_id, extra)
						extra = None
						#print res
				#print plan
				print '---'
				for fname in zf.namelist():
					if fname!='prune_cmds.plan':
						print fname
						#data = zf.read(fname)
						#with open()
				#zf.writestr('prune_cmds.plan', '\n'.join(lines))
				#for pathname in files:
				#	zf.write(pathname,pathname.split('/')[-1])
			finally:
				zf.close()
			#print lines,files
			
			return True


		elif line.upper().startswith('ORIGIN'):
			ar = line.split(' ')
			lines,files = lib.origin(ar[1])
			for line in lines:
				print line
			return True

		elif line.upper().startswith('FLOW'):
			return True

		elif line.upper().startswith('STORE'):
			ar = line.split(' ')
			fold_filename = ar[1]
			unfold_filename = ar[2]
			lib.add_store(fold_filename,unfold_filename)			
			return True

		else:
			res = lib.eval(line, 0, cmd_id)
			return True
	#except OperationalError:
	#	print 'There was a problem with the database. Please make sure you are not trying to run two sessions of Prune using the same database.'
	except KeyboardInterrupt:
		print ''
	except Exception, e:
		
		print 'Prune couldn\'t understand that command. Please refer to the manual for help.'
		debug('Exception on:'+line, traceback.format_exc())
		return True
	sys.stdout.flush()



def pretty_date(ts=False):
    from datetime import datetime
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')



try:
	db_version = database.initialize(db_pathname)
	lib.initialize(data_folder, sandbox_prefix, hadoop_data)
	with open(config_file,'w') as f:
		f.write('data_folder\t%s\n'%data_folder)
		f.write('database\t%s\n'%db_pathname)
		f.write('sandbox\t%s\n'%sandbox_prefix)
	


except Exception as e:
	print traceback.format_exc()

if reset_all:
	lib.truncate()
	database.truncate()
	print 'PRUNE RESET'

elif run_lines:
	for line in run_lines:
		line = line.strip()
		print line
		done = False
		while not done:
			done = process_line(line)

elif run_filename:
	if run_filename=='stdin':
		lines = sys.stdin
	else:
		lines = [line.strip() for line in open(run_filename)]
	for line in lines:
		print line
		done = False
		while not done:
			done = process_line(line)
else:
	user_interface()
	terminate = True
	lib.terminate_now()
	print 'PRUNE terminated'





