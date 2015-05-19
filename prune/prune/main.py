# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, traceback, os.path
import time, threading

import readline, re

import zipfile
import operator

from work_queue import *
import prunelib
prunedb = prunelib.getdb()


HOME = os.path.expanduser("~")
CWD = os.getcwd()
config_file = CWD+'/.prune.conf'
run_filename = run_lines = None
reset_all = False

argi = 1
while argi<len(sys.argv):
	arg = sys.argv[argi]
	if arg=='--mf':
		argi += 1
		mf_filename = sys.argv[argi]
	elif arg=='--plan':
		argi += 1
		run_filename = sys.argv[argi]
	elif arg=='--run':
		argi += 1
		run_lines = sys.argv[argi].split(';')
	elif arg=='--cwd':
		argi += 1
		nwd = sys.argv[argi]
		if nwd[0]=='/':
			os.chdir(nwd)
		else:
			os.chdir(os.getcwd()+'/'+nwd)
		CWD = os.getcwd()
		config_file = CWD+'/.prune.conf'
	elif arg=='-reset' or arg=='--reset':
		reset_all = True

	else:
		run_filename = arg
	argi += 1


terminate = False
block = False
hadoop_data = False

repo_puid = prunelib.new_puid()
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
			elif meta_type=='repo_puid':
				repo_puid = value
else:
	data_folder = '/tmp/prune/data/'
	db_pathname = '/tmp/prune/___prune.db'
	sandbox_prefix = '/tmp/prune/sandbox/'

	line = raw_input('Enter location for data files [%s]: '%data_folder)
	if len(line)>0:
		if line[-1] != '/':
			line = line + '/'
		data_folder = line
	line = raw_input('Enter filepath for the database [%s]: '%db_pathname)
	if len(line)>0:
		db_pathname = line
	line = raw_input('Enter location for execution sandboxes [%s]: '%sandbox_prefix)
	if len(line)>0:
		if line[-1] != '/':
			line = line + '/'
		sandbox_prefix = line


def prompt():
	return 'PRUNE$ '

forever_back = '\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b'
	
def user_interface():
	global block, terminate, forever_back, wait_start
	spaces = '                                        '
	cmds = prunedb.cmds_get()
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
		except KeyboardInterrupt:
			print 'PRUNE exiting.'
			return
		except Exception:
			print traceback.format_exc()


work_start = None
last_report = None
def process_line(line):
	global block, work_start, last_report, hadoop_data, terminate
	cmd_id = prunedb.cmd_ins(line)
	try:
		print line, ord(line[0])
		if len(line)==0 or line[0]=='#':
			return True
		elif line.startswith('HELP'):
			message = '''Hello
			'''
			print message
			return True
		elif line.startswith('PUT'):
			wait = True
			filename = pname = puid = data = form = None
			if line.find('"')>=0:
				data = line[line.find('"')+1:line.rfind('"')]
			line = line[:line.find('"')-1]+line[line.rfind('"')+1:]
			ar = line.split()
			i = 1
			while i < len(ar):
				item = ar[i]
				if item == 'AS':
					i += 1
					pname = ar[i]
				elif item == 'UUID' or item == 'GUID':
					i += 1
					puid = ar[i]
				elif item == 'ENV':
					i += 1
					form = ar[i]
				elif not filename:
					filename = item
				#elif item == 'LAZY':
				#	wait = False
				i += 1
			if not pname and filename:
				pname = filename
			if filename:
				ids = prunelib.getDataIDs(filename)
				if not ids:
					print 'File does not exist: %s'%(filename)
					return True
			elif data:
				ids = prunelib.getDataIDs2(data)
				if not ids:
					print 'File does not exist: %s'%(data)
					return True
			ids['pname'] = pname
			if ids['exists']:
				print 'That file already exists with that name: %s=%s'%(pname,ids['puid'])
				if ids['pname']:
					prunedb.var_set(ids['pname'], ids['puid'])
				if form:
					prunelib.env_name(pname)
			elif data:
				prunelib.store_data(data,ids['puid'],wait)
				op_id = prunelib.putMetaData(ids, cmd_id)
			elif filename:
				op_id = prunelib.putMetaData(ids, cmd_id)
				if form:
					prunelib.store_file(filename,ids['puid'],wait,form,ids['pack'])
					prunelib.env_name(pname)
				else:
					prunelib.store_file(filename,ids['puid'],wait,pack=ids['pack'])
			return True

		elif line.startswith('USE'):
			ar = line.split(' ')
			resource_type = ar[1]
			if 'TRY_LOCAL' in line:
				local_fs = True
			else:
				local_fs = False
			if resource_type=='local':
				concurrency = int(ar[2])
				prunelib.useLocal(concurrency,local_fs)
			elif resource_type=='wq':
				master_name = ar[2]
				prunelib.useWQ(master_name,local_fs)
			return True

		elif line.startswith('ENV'):
			ar = line.split()

			pname = ar[1]
			prunelib.env_name(pname)

			return True


		elif line.startswith('GET'):
			wait = True
			filename = expr = None
			ar = line.split(' ')
			i = 1
			while i < len(ar):
				item = ar[i]
				if not expr:
					expr = item
				elif item == 'AS':
					filename = ar[i+1]
					i += 1
				#elif item == 'LAZY':
				#	wait = False
				if not filename:
					filename = expr
				i += 1
			return prunelib.getFile(expr,filename,wait)
			

		elif line.startswith('WORK'):
			if not work_start:
				work_start = time.time()
				last_report = work_start - 5


			res = prunelib.getQueueCounts()
			output = ''
			some_in_progress = False
			for r in res:
				output += '%s queue size: %i   '%( r['queue'], r['cnt'] )
				if r['queue']=='Running' or r['queue']=='Run':
					some_in_progress = True
			if ((time.time()-last_report)>15):
				now = time.time()
				minutes = (time.time()-work_start)/60
				seconds = (time.time()-work_start)%60

				print output, time.strftime("%H:%M:%S"), now, '(%02dm%02ds)'%( minutes, seconds )
				last_report = now


			prunelib.wq_check()
			prunelib.local_check()
			
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
			elif not some_in_progress:
				print 'Work complete, or no execution resources available.'
				minutes = (time.time()-work_start)/60
				seconds = (time.time()-work_start)%60
				print 'PRUNE finished executing after: %02dm%02ds'%( minutes, seconds )
				#print 'DB read, write count:', prunedb.get_query_cnts()

				work_start = None
				if ar[-1]=='TERMINATE':
					terminate = True
				return True
			else:
				#time.sleep(1)
				return False
			



		elif line.startswith('LOCATE'):
			ar = line.split(' ')
			name = ar[1]
			now = True if len(ar)>2 and ar[2]=='NOW' else False
			res = prunelib.locate_pathname(name)
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

		elif line.startswith('RUN'):
			start_time = time.time()
			ar = line.split(' ')
			name = ar[1]
			try:
				res = prunelib.locate_copies(name)
				cache_filename = prunelib.storage_pathname(res[0]['puid'])
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
			except Exception as e:
				print e
			return True

		elif line.startswith('STATUS'):
			if not wait_start:
				wait_start = time.time()

			ar = line.split(' ')
			if len(ar)>1:
				timeout = float(ar[2])
			else:
				timeout = 0.0

			res = prunelib.getQueueCounts()
			output = ''
			for r in res:
				output += '%s queue size: %i   '%( r['queue'], r['cnt'] )
			print forever_back+output

			if len(res)==1 and res[0]['queue']=='Complete':
				wait_start = None
				return True
			elif ( (time.time()-wait_start) > timeout ):
				wait_start = None
				return True
			else:
				#time.sleep(10)
				return False

		elif line.startswith('CUT'):
			res = prunedb.var_getAll()
			for r in res:
				prunedb.var_unset(r['name'])
			return True
				
		elif line.startswith('LS'):
			ar = line.split()
			if len(ar)>1 and ar[1]!='*':
				keyword = ar[1]
			else:
				keyword = None
			res = prunedb.var_getAll()
			for r in res:
				if keyword:
					if keyword in r['name']:
						print r['name'],' -> ',r['puid'], '  @', r['set_at']
				else:
					print r['name'],' -> ',r['puid'], '  @', r['set_at']
			return True
				

		elif line.startswith('EXPORT'):
			ar = line.split(' ')
			lines,files = prunelib.origin(ar[1])
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




		elif line.startswith('MF'):
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
						if line.startswith('LOCAL'):
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




		elif line.startswith('IMPORT'):
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
					if line.startswith('PUT'):
						ar = line.split()
						puid = ar[1]
						pname = ar[3]
						pathname = prunelib.storage_pathname(puid)
						f = open(pathname,'w')
						f.write(zf.read(puid))
						f.close()
						
						ids = prunelib.getDataIDs(filename,puid)
						ids['pname'] = pname
						print ids

						if prunelib.store_file(None, puid, pack=ids['pack']):
							op_id = prunelib.putMetaData(ids, cmd_id)
						else:
							print 'There was a problem with the command'
						#prunedb.io_ins(op_id, 'O', puid, pname, repo_id, None, 0)
					elif line.startswith('#'):
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
						res = prunelib.eval(line, 0, cmd_id, extra)
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


		elif line.startswith('ORIGIN'):
			ar = line.split(' ')
			lines,files = prunelib.origin(ar[1])
			for line in lines:
				print line
			return True

		elif line.startswith('FLOW'):
			return True

		elif line.startswith('STORE'):
			ar = line.split(' ')
			fold_filename = ar[1]
			unfold_filename = ar[2]
			prunelib.add_store(fold_filename,unfold_filename)			
			return True

		elif line.startswith('RESET'):
			print data_folder
			prunelib.truncate()
			prunedb.truncate()
			return True
		else:
			res = prunelib.eval(line, 0, cmd_id)
			return True

	except Exception, e:
		print traceback.format_exc()
		return True
	sys.stdout.flush()



try:

	prunedb.initialize(db_pathname)
	#Be sure to initialize the database first!
	prunelib.initialize(data_folder, sandbox_prefix, hadoop_data)
	with open(config_file,'w') as f:
		f.write('data_folder\t%s\n'%data_folder)
		f.write('database\t%s\n'%db_pathname)
		f.write('sandbox\t%s\n'%sandbox_prefix)
		f.write('repo_puid\t%s\n'%repo_puid)



except Exception as e:
	print traceback.format_exc()

if reset_all:
	prunelib.truncate()
	prunedb.truncate()
	print 'PRUNE RESET'

elif run_lines:
	for line in run_lines:
		line = line.strip()
		print line
		done = False
		while not done:
			done = process_line(line)

elif run_filename:
	lines = [line.strip() for line in open(run_filename)]
	for line in lines:
		print line
		done = False
		while not done:
			done = process_line(line)
else:
	user_interface()
	terminate = True
	prunelib.terminate_now()
	print 'PRUNE terminated'




