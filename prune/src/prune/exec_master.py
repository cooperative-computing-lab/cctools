# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, time, sys, traceback
from threading import Thread
import json as jsonlib
import subprocess, hashlib
import shutil

from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi

from work_queue import *


class Master:
	debug = True

	def __init__( self, master ):
		self.master = master
		self.concurrency = master.cfg['cores']
		self.workers = []
		
		self.calls = {}
		self.running = []
		self.queued = []
		self.delayed = []

		self.wq_port = WORK_QUEUE_DEFAULT_PORT

		self.start()
		
		

	def start( self ):
		print 'Allocating %i local workers.' % self.concurrency

		self.thread = Thread( target=self.monitor_loop, args=([  ]) )
		self.thread.daemon = True
		self.thread.start()

		try:
			self.wq = WorkQueue(self.wq_port)
		except:
			print "Instantiation of Work Queue failed!"



	def stop( self ):
		self.thread.stop()
		self.workers = []
		for k, worker in enumerate( self.workers ):
			subprocess.Popen.kill( worker.process )
			shutil.rmtree( worker.sandbox )
		self.running = []
		self.queued = self.calls.items()
		self.delayed = []

	def restart( self ):
		self.stop()
		self.start()



	def debug_print( self, *args ):
		if self.debug:
			for arg in args:
				print arg,
			print ''

	def debug_file( self, filename ):
		if self.debug:
			if os.path.isfile( filename ):
				afile = open( filename, 'rb' )
				buf = afile.read( 2048 )
				while len( buf ) > 0:
					print buf,
					buf = afile.read( 2048 )

	def monitor_loop( self ):
		while not self.master.terminate:
			for k, worker in enumerate( self.workers ):
				if worker.process.poll() is not None:
					(stdout, stderr) = worker.process.communicate()

					if worker.process.returncode != 0:
						print 'return:', worker.process.returncode
						print 'cmd:', worker.call['cmd']
						print 'stdout:', stdout
						print 'stderr:', stderr
					else:
						self.debug_print( "\ncmd (%s) succeeded" % ( worker.call['cmd'] ) )
						if len(stdout)>0:
							print 'stdout:', stdout
						if len(stderr)>0:
							print 'stderr:', stderr
					full_content = True
					results = []
					sizes = []
					for i, src in enumerate( worker.call['returns'] ):

						if os.path.isfile( worker.sandbox + src ):
							#combining the following lines would be more efficient at some point (if possible)
							key, size = self.hashfile( worker.sandbox + src )
							if size>0:
								results.append( key )
								sizes.append( size )
								node = self.master.db.fetch( key )
								#if not node: # commented out to allow for multiple source files
								filename = 'tmp_'+key
								shutil.move( worker.sandbox + src, filename )
								fl = File( key=key, tree_key=worker.key+str(i), path=filename, size=size )
								self.master.db.store( fl )

						else:
							self.debug_print( 'sandbox return file not found: '+src )
							full_content = False
					if full_content:
						self.debug_print( 'deleted sandbox: '+worker.sandbox )
						'''
						for root, dirs, files in os.walk(worker.sandbox, topdown=False):
							for name in files:
								os.remove(os.path.join(root, name))
							for name in dirs:
								os.rmdir(os.path.join(root, name))
						'''
						#shutil.rmtree( worker.sandbox )
					else:
						self.debug_print( 'sandbox kept: '+worker.sandbox )
						self.debug_print( 'sandbox cmd: '+worker.call['cmd'] )

					duration = time.time() - worker.start
					ex = Exec( key=worker.key+'_', duration=duration, body={'results':results, 'sizes':sizes} )
					self.master.db.store( ex )
						


					del self.workers[k]



			while len( self.workers ) < self.concurrency:
				key = self.dequeue()
				call = None
				if key:
					call = self.calls[key]
				else:
					for k, key2 in enumerate( self.delayed ):
						call = self.calls[key2]
						key = key2
						if self.isready( call ):
							break;
						else:
							call = None
					if call:
						del self.delayed[k]

				if not call:
					#if len( self.workers ) == 0:
					time.sleep(1)

					break
				sandbox = self.master.sandbox_drive.new_sandbox()
				worker = Worker( call, sandbox, self.master.db )
				self.workers.append( worker )




	def enqueue( self, call_obj ):
		self.calls[call_obj.key] = call_obj
		if self.isready( call_obj ):
			self.queued.append( call_obj.key )
		else:
			self.delayed.append( call_obj.key )




	def dequeue( self ):
		if self.queued:
			return self.queued.pop(0)
		else:
			return None



	def isready( self, call_obj ):
		for f in call_obj.body['args']:
			if not self.master.db.exists( f ):
				return False
		return True


	def hashfile( self, fname, blocksize=65536 ):
		key = hashlib.sha1()
		afile = open( fname, 'rb' )
		buf = afile.read( blocksize )
		length = len( buf )
		while len( buf ) > 0:
			key.update( buf )
			buf = afile.read( blocksize )
			length += len( buf )
		return key.hexdigest(), length
		
		

class Worker:
	process = None
	debug = True
	key = None
	call = None
	sandbox = None
	env = None
	execution = {}
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	def __init__( self, call_obj, sandbox, db ):
		self.key = call_obj.key
		self.sandbox = sandbox
		self.db = db
		self.start = time.time()

		call_body = call_obj.body
		self.call = call_body
		
		#self.debug_print( 'Executing the call below in the sandbox:',sandbox )
		#self.debug_print( jsonlib.dumps(self.call, sort_keys=True, indent=4, separators=(',', ': ')) )


		all_args = call_body['args']
		all_params = call_body['params']
		env_names = []


		env_key = call_body['env']
		env_body = targz_env_body = None
		if env_key != self.nil:
			node = self.db.fetch( env_key )
			env_body = node.obj.body
			if env_body['engine'] == 'targz':
				targz_env_body = env_body
			elif env_body['engine'] == 'umbrella' and 'targz_env' in env_body:
				node = self.db.fetch( env_body['targz_env'] )
				targz_env_body = node.obj.body


		my_env = os.environ
		#for evar, key in call_body['env_vars'].iteritems():
		#	body = self.db.fetch_body( key )
		#	my_env[evar] = body
		#	exec_file.write( "%s=%s;\nexport %s;\n" % (evar, body, evar) )


		targz_lines = []
		if targz_env_body:
			env_names.append('targz')

			all_args += targz_env_body['args']
			all_params += targz_env_body['params']
			
			for param in targz_env_body['params']:
				targz_lines.append( 'tar -zxf %s' % param )




		if not env_body or env_body['engine'] == 'targz':
			for i, arg in enumerate( all_args ):
				param = all_params[i]
				self.db.put_file( arg, self.sandbox + param )

			exec_file = open( self.sandbox + 'PRUNE_EXEC', 'w+' )
			exec_file.write( "#!/bin/sh\n\n" )
			for line in targz_lines:
				exec_file.write( line+"\n" )
			exec_file.write( 'chmod 755 *\n' )
			exec_file.write( call_body['cmd'] + "\n" )
			exec_file.close()

			os.chmod( self.sandbox + 'PRUNE_EXEC', 0755);
			self.process = subprocess.Popen( "./PRUNE_EXEC", cwd=sandbox, env=my_env,
				stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )

		elif env_body['engine'] == 'umbrella':
			env_names.append('umbrella')

			prune_folder = '/tmp/prune5/'
			cache_folder = prune_folder + 'prune_cache/'
			if not os.path.isdir( cache_folder ):
				os.makedirs( cache_folder, 0755 );
			virtual_folder = '/tmp/'

			umbrella_file = which('umbrella')
			filename = prune_folder + 'UMBRELLA_EXECUTABLE'
			if not os.path.isfile( filename ):
				shutil.copy( umbrella_file[0], filename )

			arg_str = '  %sPRUNE_EXEC=%s' % (virtual_folder, self.sandbox + 'PRUNE_EXEC')
			ret_str = ''
			for i, arg in enumerate( all_args ):
				filename = cache_folder + arg
				if not os.path.isfile( filename ):
					self.db.copy_file( arg, filename )

				param = all_params[i]
				arg_str += ', %s=%s' % (virtual_folder+param,filename)

			for r,rtrn in enumerate(call_body['returns']):
				#filename = cache_folder + 'prune_cache/' + self.key + str(r)
				filename = self.sandbox + rtrn
				if os.path.isfile( filename ):
					os.remove( filename )
				ret_str += ', %s=%s' % (virtual_folder+rtrn,filename)

			sub_cmd = ''
			if targz_lines:
				sub_cmd = '; '.join(targz_lines) + '; '
			sub_cmd += call_body['cmd']
			
			cmd = '../../UMBRELLA_EXECUTABLE'
			cmd += ' --spec spec.umbrella --localdir %s' % ( prune_folder )
			cmd += ' --sandbox_mode parrot --log umbrella.log'
			cmd += ' --inputs "%s"' % (arg_str[2:])
			cmd += ' --output "%s"' % (ret_str[2:])
			cmd += ' run "%s"' % sub_cmd

			#shutil.copy( , self.sandbox + 'spec.umbrella' )
			for a, arg in enumerate(env_body['args']):
				param = env_body['params'][a]
				self.db.copy_file( arg, self.sandbox + param )

			uexec_file = open( self.sandbox + 'UMBRELLA_EXEC', 'w+' )
			uexec_file.write( "#!/bin/sh\n\n" )
			uexec_file.write( cmd+"\n" )
			uexec_file.close()

			os.chmod( self.sandbox + 'UMBRELLA_EXEC', 0755);
			self.process = subprocess.Popen( "./UMBRELLA_EXEC", cwd=sandbox, env=my_env,
				stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )


		self.debug_print( 'Executing cmd (%s) in a %s sandbox: %s\n' % ( call_body['cmd'], '+'.join(env_names), self.sandbox ) )



	def debug_print( self, *args ):
		if self.debug:
			for arg in args:
				print arg,
			print ''

	def debug_file( self, filename ):
		if self.debug:
			if os.path.isfile( filename ):
				afile = open( filename, 'rb' )
				buf = afile.read( 2048 )
				while len( buf ) > 0:
					print buf,
					buf = afile.read( 2048 )



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






