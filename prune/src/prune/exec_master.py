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

		self.start()
		
		

	def start( self ):
		print 'Allocating %i local workers.' % self.concurrency

		self.thread = Thread( target=self.monitor_loop, args=([  ]) )
		self.thread.daemon = True
		self.thread.start()



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
					full_content = True
					results = []
					sizes = []
					for i, src in enumerate( worker.call['returns'] ):

						if os.path.isfile( worker.sandbox + src ):
							#combining the following lines would be more efficient at some point (if possible)
							key, size = self.hashfile( worker.sandbox + src )
							results.append( key )
							sizes.append( size )
							node = self.master.db.fetch( key )
							if not node:
								data = ''
								f = open( worker.sandbox + src, 'r' )
								buf = f.read(1024)
								while buf:
									data += buf
									buf = f.read(1024)
								
								fl = File( key=key, tree_key=worker.key+str(i), body=data )
								self.master.db.store( fl )

						else:
							self.debug_print( 'sandbox return file not found: '+src )
							full_content = False
					if full_content:
						self.debug_print( 'deleting sandbox.' )
						shutil.rmtree( worker.sandbox )
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
		self.call_obj = call_obj
		self.call = call_obj.body
		self.sandbox = sandbox
		self.db = db
		self.start = time.time()

		#self.debug_print( 'Executing the call below in the sandbox:',sandbox )
		#self.debug_print( jsonlib.dumps(self.call, sort_keys=True, indent=4, separators=(',', ': ')) )

		exec_file = open( self.sandbox + 'PRUNE_EXEC', 'w+' )
		exec_file.write( "#!/bin/sh\n\n" )

		for i, input_file in enumerate( self.call['args'] ):
			target = self.call['params'][i]
			self.db.put_file( input_file, self.sandbox + target )
			#self.db.symlink( input_file, self.sandbox + target )

		env_key = self.call['env']
		if env_key == self.nil:
			cmd = self.call['cmd']
			self.debug_print( 'Executing cmd (%s) in a local sandbox: %s\n' % ( self.call['cmd'], sandbox ) )

		else:
			node = self.db.fetch( env_key )
			env_body = node.obj.body
			
			for a, arg in enumerate(env_body['args']):
				param = env_body['params'][a]
				self.db.put_file( arg, self.sandbox + param )

			if env_body['engine'] == 'targz':
				unzips = ''
				for param in env_body['params']:
					unzips += 'tar -zxvf %s; ' % param
				cmd = unzips + self.call['cmd']
				self.debug_print( 'Executing cmd (%s) in a targz sandbox: %s\n' % ( self.call, self.sandbox ) )

			elif env_body['engine'] == 'umbrella':
				umbrella_file = which('umbrella')
				os.symlink( umbrella_file[0], self.sandbox + 'UMBRELLA_EXECUTABLE' )

				arg_str = ''
				for param in env_body['params']:
					arg_str += ', %s=%s' % (param,param)

				cmd = './UMBRELLA_EXECUTABLE --spec umbrella_spec.json --localdir ./tmp/'
				cmd += ' --output ./final_output --sandbox_mode parrot --log umbrella.log'
				cmd += ' --cvmfs_http_proxy http://cache01.hep.wisc.edu:3128 run "%s"'%(self.call['cmd'])
				self.debug_print( 'Executing cmd (%s) in a umbrella sandbox: %s\n' % ( call, sandbox ) )

				#final_cmd = 'chmod 755 PRUNE_RUN PRUNE_EXECUTOR; ./PRUNE_RUN'
		my_env = os.environ
		for evar, key in self.call['env_vars'].iteritems():
			body = self.db.fetch_body( key )
			my_env[evar] = body
			exec_file.write( "%s=%s;\nexport %s;\n" % (evar, body, evar) )

		exec_file.write( cmd )
		exec_file.close()
		os.chmod( self.sandbox + 'PRUNE_EXEC', 0755);
		self.process = subprocess.Popen( "./PRUNE_EXEC", cwd=sandbox, env=my_env,
			stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )




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






