# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, time, sys, traceback
from threading import Thread
import subprocess
import shutil

from utils import *
import glob


from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi

concurrency = 16

class Master:
	debug = True


	def hungry( self ):
		if len(self.workers) < concurrency:
			return concurrency - len(self.workers)
		return 0


	def execute( self, call ):
		if self.shutting_down:
			d('exec', 'Skipping request to execute call (shutting down):', call )
		else:
			worker = Worker( call )
			worker.start()
			self.workers.append( worker )




	def __init__( self, parent ):
		self.workers = []
		self.parent = parent
		self.shutting_down = False

		self.thread = Thread( target=self.loop, args=([  ]) )
		self.thread.daemon = True
		self.thread.start()

		#self.run()	
		

	def loop( self ):

		print 'Allocating %i local workers.' % concurrency
		self.workers = []

		while True:

			#glob.starts['exec_local_loop'] = time.time()
			for k, worker in enumerate( self.workers ):
				if worker.process.poll() is not None:
					(stdout, stderr) = worker.process.communicate()
					print "Local execution complete (%s): %s (return code %d)" % (worker.call.key, worker.call.body['cmd'], worker.process.returncode)
					
					if worker.process.returncode != 0:
						if len(stdout)>0:
							d( 'exec', 'stdout:', stdout )
						if len(stderr)>0:
							d( 'exec', 'stderr:', stderr )
					else:
					#	d( 'exec', "cmd (%s) succeeded" % ( worker.call['cmd'] ) )
					#	if len(stdout)>0:
					#		d( 'exec', 'stdout:', stdout )
						if len(stderr)>0:
							print 'stderr:', stderr

					self.parent.end_call( worker.call )
					worker.finish()
					del self.workers[k]
			#glob.sums['exec_local_loop'] += time.time() - glob.starts['exec_local_loop']
			
			#cnt4 += 1
			#if (cnt4%1000) == 0:
			#	print glob.sums
			
			time.sleep(0.01)
		
		

class Worker:
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	def __init__( self, call ):
		self.call = call
		self.process = None

	def start( self ):
		self.db = glob.get_ready_handle( 'db' )

		self.start = time.time()
		self.sandbox = glob.store.new_sandbox()

		call_body = self.call.body
		key = self.call.key

		#self.debug_print( 'Executing the call below in the sandbox:',sandbox )
		#self.debug_print( jsonlib.dumps(self.call, sort_keys=True, indent=4, separators=(',', ': ')) )


		self.args = []
		for arg in call_body['args']:
			self.args.append( arg )

		self.params = []
		for param in call_body['params']:
			self.params.append( param )


		#all_args = call_body['args']
		#all_params = call_body['params']
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

			for arg in targz_env_body['args']:
				self.args += [arg]
			for param in targz_env_body['params']:
				self.params += [param]
				targz_lines.append( 'tar -zxf %s' % param )



		if not env_body or env_body['engine'] == 'targz':
			for i, arg in enumerate( self.args ):
				param = self.params[i]
				self.db.put_file( arg, self.sandbox + param )


			exec_lines = []
			for line in targz_lines:
				exec_lines.append( line )
			exec_lines.append( 'chmod 755 *' )
			exec_lines.append( call_body['cmd'] )

			exec_file = open( self.sandbox + 'PRUNE_EXEC', 'w+' )
			exec_file.write( "#!/bin/sh\n\n" )
			for line in exec_lines:
				exec_file.write( line+"\n" )
			exec_file.close()


			os.chmod( self.sandbox + 'PRUNE_EXEC', 0755);
			self.process = subprocess.Popen( "./PRUNE_EXEC", cwd=self.sandbox, env=my_env,
				stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )

		elif env_body['engine'] == 'umbrella':

			for arg in env_body['args']:
				self.args += [arg]
			for param in env_body['params']:
				self.params += [param]

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
			for i, arg in enumerate( self.args ):
				param = self.params[i]
				filename = cache_folder + arg
				print arg, param, filename

				if not os.path.isfile( filename ):
					self.db.copy_file( arg, filename )

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
			for a, arg in enumerate(self.args):
				param = self.params[a]
				self.db.copy_file( arg, self.sandbox + param )

			uexec_file = open( self.sandbox + 'UMBRELLA_EXEC', 'w+' )
			uexec_file.write( "#!/bin/sh\n\n" )
			uexec_file.write( cmd+"\n" )
			uexec_file.close()

			os.chmod( self.sandbox + 'UMBRELLA_EXEC', 0755);
			self.process = subprocess.Popen( "./UMBRELLA_EXEC", cwd=self.sandbox, env=my_env,
				stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )


		#d( 'exec', 'Executing cmd (%s) in a %s sandbox: %s\n' % ( call_body['cmd'], '+'.join(env_names), self.sandbox ) )
		print 'Executing cmd (%s) in a %s sandbox: %s' % ( call_body['cmd'], '+'.join(env_names), self.sandbox )






	def finish( self ):
		call_body = self.call.body
		full_content = True
		results = []
		sizes = []
		for i, src in enumerate( call_body['returns'] ):

			if os.path.isfile( self.sandbox + src ):
				#combining the following lines would be more efficient at some point (if possible)

				#glob.starts['exec_local_hash'] = time.time()
				key, size = hashfile( self.sandbox + src )
				#glob.sums['exec_local_hash'] += time.time() - glob.starts['exec_local_hash']
				if size>0:
					results.append( key )
					sizes.append( size )
					node = self.db.fetch( key )
					#if not node: # commented out to allow for multiple source files
					filename = glob.store.tmp_file_path( key )
					#glob.starts['exec_local_move'] = time.time()
					shutil.move( self.sandbox + src, filename )
					fl = File( key=key, tree_key=self.call.key+str(i), path=filename, size=size )
					#glob.sums['exec_local_move'] += time.time() - glob.starts['exec_local_move']
					self.db.store( fl )
				else:
					full_content = False

			else:
				d( 'exec', 'sandbox return file not found: '+src )
				full_content = False
		if full_content:
			d( 'exec', 'sandbox kept at: '+self.sandbox )
			'''
			for root, dirs, files in os.walk(self.sandbox, topdown=False):
				for name in files:
					os.remove(os.path.join(root, name))
				for name in dirs:
					os.rmdir(os.path.join(root, name))
			'''
			#shutil.rmtree( worker.sandbox )
			duration = time.time() - self.start
			ex = Exec( key=self.call.key+'_', duration=duration, body={'results':results, 'sizes':sizes} )
			self.db.store( ex )
		else:
			d( 'exec', 'sandbox kept: '+self.sandbox )




