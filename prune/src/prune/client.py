# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback
import json as jsonlib
import subprocess, shutil
import Queue

import glob
from utils import *
from class_item import Item
from parser import Parser
from db_sqlite import Database

ROLE = 'Client'

class Connect:
	debug = False
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
	glob.workflow_id = uuid()



	def envi_add( self, **kwargs ):
		timer.start('client.envi_add')
		if 'engine' in kwargs:
			if kwargs['engine'] == 'wrapper':
				obj = {'engine':kwargs['engine'], 'open':kwargs['open'], 'close':kwargs['close'], 'args':kwargs['args'], 'params':kwargs['params']}
				it = Item( type='envi', body=obj )
				glob.db.insert(it)
				return it.cbid

				'''
			elif kwargs['engine'] == 'targz':
				if glob.store:
					filename = glob.tmp_file_path()
				else:
					filename = './tmp_'+uuid()
				zip_cmd = "tar -hzcvf %s %s" % (filename, ' '.join(kwargs['folders2include']))
				p = subprocess.Popen( zip_cmd , stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
				(stdout, stderr) = p.communicate()
				#print 'stdout:',stdout
				#print 'stderr:',stderr
				


				it = Item( type='file', path=filename )
				glob.db.insert(it)
				#self.holds[it.cbid] = it


				mesg = Mesg( action='have', key=key )
				self.out_msgs.put_nowait( {'mesg':mesg} )

				fl = File( key=key, path=filename, size=length, workflow_id=self.workflow_id )
				self.files[key] = fl

				obj = {'engine':'targz', 'args':[key], 'params':['_folders2include.tar.gz']}

				env_key = hashstring( str(obj) )

				mesg = Mesg( action='save' )
				en = Envi( key=env_key, body=obj, workflow_id=self.workflow_id )
				# Send the server this environment specification
				self.out_msgs.put_nowait( {'mesg':mesg, 'body':en} )

				return env_key
					
			elif kwargs['engine'] == 'umbrella':
				
				filename = kwargs['spec']
				key, length = hashfile( filename )
				mesg = Mesg( action='have', key=key )
				self.out_msgs.put_nowait( {'mesg':mesg} )

				fl = File( key=key, path=filename, size=length, workflow_id=self.workflow_id )
				self.files[key] = fl

				obj = {'engine':'umbrella', 'args':[key], 'params':['spec.umbrella']}
				if 'targz_env' in kwargs:
					obj['targz_env'] = kwargs['targz_env']

				env_key = hashstring( str(obj) )
				
				mesg = Mesg( action='save' )
				en = Envi( key=env_key, body=obj, workflow_id=self.workflow_id )
				# Send the server this environment specification
				self.out_msgs.put_nowait( {'mesg':mesg, 'body':en} )

				return env_key
				'''

			else:
				timer.stop('client.envi_add')
				self.op_cnt += 1
				return self.nil
		else:
			timer.stop('client.envi_add')
			self.op_cnt += 1
			return self.nil

	def file_add( self, filename ):
		timer.start('client.file_add')

		#start = time.time()
		it = Item( type='file', path=filename, new_path=glob.tmp_file_directory+uuid() )
		#elapsed = time.time()-start
		glob.db.insert(it)

		timer.stop('client.file_add')

		self.op_cnt += 1
		return it.cbid


	def data_add( self, data ):
		timer.start('client.data_add')

		it = Item( type='file', body=data )
		glob.db.insert(it)

		timer.stop('client.data_add')
		self.op_cnt += 1
		return it.cbid



	def file_dump( self, key, filename ):
		timer.start('client.file_dump')

		glob.db.dump( key, filename )
		
		timer.start('client.file_dump')


	def call_add( self, returns, env, cmd, args=[], params=[], types=[], env_vars={}, precise=True):
		timer.start('client.call_add')
		obj = {'returns':returns, 'env':env, 'cmd':cmd, 'args':args, 'params':params, 'types':types, 'env_vars':env_vars, 'precise':precise}
		it = Item( type='call', body=obj )
		glob.db.insert( it )
		glob.db.task_add( it )
		results = []
		for i in range( 0, len(returns) ):
			results.append(it.cbid+':'+str(i))
		timer.stop('client.call_add')
		self.op_cnt += 1
		#if (self.op_cnt%100)==0:
		#	time.sleep(0.1)
		return results


	def step_add( self, name ):
		self.wait()
		self.report()
			

		print name
		glob.workflow_step = name
		print '=================='
		print '------------------'
		print '=================='
		print '------------------'
		print '=================='
		print '------------------'
		print '=================='
		


	def report( self ):
		print '------------------'
		print '------------------'
		print '------------------'
		timer.report()
		print '------------------'
		print '------------------'
		print '------------------'
		timer.reset()

		if glob.workflow_step and glob.workflow_step != 'Stage 0':
			cmd = 'prune_worker'
			#if glob.workflow_step=='Stage 5':
			#raw_input("Press enter to execute %s"%glob.workflow_step)

			#print 'Executing', glob.workflow_step
			#p = subprocess.Popen( cmd , stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
			#(stdout, stderr) = p.communicate()
			#print 'stdout:',stdout
			#print 'stderr:',stderr


		#val = raw_input("Press enter to continue...")
		timer.report()
		timer.reset()


	def wait( self ):
		while glob.db.task_remain( glob.workflow_id ):
			time.sleep(3)













	def flow_dump( self, key, filename, depth=1 ):
		flow = {'key':key,'depth':depth}
		mesg = Mesg( action='send', key=key, flow=flow )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		self.dump_flows[key] = filename
		return key


	def flow_add( self, filename ):
		parser = Parser()
		with open(filename, 'r') as f:
			for line in f:
				results = parser.parse(line)
				if results:
					for objects in results:
						i = 0
						while i<len(objects) and len(objects)>0:
							obj = objects[i]
							if (i+1)<len(objects):
								body = objects[i+1]
							else:
								body = None

							if obj['type'] == 'envi':
								mesg = Mesg( action='save' )
								en = Envi( key=obj['key'], body=body, workflow_id=self.workflow_id )
								self.out_msgs.put_nowait( {'mesg':mesg, 'body':en} )

								#en = Envi( obj, body=body )
								#self.index_obj( en )
							elif obj['type'] == 'call':
								mesg = Mesg( action='save' )
								cl = Call( key=obj['key'], body=body, workflow_id=self.workflow_id )
								self.out_msgs.put_nowait( {'mesg':mesg, 'body':cl} )

								#cl = Call( obj, body=body )
								#self.index_obj( cl )
							elif obj['type'] == 'file':
								mesg = Mesg( action='save' )
								fl = File( key=obj['key'], body=body, workflow_id=self.workflow_id )
								# Send the server this file
								self.out_msgs.put_nowait( {'mesg':mesg, 'body':fl} )


								#fl = File( obj, body=body )
								#self.index_obj( fl )
							elif obj['type'] == 'exec':
								#ex = Exec( obj, body=body )
								#self.index_obj( ex )
								pass
							
							i += 2

		return None


	def stats_dump( self, keys, filename ):
		key = uuid()
		flow = {'key':key, 'keys':keys, 'files':False}
		mesg = Mesg( action='send', key=key, flow=flow )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		self.dump_stats[key] = filename
		return key



	'''
	def wait( self ):
		print '.'
		self.waiting = True

		mesg = Mesg( action='ping' )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		
		try:
			while self.waiting or self.sending:
				time.sleep(1)

			while True:
				if len(self.files)<=0 and len(self.dump_flows)<=0 and len(self.dump_files)<=0 and len(self.dump_stats)<=0 :
					print '%i %i %i %i' %(len(self.files), len(self.dump_flows), len(self.dump_files), len(self.dump_stats))
					# Exit when complete
					break
				time.sleep(1)
		except KeyboardInterrupt:
			sys.exit(0)
			#raise KeyboardInterrupt
	'''


	def _receive( self, sock, objects ):
		
		if len(objects) <= 0:
			return
		mesg = body = None
		if len(objects) > 0:
			mesg = Mesg( objects[0] )
		d( ROLE, '---->' + ROLE + ':   \n' + mesg )

		if mesg.action == 'pong':
			self.waiting = False

		elif mesg.action == 'send':
			fl = self.files[mesg.key]
			del self.files[mesg.key]
			mesg.action = 'save'
			self.out_msgs.put_nowait( {'mesg':mesg, 'body':fl} )


		elif mesg.action == 'have':
			if mesg.key in self.files:
				del self.files[mesg.key]


		elif mesg.action == 'save':
			objs = []
			i = 1
			while i<len(objects):
				obj = objects[i]
				if (i+1)<len(objects):
					body = objects[i+1]
				else:
					body = None

				if 'type' not in obj:
					pass
				elif obj['type'] == 'envi':
					en = Envi( obj, body=body )
					objs.append( en )
					d( ROLE, en )
				elif obj['type'] == 'call':
					cl = Call( obj, body=body )
					objs.append( cl )
					d( ROLE, cl )
				elif obj['type'] == 'file':
					fl = File( obj, body=body )
					objs.append( fl )
					d( ROLE, fl )
				elif obj['exec'] == 'exec':
					ex = Exec( obj, body=body )
					objs.append( ex )
					d( ROLE, ex )
				i += 2

			if mesg.flow:
				if mesg.flow['key'] in self.dump_flows:
					filename = self.dump_flows[mesg.flow['key']]
					all_keys = []
					with open( filename, 'wb' ) as f:
						for obj in objs:
							f.write( str(obj) + "\n" )
							all_keys += [obj.key]
					del self.dump_flows[mesg.flow['key']]
					print 'Saved flow with %s as %s.' % (', '.join(all_keys), filename)

				elif mesg.flow['key'] in self.dump_stats:
					filename = self.dump_stats[mesg.flow['key']]
					body = jsonlib.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
					with open( filename, 'wb' ) as f:
						f.write( body + "\n" )
					'''
					all_keys = []
					stats = {'keys':[], 'wall_time':0, 'space':0}
					with open( filename, 'wb' ) as f:
						for obj in objs:
							print obj
							stats['all_keys'] += [obj.key]
							if 'duration' in obj.body:
								stats['wall_time'] += float(obj.body['duration'])
							if 'size' in obj.body:
								stats['keys'] += [int(obj.body[''])]
						f.write( str(mesg) + "\n" )
						f.write( str(stats) + "\n" )
					'''
					del self.dump_stats[mesg.flow['key']]
					print 'Saved stats for %s as %s.' % (', '.join(mesg.flow['keys']), filename)


			else:
				filename = self.dump_files[mesg.key]
				obj = objs[0]
				if obj.size>200:
					d( ROLE, str(obj)[0:20]+'.|.'+str(obj)[-20:] )
				else:
					d( ROLE, obj )
				print 'Saved file %s as %s.' % (obj.key, filename)
				if obj.path:
					shutil.move( obj.path, filename )
				else:
					f = open( filename, 'wb' )
					f.write( str(obj.body) )
					f.close()
				del self.dump_files[mesg.key]

		else:
			print 'unknown action: %s' % meta

		return True

		

	def _send( self, sock, msg ):

		out_str = msg['mesg'] + "\n"

		if 'body' in msg:
			d( ROLE, '<----' + ROLE + ':   \n' + out_str + msg['body'] + "\n" )
			out_str += msg['body'].full_str() + "\n"
		else:
			d( ROLE, '<----' + ROLE + ':   \n' + out_str + "\n\n" )
			out_str += "\n"


		while out_str:
			try:

				sent = sock.send(out_str)
				#print sent,'/',len(out_str),'sent'
				sys.stdout.write('.')
				sys.stdout.flush()
				if sent==0:
					time.sleep(1)
				out_str = out_str[sent:]

			except error as ex:
				#print ex

				if ex.errno == errno.EAGAIN:
					time.sleep(1)
					continue
				raise ex
		return True



	def __init__( self ):
		if not os.path.exists(glob.tmp_file_directory):
			os.makedirs(glob.tmp_file_directory)

		self.op_cnt = 0
		
		glob.ready = True
		glob.db = Database()


	def __init_old__( self, hostname, port):
		self.hostname = hostname
		self.port = port

		self.out_msgs = Queue.Queue()
		self.files = {}
		self.dump_files = {}
		self.dump_flows = {}
		self.dump_stats = {}
		self.request_cnt = 0


		try:
			self.sock = socket( AF_INET, SOCK_STREAM )
			try:
				self.sock.connect(( self.hostname, self.port ))
				fcntl.fcntl(self.sock, fcntl.F_SETFL, os.O_NONBLOCK)

				
				self.connection_thread = Thread( target=self._handler, args=([ self.sock, self.out_msgs ]) )
				self.connection_thread.daemon = True
				self.connection_thread.start()


			except KeyboardInterrupt:
				self.sock.close()
			except error as msg:
				self.sock.close()
		except error as msg:
			self.sock = None




	def _handler( self, sock, out_msgs ):
		mybuffer = ''
		retries = []
		#sock.settimeout(1)
		parser = Parser(file_threshold=1024)
		while True:
			now = time.time()
			while not out_msgs.empty():
				msg = out_msgs.get()
				self.sending = True
				self._send( sock, msg )
				self.sending = False

			
			for i,ar in enumerate( retries ):
				meta, ready_time = ar
				if ready_time < now:
					del retries[i]
					if not self._receive( sock, meta, retry=True ):
						retries.append([ meta, time.time()+3 ])
			
			try:
				raw_data = sock.recv( 4096 )
				if len(raw_data) <= 0:
					# If timed out, exception would have been thrown
					print '\nLost connection...'
					sys.exit(1)
					break
				

				results = parser.parse( raw_data )
				if results:
					for objects in results:
						self._receive( sock, objects )


			except error, e:
				err = e.args[0]
				if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
					time.sleep(0.01)
					continue
				else:
					# a "real" error occurred
					print e
					sys.exit(1)

				raw_data = ''

		
