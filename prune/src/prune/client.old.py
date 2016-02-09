# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback
import json as jsonlib
from threading import Thread
from socket import *
import fcntl, errno
import subprocess, shutil
import Queue

import glob
from utils import *
from class_mesg import Mesg
from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi
from parser import Parser


ROLE = 'Client'

class Connect:
	debug = False
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
	batch_start = time.time()
	batch_cnt = 0
	workflow_id = uuid()



	def stage( self ):
		#returns True if the stage needs to be run, otherwise
		pass


	def env_add( self, **kwargs ):
		if 'engine' in kwargs:
			if kwargs['engine'] == 'targz':
				if glob.store:
					filename = glob.store.tmp_file_path()
				else:
					filename = './tmp_'+uuid()
				zip_cmd = "tar -hzcvf %s %s" % (filename, ' '.join(kwargs['folders2include']))
				p = subprocess.Popen( zip_cmd , stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
				(stdout, stderr) = p.communicate()
				#print 'stdout:',stdout
				#print 'stderr:',stderr
				
				key, length = hashfile( filename )
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
		else:
			return self.nil

	def file_add( self, filename ):
		key, length = hashfile( filename )
		mesg = Mesg( action='have', key=key )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		
		transfer_now = False
		if transfer_now:
			data = ''
			f = open( filename, 'rb' )
			buf = f.read(1024)
			while buf:
				data += buf
				buf = f.read(1024)

			fl = File( key=key, body=data, workflow_id=self.workflow_id )
			self.files[key] = fl  # save the data in case the server asks for it

		else:
			fl = File( key=key, path=filename, size=length, workflow_id=self.workflow_id )
			self.files[key] = fl

		return key

	def data_add( self, data ):
		#data = jsonlib.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
		key = self.hashstring( data )
		mesg = Mesg( action='save' )
		fl = File( key=key, body=data, workflow_id=self.workflow_id )
		# Send the server this file
		self.out_msgs.put_nowait( {'mesg':mesg, 'body':fl} )
		return key

	def file_dump( self, key, filename ):
		mesg = Mesg( action='send', key=key )
		# Ask the server to send this data
		self.out_msgs.put_nowait( {'mesg':mesg} )
		# save the filename for when the data comes
		self.dump_files[key] = filename
		return key

	def call_add( self, returns, env, cmd, args=[], params=[], types=[], env_vars={}, precise=True):
		obj = {'returns':returns, 'env':env, 'cmd':cmd, 'args':args, 'params':params, 'types':types, 'env_vars':env_vars, 'precise':precise}
		if len(types)==0:
			for param in params:
				obj['types'].append('path')   # alternative to 'path' is 'data'

		key = hashstring( str(obj) )

		mesg = Mesg( action='save' )
		cl = Call( key=key, body=obj, workflow_id=self.workflow_id )
		# Send the server this call specification
		self.out_msgs.put_nowait( {'mesg':mesg, 'body':cl} )
		
		results = []
		for i in range( 0, len(returns) ):
			results.append(key+str(i))
		return results
		#env, paper, dist, case
		

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



	def __init__( self, hostname, port):
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

		
