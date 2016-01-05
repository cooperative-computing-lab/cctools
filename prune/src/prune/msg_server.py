# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback, io
import json as jsonlib
from threading import Thread
from socket import *
from Queue import Queue

from class_data import Data
from class_mesg import Mesg
from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi
from parser import Parser

import hashlib


class Listen:
	role = 'Server'
	debug = False
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

		

	def error( self, sock, message, original ):
		msg = { 'action':'error', 'status':status }
		msg['message'] = message
		if original:
			msg['original'] = original
		self.send( msg, sock )




	def get_base( self ):
		drive = self.master.cfg['db']['drive']
		return self.master.cfg['drives'][drive]['location']

	def get_pathname( self, key ):
		#self.debug_print( base + 'files/' + key )
		return self.get_base() + 'files/' + key

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



	def receive( self, sock, objects, retry=False ):
		if len(objects) <= 0:
			return
		if len(objects) > 0:
			mesg = Mesg( objects[0] )
		if not retry:
			self.debug_print( '---->', self.role, ':   \n', mesg)

		if mesg.action == 'send':
			if mesg.plan:
				depth = mesg.plan['depth']
				objs = []
				keys = [mesg.key]
				while depth>0:
					next_keys = []
					for k in keys:
						if len(k)>40:
							key = k[0:40]
						else:
							node = self.master.db.fetch( k )
							if not node:
								return None
							if not node.obj.tree_key:
								objs.append( node.obj )
								continue
							call = jsonlib.loads( node.obj.body )
							key = call.tree_key[0:40]
						node = self.master.db.fetch( key )
						objs.append( node.obj )
						next_keys += node.obj.body['args']
					keys = next_keys
					depth -= 1
				mesg = Mesg( action='save', plan=mesg.plan )
				self.send( sock, {'mesg':mesg, 'body':objs} )
					

			else:
				node = self.master.db.fetch( mesg.key )
				if node:
					mesg.action = 'save'
					self.send( sock, {'mesg':mesg, 'body':node.obj} )
				else:
					return None

		elif mesg.action == 'need':
			node = self.master.db.fetch( mesg.key )
			if not node:
				mesg.action = 'send'
				# Ask the client to send this data
				self.send( sock, {'mesg':mesg} )
			else:
				self.debug_print( '%s already exists' % (mesg.key) )

		elif mesg.action == 'save':
			i = 1
			while i<len(objects):
				obj = objects[i]
				body = objects[i+1]

				if obj['type'] == 'envi':
					en = Envi( obj, body=body )
					self.master.db.store( en )
					self.debug_print( en )
				elif obj['type'] == 'call':
					cl = Call( obj, body=body )
					if self.master.db.store( cl ):
						self.master.executer.enqueue( cl )
					else:
						print 'Already executed cmd (%s): %s\n' % ( cl.body['cmd'], cl.key )
 
					self.debug_print( cl )
				elif obj['type'] == 'file':
					fl = File( obj, body=body )
					self.master.db.store( fl )
					self.debug_print( fl )
				elif obj['exec'] == 'exec':
					ex = Exec( obj, body=body )
					self.master.db.store( ex )
					self.debug_print( ex )

				i += 2

		else:
			print 'unknown action: %s' % meta

		return True

		

	def send( self, sock, msg ):
		self.debug_print( '<----', self.role, ':   \n', msg['mesg'] )
		sock.sendall( msg['mesg'] + "\n" )
		
		if 'body' in msg:
			if isinstance(msg['body'], list):
				for body in msg['body']:
					self.debug_print( body )
					sock.sendall( body + "\n" )
			else:
				self.debug_print( msg['body'] )
				sock.sendall( msg['body'] + "\n" )


		sock.sendall( '\n' )




	def __init__( self, hostname, port, master ):
		self.master = master
		self.hostname = hostname
		self.port = port
		self.threads = []
		self.start()

	def queue_loop( self ):
		pass

	def start( self ):
		self.sock = socket( AF_INET, SOCK_STREAM )
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.sock.bind( ( self.hostname, self.port ) )
		self.sock.listen(5)

		self.listen_thread = Thread( target=self.listen_loop, args=([  ]) )
		self.listen_thread.daemon = True
		self.listen_thread.start()

		
		#queue_thread = Thread( target=self.json_queue_loop, args=( ) )
		#queue_thread = Thread( target=self.master.core.queue_loop, args=( ) )
		#queue_thread.daemon = True
		#queue_thread.start()

		

	def stop( self ):
		map( socket.close, [self.sock] )

	def restart( self ):
		self.stop()
		self.start()



	def listen_loop( self ):
		while not self.master.terminate:
			try:
				clientsock, addr = self.sock.accept()
				t = Thread(target=self.handler, args=([ clientsock, Queue() ]) )
				t.daemon = True
				t.start()
				self.threads.append(t)
			except error, e:
				print 'error:',e
				print e.args
				raise
				#if e.args[0] != errno.EINTR:
				#	raise


	def handler( self, sock, out_msgs ):
		mybuffer = ''
		retries = []
		sock.settimeout(3)
		parser = Parser(file_threshold=128, one_file=False)
		while True:
			now = time.time()
			
			while not out_msgs.empty():
				msg = out_msgs.get()
				self.send( sock, msg )

			for i,ar in enumerate( retries ):
				meta, ready_time = ar
				if ready_time < now:
					del retries[i]
					if not self.receive( sock, meta, retry=True ):
						retries.append([ meta, time.time()+3 ])

			try:
				raw_data = sock.recv( 4096 )
				if len(raw_data) <= 0:
					# If timed out, exception would have been thrown
					sys.stderr.write('\nLost connection...\n')
					sys.stderr.flush()
					break
				

				results = parser.parse( raw_data )
				if results:
					for objects in results:
						if not self.receive( sock, objects ):
							retries.append([ objects, time.time()+3 ])


			except timeout, e:
				raw_data = ''
							
		self.stop()


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

	def hashstring( self, str ):
		key = hashlib.sha1()
		key.update( str )
		return key.hexdigest()

