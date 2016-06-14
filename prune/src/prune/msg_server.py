# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback, io
import json as jsonlib
from threading import Thread
import socket, select, Queue

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



	def receive( self, outQ, objects, retry=False ):
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
				outQ.put( {'mesg':mesg, 'body':objs} )

			else:
				node = self.master.db.fetch( mesg.key )
				if node:
					mesg.action = 'save'
					outQ.put( {'mesg':mesg, 'body':node.obj} )
				else:
					return None

		elif mesg.action == 'have':
			node = self.master.db.fetch( mesg.key )
			if not node:
				mesg.action = 'send'
				# Ask the client to send this data
				outQ.put( {'mesg':mesg} )
			else:
				outQ.put( {'mesg':mesg} )
				self.debug_print( '%s already exists' % (mesg.key) )

		elif mesg.action == 'save':
			i = 1
			while i<len(objects):
				obj = objects[i]
				if (i+1)<len(objects):
					body = objects[i+1]
				else:
					body = None

				if obj['type'] == 'envi':
					en = Envi( obj, body=body )
					self.master.db.store( en )
					self.debug_print( en )
				elif obj['type'] == 'call':
					cl = Call( obj, body=body )
					self.master.db.store( cl )
					file_missing = False
					for r in range(0,len(cl.body['returns'])):
						okey = cl.key+str(r)
						if not self.master.db.exists( okey ):
							file_missing = True
							break
					if file_missing:
						self.master.executer.enqueue( cl )
						print 'Need execution cmd (%s): %s\n' % ( cl.body['cmd'], cl.key )
					else:
						print 'Already completed cmd (%s): %s\n' % ( cl.body['cmd'], cl.key )
						

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
			print 'Confusing mesg: %s' % mesg

		return True

		

	def send_msg( self, sock, msg ):
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
		self.states = {}
		self.start()

	def queue_loop( self ):
		pass

	def start( self ):
		self.sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		self.sock.setblocking(0)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind( ( self.hostname, self.port ) )
		self.sock.listen(5)


		self.listen_thread = Thread( target=self.socket_loop, args=([ self.sock, self.states ]) )
		self.listen_thread.daemon = True
		self.listen_thread.start()


		while not self.master.terminate:
			start = time.time()

			for s in self.states:
				inQ = self.states[s]['in']
				outQ = self.states[s]['out']
				parser = self.states[s]['parser']
				retries = self.states[s]['retries']

				try:
					next_in_msg = inQ.get_nowait()
				except Queue.Empty:
					pass
				else:
					results = parser.parse( next_in_msg )
					if results:
						for objects in results:
							if not self.receive( outQ, objects ):
								retries.append([ objects, time.time()+3 ])

			if (time.time() - start)<1:
				time.sleep(0.1)
			

	def socket_loop( self, sock, connection_states, debug=False ):
		# Sockets from which we expect to read
		inputs = [ sock ]
		# Sockets to which we expect to write
		outputs = [ ]
		# In/Out message queues (socket:Queue), Parser, retries
		#connection_states = []

		while inputs:

			# Wait for at least one of the sockets to be ready for processing
			readable, writable, exceptional = select.select(inputs, outputs, inputs, 1)


			# Handle inputs
			for s in readable:

				if s is sock:
					# A "readable" server socket is ready to accept a connection
					connection, client_address = s.accept()
					print >>sys.stderr, 'new connection from', client_address
					connection.setblocking(0)
					inputs.append(connection)

					# Give the connection a queue for data received and to send, and a parser
					parser = Parser(file_threshold=128, one_file=False, debug=debug)
					connection_states[connection] = { 'in': Queue.Queue(), 'out': Queue.Queue(), 'parser': parser, 'retries':[] }

				else:
					try:
						data = s.recv(1024)
						if data:
							# A readable client socket has data
							print >>sys.stderr, 'received %i bytes from %s' % (len(data), s.getpeername())
							connection_states[s]['in'].put(data)
							# Add output channel for response
							if s not in outputs:
								outputs.append(s)
						else:
							# Interpret empty result as closed connection
							print >>sys.stderr, 'closing', client_address, 'after reading no data'
							# Stop listening for input on the connection
							if s in outputs:
								outputs.remove(s)
							inputs.remove(s)
							s.close()

							# Remove message queue
							del connection_states[s]
					except socket.error, e:
						err = e.args[0]
						if err == 104:
							# Interpret empty result as closed connection
							print >>sys.stderr, 'closing', client_address, 'after client disconnected'
							# Stop listening for input on the connection
							if s in outputs:
								outputs.remove(s)
							inputs.remove(s)
							s.close()

							# Remove message queue
							del connection_states[s]
						else:
							traceback.format_exc()
							print 'err:',err
							print e
			
			# Handle outputs
			for s in writable:
				try:
					next_msg = connection_states[s]['out'].get_nowait()
				except Queue.Empty:
					# No messages waiting so stop checking for writability.
					outputs.remove(s)
				else:
					print >>sys.stderr, 'sending %i objects to %s' % (len(next_msg), s.getpeername())
					self.send_msg( s, next_msg )
			

			# Handle "exceptional conditions"
			for s in exceptional:
				print >>sys.stderr, 'handling exceptional condition for', s.getpeername()
				# Stop listening for input on the connection
				inputs.remove(s)
				if s in outputs:
					outputs.remove(s)
				s.close()

				# Remove connection state
				del connection_states[s]


			# Handle outputs
			for s in connection_states:
				try:
					while True:
						next_msg = connection_states[s]['out'].get_nowait()
						print >>sys.stderr, 'sending %i objects to %s' % (len(next_msg), s.getpeername())

						self.send_msg( s, next_msg )

				except Queue.Empty:
					pass

	def stop( self ):
		map( socket.close, [self.sock] )

	def restart( self ):
		self.stop()
		self.start()
		


	def listen_loop( self ):
		while not self.master.terminate:
			try:
				clientsock, addr = self.sock.accept()
				t = Thread(target=self.handler, args=([ clientsock, Queue.Queue() ]) )
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
		parser = Parser(file_threshold=128, one_file=False, debug=self.debug)
		while True:
			now = time.time()
			
			while not out_msgs.empty():
				msg = out_msgs.get()
				self.send( sock, msg )

			for i,ar in enumerate( retries ):
				objects, ready_time = ar
				if ready_time < now:
					del retries[i]
					if not self.receive( sock, objects, retry=True ):
						sys.stdout.write('.')
						sys.stdout.flush()
						retries.append([ objects, time.time()+3 ])

			try:
				raw_data = sock.recv( 1024*1024 )
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

