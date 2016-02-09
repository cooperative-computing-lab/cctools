# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback
import json as jsonlib
from threading import Thread
import socket, select
import Queue

import glob
from utils import *
from class_mesg import Mesg
from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi
from parser import Parser


ROLE = 'Server'

class Listen:
	debug = False
	socket_debug = False
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	

	def receive( self, outQ, objects, retry=False ):
		if len(objects) <= 0:
			return
		if len(objects) > 0:
			mesg = Mesg( objects[0] )
		if not retry:
			d( ROLE, '---->', ROLE, ':   \n', mesg)

		if mesg.action == 'ping':
			if glob.throttle:
				# retry this command later when not so busy
				return False
			mesg.action = 'pong'
			self.send_msg( outQ, {'mesg':mesg} )

		elif mesg.action == 'send':
			if mesg.flow:
				if 'keys' in mesg.flow:
					depth = sys.maxint
					keys = mesg.flow['keys']
					stats = {'keys':[], 'wall_time':0, 'space':0, 'derived_space':0, 'stage_space':[], 'stage_derived_space':[], 'stage_wall_time':[]}

					visited = {}
					while True:
						next_keys = []
						stats['stage_space'].append(0)
						stats['stage_derived_space'].append(0)
						stats['stage_wall_time'].append(0)

						if len(keys) == 0:
							break
						for k in keys:
							#stats['keys'].append( k )
							if k not in visited:
								visited[k] = True
								if len(k)>40:
									key = k[0:40]
									call_key = key
									if call_key not in stats['keys']:
										stats['keys'].append( call_key )

									node = self.db.fetch( call_key+'_' )
									if node:
										stats['wall_time'] += node.obj.duration
										stats['stage_wall_time'][-1] += node.obj.duration
										for size in node.obj.body['sizes']:
											stats['derived_space'] += size # Add the size of the files generated
											stats['stage_derived_space'][-1] += size

										stats['space'] += len(node.obj) #Add the size of the execution
										stats['stage_space'][-1] += len(node.obj)

									node = self.db.fetch( call_key )
									if node:
										stats['space'] += len(node.obj) #Add the size of the call
										stats['stage_space'][-1] += len(node.obj)
										next_keys.append( node.obj.body['env'] )
										
										for arg in node.obj.body['args']:
											if arg not in visited:
												next_keys.append( arg )
								
								elif k != self.nil:
									node = self.db.fetch( k )
									if node:
										stats['space'] += len(node.obj) #Add the size of the original file or envi
										stats['stage_space'][-1] += len(node.obj)
										if isinstance(node.obj, Envi):
											for arg in node.obj.body['args']:
												if arg not in visited:
													next_keys.append( arg )
										
										if k not in stats['keys']:
											stats['keys'].append( k )

						keys = next_keys


					body = jsonlib.dumps(stats, sort_keys=True, indent=2, separators=(',', ': '))
					mesg = Mesg( action='save', flow=mesg.flow )
					self.send_msg( outQ, {'mesg':mesg, 'body':body} )

				else:
					depth = sys.maxint
					if 'depth' in mesg.flow:
						depth = mesg.flow['depth']
					objs = []
					keys = [mesg.key]
					while depth>0:
						next_keys = []
						for k in keys:
							if len(k)>40:
								key = k[0:40]
							else:
								node = self.db.fetch( k )
								if not node:
									return None

								if not node.obj.tree_key:
									objs.append( node.obj )
									continue
								call = jsonlib.loads( node.obj.body )
								key = call.tree_key[0:40]
							node = self.db.fetch( key )
							objs.append( node.obj )
							next_keys += node.obj.body['args']
						keys = next_keys
						depth -= 1
					mesg = Mesg( action='save', flow=mesg.flow )
					self.send_msg( outQ, {'mesg':mesg, 'body':objs} )

			else:
				node = self.db.fetch( mesg.key )
				if node:
					mesg.action = 'save'
					self.send_msg( outQ, {'mesg':mesg, 'body':node.obj} )
				else:
					return None

		elif mesg.action == 'have':
			node = self.db.fetch( mesg.key )
			if not node:
				mesg.action = 'send'
				# Ask the client to send this data
				self.send_msg( outQ, {'mesg':mesg} )
			else:
				self.send_msg( outQ, {'mesg':mesg} )
				d( ROLE, '%s already exists' % (mesg.key) )

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
					self.db.store( en )
					d( ROLE, en )
				elif obj['type'] == 'call':
					cl = Call( obj, body=body )
					self.db.store( cl )
					file_missing = False
					for r in range(0,len(cl.body['returns'])):
						okey = cl.key+str(r)
						if not self.db.exists( okey ):
							file_missing = True
							break
					node = self.db.fetch( cl.key+'_' )
					if file_missing or (node and len(node.obj.body['results']) == 0):
						self.exec_master.add_call( cl )
						print 'Need to execute cmd (%s): %s' % ( cl.body['cmd'], cl.key )
					else:
						print 'Already completed cmd (%s): %s' % ( cl.body['cmd'], cl.key )
						#print node.obj
						

					d( ROLE, cl )
				elif obj['type'] == 'file':
					fl = File( obj, body=body )
					self.db.store( fl )
					d( ROLE, fl )
				elif obj['exec'] == 'exec':
					ex = Exec( obj, body=body )
					self.db.store( ex )
					d( ROLE, ex )

				i += 2

		else:
			print 'Confusing mesg: %s' % mesg

		return True

		

	def send_msg( self, outQ, msg ):
		d( ROLE, '<----', ROLE, ':   \n', msg['mesg'] )
		outQ.put( msg['mesg'] + "\n" )
		
		if 'body' in msg:
			if isinstance(msg['body'], list):
				for body in msg['body']:
					d( ROLE, body )
					outQ.put( body + "\n" )
			else:
				d( ROLE, msg['body'] )
				outQ.put( msg['body'] + "\n" )


		outQ.put( '\n' )




	def __init__( self, hostname, port ):
		self.hostname = hostname
		self.port = port
		self.threads = []
		self.states = {}
		self.db = glob.get_ready_handle( 'db' )
		self.start()

	def start( self ):
		self.exec_master = glob.get_ready_handle( 'exec_master' )
		self.sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		self.sock.setblocking(0)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind( ( self.hostname, self.port ) )
		self.sock.listen(5)


		self.listen_thread = Thread( target=self.socket_loop, args=([ self.sock, self.states ]) )
		self.listen_thread.daemon = True
		self.listen_thread.start()


		#while not glob.shutting_down:
		while True:
			start = time.time()

			try:
				close = []
				for s in self.states:
					inQ = self.states[s]['in']
					outQ = self.states[s]['out']
					parser = self.states[s]['parser']
					retries = self.states[s]['retries']
					#remainder = self.states[s]['remainder']
					closed = self.states[s]['closed']

					while True:
						try:
							next_in_msg = inQ.get_nowait()
						except Queue.Empty:
							if closed:
								close.append( s )
							break
							pass
						else:
							results = parser.parse( next_in_msg )
							if results:
								for objects in results:
									if not self.receive( outQ, objects ):
										retries.append([ objects, time.time()+3 ])
				for s in close:
					del self.states[s]

				if (time.time() - start)<1:
					time.sleep(0.01)
			except KeyboardInterrupt:
				raise KeyboardInterrupt
			except:
				print traceback.format_exc() 
				#pass
			

	def socket_loop( self, sock, connection_states ):
		# Sockets from which we expect to read
		inputs = [ sock ]
		# Sockets to which we expect to write
		outputs = [ ]
		# In/Out message queues (socket:Queue), Parser, retries
		#connection_states = []

		start = time.time()

		while inputs:
			#try:
			# Wait for at least one of the sockets to be ready for processing
			readable, writable, exceptional = select.select(inputs, outputs, inputs, 1)
			#glob.starts['socket_loop1'] = time.time()
			#except ValueError:
			#	self.sock.bind( ( self.hostname, self.port ) )
			#	self.sock.listen(5)

			'''
			Exception in thread Thread-4:
			Traceback (most recent call last):
			  File "/usr/lib64/python2.6/threading.py", line 532, in __bootstrap_inner
			    self.run()
			  File "/usr/lib64/python2.6/threading.py", line 484, in run
			    self.__target(*self.__args, **self.__kwargs)
			  File "/afs/crc.nd.edu/user/p/pivie/cctools.src/prune/src/prune/server.py", line 295, in socket_loop
			    readable, writable, exceptional = select.select(inputs, outputs, inputs, 1)
			ValueError: filedescriptor out of range in select()			
			'''


			# Handle inputs
			for s in readable:

				if s is sock:
					# A "readable" server socket is ready to accept a connection
					connection, client_address = s.accept()
					print >>sys.stderr, 'new connection from', client_address
					connection.setblocking(0)
					inputs.append(connection)

					# Give the connection a queue for data received and to send, and a parser
					parser = Parser(file_threshold=128, one_file=False, debug=self.socket_debug)
					connection_states[connection] = { 'in': Queue.Queue(), 'out': Queue.Queue(), 'parser': parser, 'retries':[], 'remainder':None, 'closed':None }

				else:
					try:
						data = s.recv(65468)
						if data:
							# A readable client socket has data
							#print >>sys.stderr, 'received %i bytes from %s' % (len(data), s.getpeername())
							
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
							connection_states[s]['closed'] = time.time()
							#del connection_states[s]
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
							connection_states[s]['closed'] = time.time()
							#del connection_states[s]
						else:
							traceback.format_exc()
							print 'err:',err
							print e

			#glob.sums['socket_loop1'] += time.time() - glob.starts['socket_loop1']
			#glob.starts['socket_loop2'] = time.time()
			
			# Handle outputs
			for s in writable:
				if s in connection_states:
					try:
						if connection_states[s]['remainder']:
							next_msg = connection_states[s]['remainder']
						else:
							next_msg = connection_states[s]['out'].get_nowait()
					except Queue.Empty:
						# No messages waiting so stop checking for writability.
						#outputs.remove(s)
						pass # Never stop checking for outgoing messages
					else:
						#print >>sys.stderr, 'sending %i objects to %s' % (len(next_msg), s.getpeername())
						sent = s.send( next_msg )
						if sent < len(next_msg):
							connection_states[s]['remainder'] = next_msg[sent:]
						else:
							connection_states[s]['remainder'] = None

			#glob.sums['socket_loop2'] += time.time() - glob.starts['socket_loop2']
			#glob.starts['socket_loop3'] = time.time()
			

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

			#glob.sums['socket_loop3'] += time.time() - glob.starts['socket_loop3']
			#glob.starts['socket_loop4'] = time.time()
			
			# Handle outputs
			for s in connection_states:
				now = time.time()
				retries = connection_states[s]['retries']
				for i,ar in enumerate( retries ):
					objects, ready_time = ar
					if ready_time < now:
						del retries[i]
						if not self.receive( connection_states[s]['out'], objects, retry=True ):
							retries.append([ objects, time.time()+3 ])

			#glob.sums['socket_loop4'] += time.time() - glob.starts['socket_loop4']
			time.sleep(0.01)
			

	def stop( self ):
		map( socket.close, [self.sock] )

	def restart( self ):
		self.stop()
		self.start()
		


