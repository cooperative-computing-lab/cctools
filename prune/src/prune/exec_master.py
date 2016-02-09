# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, time, sys, traceback

import Queue
from threading import Thread

from utils import *
import glob
import exec_local
import exec_wq


class Master:


	def add_call( self, call_obj ):
		if call_obj.key not in self.in_progress:
			self.in_progress.add( call_obj.key )
			self.queued.put_nowait( call_obj )
			if self.queued.qsize()>1000:
				glob.throttle = True
		else:
			print 'Already in progress:', call_obj.body['cmd']


	def call_is_ready( self, call_obj ):
		for f, arg in enumerate(call_obj.body['args']):
			if not self.db.exists( arg ):
				#print '%s (%s) cmd="%s"' % ( arg, call_obj.body['params'], call_obj.body['cmd'] )
				#time.sleep(0.1)
				return False
		return True

	def end_call( self, call_obj ):
		self.in_progress.discard( call_obj.key )
		if self.queued.qsize()<1000:
			glob.throttle = False



	def __init__( self ):		
		self.queued = Queue.Queue()
		self.delayed = []
		self.in_progress = set()
		self.local_master = exec_local.Master( self )
		self.wq_master = exec_wq.Master( self )
		self.start()


	def start( self ):
		self.thread = Thread( target=self.loop, args=([  ]) )
		self.thread.daemon = True
		self.thread.start()

	def stop( self ):
		self.wq_master.stop()
		self.local_master.stop()
		print '%i calls were in the queue.' % self.queued.qsize()
		print '%i calls were in the delay queue.' % len(self.queued)

	def restart( self ):
		self.stop()
		self.start()




	def loop( self ):
		self.db = glob.get_ready_handle( 'db' )
		glob.set_ready_handle( 'exec_master', self )
		
		while True:
			#glob.starts['exec_loop'] = time.time()
			fed = 0
			# Use local workers first
			hunger = self.local_master.hungry()
			if hunger > 0:
				while hunger > 0:
					
					# Execute a newly queued call
					try:
						call = self.queued.get_nowait()

						if self.call_is_ready( call ):
							self.local_master.execute( call )
							hunger -= 1
							fed += 1
							time.sleep(0.0001)
						else:
							self.delayed.append( call )
					except Queue.Empty:
						break



				# Execute calls whose inputs recently became available
				if hunger > 0:
					for k, call in enumerate( self.delayed ):
						if self.call_is_ready( call ):
							self.local_master.execute( call )
							hunger -= 1
							fed += 1
							time.sleep(0.0001)
							del self.delayed[k]
						if hunger <= 0:
							break


			# Use wq if the local machine is full
			if hunger == 0:
				hunger = self.wq_master.hungry()
				if hunger <= 0:
					# No local OR wq workers
					time.sleep(1)
				else:

					while hunger > 0:
						try:
							# Execute a newly queued call
							call = self.queued.get_nowait()

							if self.call_is_ready( call ):
								self.wq_master.execute( call )
								hunger -= 1
								fed += 1
								time.sleep(0.0001)
							else:
								self.delayed.append( call )
						except Queue.Empty:
							break

					# Execute calls whose inputs recently became available
					if hunger > 0:

						for k, call in enumerate( self.delayed ):
							if self.call_is_ready( call ):
								self.wq_master.execute( call )
								hunger -= 1
								fed += 1
								time.sleep(0.0001)
								del self.delayed[k]
							if hunger <= 0:
								break
	
				



			#glob.sums['exec_loop'] += time.time() - glob.starts['exec_loop']
			if fed == 0:
				time.sleep(1)
			else:
				time.sleep(0.0001)
		



