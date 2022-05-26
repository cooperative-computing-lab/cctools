# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback
import json as jsonlib
import subprocess, shutil
import Queue
from collections import deque

import glob
from utils import *
from class_item import Item
from parser import Parser
from db_sqlite import Database

from prune import master_local
from prune import master_wq
from prune import worker


ROLE = 'Client'

class Connect:
	debug = False
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
	glob.workflow_id = uuid()


	def execute( self, worker_type, **kwargs ):
		if 'stage' in kwargs:
			glob.wq_stage = kwargs['stage']
		if worker_type == 'local':
			if 'cores' in kwargs:
				glob.exec_local_concurrency = int(kwargs['cores'])
			workers = master_local.Master()
			workers.run()
		elif worker_type == 'work_queue' or worker_type == 'wq':
			if 'name' in kwargs:
				glob.wq_name = kwargs['name']
			workers = master_wq.Master()
			workers.run()



	def envi_add( self, **kwargs ):
		timer.start('client.envi_add')
		if 'engine' in kwargs:
			if kwargs['engine'] == 'wrapper':
				obj = {'engine':kwargs['engine'],
					'open':kwargs['open'], 'close':kwargs['close'],
					'args':kwargs['args'], 'params':kwargs['params']}
				if 'http_proxy' in kwargs:
					obj['http_proxy'] = kwargs['http_proxy']
				it = Item( type='envi', body=obj )
				glob.db.insert(it)
				return it.cbid

			elif kwargs['engine'] == 'umbrella':

				filename = kwargs['spec']
				cbid = self.file_add( filename )
				args = [cbid]
				params = ['SPEC.umbrella']

				if kwargs['cms_siteconf']:
					filename2 = kwargs['cms_siteconf']
					cbid2 = self.file_add( filename2 )
					args.append(cbid2)
					params.append(filename2)

				obj = {'engine':kwargs['engine'], 'spec':cbid,
					'cvmfs_http_proxy':kwargs['cvmfs_http_proxy'],
					'cms_siteconf':kwargs['cms_siteconf'],
					'sandbox_mode':kwargs['sandbox_mode'],
					'log':kwargs['log'], 'args':args, 'params':params}

				it = Item( type='envi', body=obj )
				glob.db.insert(it)
				return it.cbid

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


	def task_add( self, returns, env, cmd, args=[], params=[], types=[], env_vars={}, precise=True):

		# print '%s = %s (%s)'%(returns,cmd,args)
		timer.start('client.task_add')
		obj = {'returns':returns, 'env':env, 'cmd':cmd, 'args':args, 'params':params, 'types':types, 'env_vars':env_vars, 'precise':precise}
		it = Item( type='call', body=obj )
		glob.db.insert( it )
		glob.db.task_add( it )
		results = []
		for i in range( 0, len(returns) ):
			results.append(it.cbid+':'+str(i))
		timer.stop('client.task_add')
		self.op_cnt += 1
		#if (self.op_cnt%100)==0:
		#	time.sleep(0.1)
		return results


	def step_name( self, name ):
		self.wait()
		self.report()


		print name
		glob.workflow_step = name



	def report( self ):
		timer.report()
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



	def keep_quota( self, quota_bytes ):
		return glob.db.keep_quota( quota_bytes )


	def restore_trash( self ):
		return glob.db.restore_trash()


	def export( self, prid, pathname, **kwargs ):
		timer.start('client.export')


		visited = {}
		if isinstance( prid, str ):
			prid_list = [prid]
		else:
			prid_list = prid
		next_prid_list = []
		depth = 0
		if 'lineage' in kwargs:
			lineage = int(kwargs['lineage'])
		else:
			lineage = 0
		if 'progeny' in kwargs:
			progeny = int(kwargs['progeny'])
		else:
			progeny = 0

		if 'files' in kwargs and kwargs['files']:
			if isinstance(kwargs['files'], str):
				filescope = [ kwargs['files'] ]
			else:
				filescope = kwargs['files']
		else:
			filescope = ['min']

		if lineage==0 and progeny==0 and (isinstance( prid, str ) or len(prid)==1):
			single_file = True
		else:
			single_file = False


		file_cnt = 0
		task_cnt = 0
		temp_cnt = 0
		more_cnt = 0
		size = 0
		with open( pathname, 'w' ) as f:
			print '\nExport (%s) contains the following objects...' % pathname
			while prid_list:
				#print 'depth',depth
				for prid in prid_list:
					if prid not in visited:
						visited[prid] = True
						item = glob.db.find_one( prid )
						if not item:
							if single_file:
								print "File not yet available. Don't forget to start a prune_worker"
							elif ':' in prid:
								task_id,file_id = prid.split(':')
								item = glob.db.find_one( task_id )
						if not item:
							print prid,'=',item
						elif item.type=='temp':
							#print 'temp:',item.cbid
							task_id,file_id = item.dbid.split(':')
							#print '----->',task_id
							if single_file:
								print 'file:', item.cbid, item.size
								item.stream_content( f )
								temp_cnt += 1

							elif depth<lineage:
								if depth==0 and 'results' in filescope:
									print 'result:',item.cbid
									#size += item.size
									item.stream( f )
									temp_cnt += 1
								elif 'all' in filescope:
									print 'all:',item.cbid
									#size += item.size
									item.stream( f )
									temp_cnt += 1
								item2 = glob.db.find_one( task_id )
								if item2.cbid not in visited:
									print 'task:', item2.cbid, item2.body['cmd']
									visited[item2.cbid] = True
									item2.stream( f )
									for arg in item2.body['args']:
										#print '----->',arg
										next_prid_list.append(arg)
									task_cnt += 1
							else:
								if 'min' in filescope or 'all' in filescope:
									print 'temp:', item.cbid, item.size
									#size += item.size
									item.stream( f )
									temp_cnt += 1
						elif item.type=='file':
							#print 'file:',item
							if single_file:
								print 'file:', item.cbid, item.size
								item.stream_content( f )
							else:
								print 'file:', item.cbid, item.size
								#size += item.size
								item.stream( f )
							file_cnt += 1
						elif item.type=='call':
							#print 'call',item
							print 'task:', item.cbid, item.body['cmd']
							if single_file:
								item.stream_content( f )
							else:
								item.stream( f )
								for arg in item.body['args']:
									#print '----->',arg
									next_prid_list.append(arg)

								if 'env' in item.body and item.body['env']!=self.nil:
									next_prid_list.append(item.body['env'])

							task_cnt += 1
						else:
							print item.type+':', item.cbid, item.size
							item.stream( f )
							if 'args' in item.body:
								for arg in item.body['args']:
									#print '----->',arg
									next_prid_list.append(arg)

							more_cnt += 1

				prid_list = next_prid_list
				next_prid_list = []
				depth += 1



		if not single_file:
			diff = timer.stop('client.export')
			statinfo = os.stat(pathname)
			total_size = statinfo.st_size + size
			print 'Export description: pathname=%s prid(s)=%s' % ( pathname, prid ), kwargs
			print 'Export results: duration=%f size=%i file_cnt=%i task_cnt=%i temp_cnt=%i more_cnt=%i' % (diff, total_size, file_cnt, task_cnt, temp_cnt, more_cnt)
		'''
		timer.start('client.export_zip')

		zipped = pathname+'.gz'
		cmd = 'gzip < %s > %s' % (pathname,zipped)
		p = subprocess.Popen( cmd , stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
		(stdout, stderr) = p.communicate()
		#if stdout:
		#	print 'stdout:',stdout
		if stderr:
			print 'stderr:',stderr

		diff = timer.stop('client.export_zip')
		statinfo = os.stat(zipped)
		print 'Export zipped: duration=%f size=%i' % (diff, statinfo.st_size)
		'''






	def load( self, pathname ):
		timer.start('client.import')

		with open(pathname, 'r') as f:
			line = f.readline()
			cnt = 0
			while line:
				if (cnt%1000)==0:
					sys.stdout.write(str(cnt))
				sys.stdout.write('.')
				sys.stdout.flush()
				obj = jsonlib.loads( line )
				if 'size' in obj and 'body' not in obj:
					pathname = glob.tmp_file_directory+uuid()
					with open(pathname,'w') as tf:
						remaining = obj['size']
						while remaining>(1024*1024):
							sys.stdout.write(',')
							sys.stdout.flush()
							tf.write( f.read(1024*1024) )
							remaining -= (1024*1024)
						tf.write( f.read(remaining) )
					item = Item( obj, path=pathname )
				else:
					item = Item( obj )
				glob.db.insert( item )
				if item.type=='call':
					glob.db.task_add( item )

				cnt += 1
				line = f.readline()


		diff = timer.stop('client.import')
		print 'Imported in:',diff



	def work_stats( self, keys ):
		key = uuid()
		flow = {'key':key, 'keys':keys, 'files':False}
		mesg = Mesg( action='send', key=key, flow=flow )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		self.dump_stats[key] = filename
		return key


	'''
	def flow_dump( self, key, filename, depth=1 ):
		flow = {'key':key,'depth':depth}
		mesg = Mesg( action='send', key=key, flow=flow )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		self.dump_flows[key] = filename
		return key
	'''

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



	def __init__( self, base_dir = None, config = None ):
		if config:
			glob.set_config_file(config)

		if base_dir:
			if base_dir[-1]!='/':
				base_dir += '/'
			glob.set_base_dir(base_dir)


		if not os.path.exists(glob.tmp_file_directory):
			os.makedirs(glob.tmp_file_directory)

		self.op_cnt = 0

		print 'Working from base directory:', glob.base_dir

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
