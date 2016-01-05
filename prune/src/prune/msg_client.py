# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, io, sys, time, traceback
import json as jsonlib
import uuid as uuidlib
from threading import Thread
from Queue import Queue
from socket import *
import hashlib
import subprocess

from class_data import Data
from class_mesg import Mesg
from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi
from parser import Parser

from json import JSONEncoder
from uuid import UUID
JSONEncoder_olddefault = JSONEncoder.default
def JSONEncoder_newdefault( self, o ):
	if isinstance( o, UUID ): return str( o )
	return JSONEncoder_olddefault( self, o )
JSONEncoder.default = JSONEncoder_newdefault



class Connect:
	role = 'Client'
	debug = True
	ready = False
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	def env_add( self, **kwargs ):
		if 'engine' in kwargs:
			if kwargs['engine'] == 'targz':
				filename = 'tmp_' + str(uuidlib.uuid4()) + '.tar.gz'
				p = subprocess.Popen( "tar -zcvf %s %s" % (filename, kwargs['folders2include']) , stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
				(stdout, stderr) = p.communicate()
				print 'stdout:',stdout
				print 'stderr:',stderr
				
				key, length = self.hashfile( filename )
				mesg = Mesg( action='need', key=key )
				self.out_msgs.put_nowait( {'mesg':mesg} )

				fl = File( key=key, path=filename, size=length )
				self.files[key] = fl

				obj = {'engine':'targz', 'args':[key], 'params':['_folders2include.tar.gz']}

				keyinfo = hashlib.sha1()
				keyinfo.update( str(obj) )
				env_key = keyinfo.hexdigest()
				
				mesg = Mesg( action='save' )
				en = Envi( key=env_key, body=obj )
				# Send the server this environment specification
				self.out_msgs.put_nowait( {'mesg':mesg, 'body':str(en)} )

				return env_key
					
			elif kwargs['engine'] == 'umbrella':
				'''env_obj = Data( kwargs )
				env_key = env_obj.key()
				mesg = Mesg( action='save' )

				info = Envi( key=env_key, data_type='env', format='json', size=env_obj.size() )
				copy = DataCopy( key=env_key, copy_type='stream', body=env_obj, size=env_obj.size() )
				self.out_msgs.put_nowait( {'mesg':mesg, 'info':info, 'copy':copy} ) # Send the server this environment specification
				return hash_index
				'''
				return self.nil
		else:
			return self.nil

	def file_add( self, filename ):
		key, length = self.hashfile( filename )
		mesg = Mesg( action='need', key=key )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		
		data = ''
		f = open( filename, 'rb' )
		buf = f.read(1024)
		while buf:
			data += buf
			buf = f.read(1024)

		fl = File( key=key, body=data )
		self.files[key] = fl  # save the data in case the server asks for it
		return key

	def data_add( self, data ):
		#data = jsonlib.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
		key = self.hashstring( data )
		mesg = Mesg( action='save' )
		fl = File( key=key, body=data )
		# Send the server this file
		self.out_msgs.put_nowait( {'mesg':mesg, 'body':str(fl)} )
		return key

	def file_dump( self, key, filename ):
		mesg = Mesg( action='send', key=key )
		# Ask the server to send this data
		self.out_msgs.put_nowait( {'mesg':mesg} )
		# save the filename for when the data comes
		self.files[key] = filename
		return key

	def call_add( self, returns, env, cmd, args=[], params=[], types=[], env_vars={}, precise=True):
		obj = {'returns':returns, 'env':env, 'cmd':cmd, 'args':args, 'params':params, 'types':types, 'env_vars':env_vars, 'precise':precise}
		if len(types)==0:
			for param in params:
				obj['types'].append('path')   # alternative to 'path' is 'data'

		keyinfo = hashlib.sha1()
		keyinfo.update( str(obj) )
		key = keyinfo.hexdigest()
		
		mesg = Mesg( action='save' )
		cl = Call( key=key, body=obj )
		# Send the server this call specification
		self.out_msgs.put_nowait( {'mesg':mesg, 'body':str(cl)} )
		
		results = []
		for i in range( 0, len(returns) ):
			results.append(key+str(i))
		return results
		

	def plan_dump( self, key, filename, depth=1 ):
		plan = {'key':key,'depth':depth}
		mesg = Mesg( action='send', key=key, plan=plan )
		self.out_msgs.put_nowait( {'mesg':mesg} )
		self.plans[key] = open( filename, 'wb' )
		return key






	def demo_census( self ):
		E1 = self.env_add( engine='targz', directories=['jellyfish'] )
		



	def demo_merge_sort( self ):
		
		E1 = self.env_add()
		D1 = self.file_add( 'nouns.txt' )
		D2 = self.file_add( 'verbs.txt' )

		D3, = self.call_add( returns=['output.txt'],
			env=E1,	cmd='sort input.txt > output.txt',
			args=[D1], params=['input.txt'] )
			
		
		D4, = self.call_add( returns=['output.txt'],
			env=E1, cmd='sort input.txt > output.txt',
			args=[D2], params=['input.txt'] )
		
		
		E2 = self.env_add(engine='targz', folders2include='libraries')

		D5,D6 = self.call_add(
			returns=['merged_output.txt','file_not_here.txt'],
			env=E2, cmd='sort -m input*.txt > merged_output.txt',
			args=[D3,D4], params=['input1.txt','input2.txt'] )

		
		self.file_dump( D5, 'merged_result.txt' )
		self.plan_dump( D5, 'merge_sort.dump', depth=3 )




		
	def demo_hep( self ):

		D0 = self.file_add( 'cms.umbrella' )
		E1 = self.env_add( engine='umbrella', args=[D0], params=['umbrella_spec.json'] )
		D1 = self.data_add( '20' )
		D2 = self.data_add( 'CMSSW_5_3_11' )
		D3 = self.data_add( 'slc5_amd64_gcc462' )

		cmd = '''
. /cvmfs/cms.cern.ch/cmsset_default.sh

scramv1 project -f CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

cmsDriver.py SinglePi0E10_cfi --conditions auto:startup \
-s GEN,SIM --datatier GEN-SIM -n ${NumEvents} \
--relval 25000,100 --eventcontent RAWSIM
		'''

		D4, = self.call_add(
			returns=['SinglePi0E10_cfi_GEN_SIM.root'],
			env=E1, cmd=cmd,
			env_vars={ 'NumEvents':D1,
				'CMS_VERSION':D2,
				'SCRAM_ARCH':D3 } )




	def demo_hep2( self ):		




		cmd = '''
. /cvmfs/cms.cern.ch/cmsset_default.sh;

scramv1 project CMSSW ${CMS_VERSION}
cd ${CMS_VERSION}
eval `scram runtime -sh`
cd ..

cmsDriver.py SinglePi0E10_cfi_GEN  --datatier GEN-SIM-DIGI-RAW-HLTDEBUG --conditions auto:startup -s DIGI,L1,DIGI2RAW,HLT:@relval,RAW2DIGI,L1Reco --eventcontent FEVTDEBUGHLT -n ${NumEvents}
		'''

		D5, = self.call_add( 
			returns=['SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root'],
			env=E1, cmd=cmd,
			args=[D4],
			params=['SinglePi0E10_cfi_GEN_SIM.root'],
			#types=['filename'],
			env_vars={ 'NumEvents':D1, 
				'CMS_VERSION':D2, 
				'SCRAM_ARCH':D3 } )


		D5, = self.call_add( 
			returns=['SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root'],
			env=E1, cmd=cmd,
			args=[D1,D4],
			params=['$1','SinglePi0E10_cfi_GEN_SIM.root'],
			types=['string', 'filename'],
			env_vars={ 
				'CMS_VERSION':D2, 
				'SCRAM_ARCH':D3 } )


		D5, = self.call_add( 
			returns=['SinglePi0E10_cfi_GEN_DIGI_L1_DIGI2RAW_HLT_RAW2DIGI_L1Reco.root'],
			env=E1, cmd=cmd,
			args=[D1,D4],
			params=['NumEvents','SinglePi0E10_cfi_GEN_SIM.root'],
			types=['env_var', 'filename'],
			env_vars={ 
				'CMS_VERSION':D2, 
				'SCRAM_ARCH':D3 } )



		cmd = '''
. /cvmfs/cms.cern.ch/cmsset_default.sh;

scramv1 project CMSSW ${CMS_VERSION};
cd ${CMS_VERSION};
eval `scram runtime -sh`;
cd ..;

cmsDriver.py SinglePi0E10_cfi_GEN --datatier GEN-SIM-RECO,DQM --conditions auto:startup -s RAW2DIGI,L1Reco,RECO,VALIDATION,DQM --eventcontent RECOSIM,DQM -n ${NumEvents}
		'''


		D6,D7,D8 = self.call_add( 
			returns=['SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.py', #D6
				'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.root', #D7
				'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM_inDQM.root'], #D8
			env=E1, cmd=cmd,
			args=[D5],
			params=['SinglePi0E10_cfi_GEN_DIGI2RAW.root'],
			env_vars={ 'NumEvents':D1, 
				'CMS_VERSION':D2, 
				'SCRAM_ARCH':D3 } )



	
		self.file_dump( D7, 'SinglePi0E10_cfi_GEN_RAW2DIGI_L1Reco_RECO_VALIDATION_DQM.root' )
		





	def get_pathname( self, hash_index ):
		return './' + hash_index

	def debug_print( self, *args ):
		if self.debug:
			for arg in args:
				print arg,
			print ''



	def receive( self, sock, objects ):
		if len(objects) <= 0:
			return
		mesg = body = None
		if len(objects) > 0:
			mesg = Mesg( objects[0] )
		self.debug_print( '---->', self.role, ':   \n', mesg)

		if mesg.action == 'send':
			fl = self.files[mesg.key]
			mesg.action = 'save'
			self.send( sock, {'mesg':mesg, 'body':fl} )


		elif mesg.action == 'need':
			node = self.master.db.fetch( mesg.key )
			if not node:
				mesg.action = 'send'
				# Ask the client to send this data
				self.send( sock, {'mesg':mesg} )
			else:
				self.debug_print( '%s already exists' % (info.key) )

		elif mesg.action == 'save':

			objs = []
			i = 1
			while i<len(objects):
				obj = objects[i]
				body = objects[i+1]

				if obj['type'] == 'envi':
					en = Envi( obj, body=body )
					objs.append( en )
					self.debug_print( en )
				elif obj['type'] == 'call':
					cl = Call( obj, body=body )
					objs.append( cl )
					self.debug_print( cl )
				elif obj['type'] == 'file':
					fl = File( obj, body=body )
					objs.append( fl )
					self.debug_print( fl )
				elif obj['exec'] == 'exec':
					ex = Exec( obj, body=body )
					objs.append( ex )
					self.debug_print( ex )
				i += 2

			if mesg.plan:
				pl = self.plans[mesg.plan['key']]
				for obj in objs:
					pl.write( str(obj) + "\n" )
				pl.close()
				del self.plans[mesg.plan['key']]

			else:
				filename = self.files[mesg.key]
				f = open( filename, 'wb' )
				f.write( str(objs[0].body) )
				f.close()
				del self.files[mesg.key]
				

		else:
			print 'unknown action: %s' % meta

		return True

		

	def send( self, sock, msg ):
		self.debug_print( '<----', self.role, ':   \n', msg['mesg'] )
		sock.sendall( msg['mesg'] + "\n" )
		
		if 'body' in msg:
			self.debug_print( msg['body'] )
			sock.sendall( msg['body'] + "\n" )

		sock.sendall( '\n' )




	def __init__( self, hostname, port):
		self.hostname = hostname
		self.port = port
		self.start()

	def start( self ):

		self.out_msgs = Queue()
		self.files = {}
		self.plans = {}
		self.request_cnt = 0

		self.demo_merge_sort()

		try:
			self.sock = socket( AF_INET, SOCK_STREAM )
			try:
				self.sock.connect(( self.hostname, self.port ))
				self.ready = True

				
				#self.connection_thread = Thread( target=self.handler, args=([ self.sock ]) )
				#self.connection_thread.daemon = True
				#self.connection_thread.start()

				self.handler( self.sock, self.out_msgs )


			except KeyboardInterrupt:
				self.sock.close()
			except error as msg:
				self.ready = False
				self.sock.close()
		except error as msg:
			self.sock = None


		

	def stop( self ):
		self.sock.close()

	def restart( self ):
		self.stop()
		self.start()


	#def set_ui( self, ui ):
	#	self.ui = ui



	def handler( self, sock, out_msgs ):
		mybuffer = ''
		retries = []
		sock.settimeout(3)
		parser = Parser()
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
						self.receive( sock, objects )


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
