import json, time

import glob
import timer
from utils import *

class Item(object):
	__slots__ = ( 'type', 'cbid', 'dbid', 'wfid', 'step', 'when', 'meta', 'body', 'repo', 'path', 'size', 'id' )
	def __init__( self, obj={}, **kwargs ):
		# Three situations: Network streamed data (obj), Database data (obj), Newly created data (kwargs)
		kwargs.update( obj )
		self.type = kwargs['type']

		self.cbid = kwargs['cbid'] if 'cbid' in kwargs else None
		self.dbid = kwargs['dbid'] if 'dbid' in kwargs else None
		self.wfid = kwargs['wfid'] if 'wfid' in kwargs else glob.workflow_id
		self.step = kwargs['step'] if 'step' in kwargs else glob.workflow_step
		self.when = kwargs['when'] if 'when' in kwargs else time.time()
		self.id = kwargs['id'] if 'id' in kwargs else None

		if 'meta' in kwargs:
			if isinstance( kwargs['meta'], basestring ):
				self.meta = json.loads( kwargs['meta'] )
			else:
				self.meta = kwargs['meta']
		else:
			self.meta = {}

		self.body = None
		self.repo = None
		self.path = None
		self.size = 0
		if 'body' in kwargs and kwargs['body'] != None:
			if isinstance( kwargs['body'], basestring ):
				self.body = json.loads( kwargs['body'] )
				tmp_str = kwargs['body']
			else:
				self.body = kwargs['body']
				tmp_str = json.dumps( kwargs['body'], sort_keys=True )
			self.size = len(tmp_str)
			if not self.cbid:
				self.cbid = hashstring(tmp_str)

		elif 'repo' in kwargs and kwargs['repo'] != None:
			self.repo = kwargs['repo']
			if not self.cbid:
				log.error("No cbid for an object in a remote repository. There is no way to obtain it.")

		elif 'path' in kwargs and kwargs['path'] != None:
			self.path = kwargs['path']
			if not self.cbid:
				if 'new_path' in kwargs:
					self.cbid, self.size = hashfile_copy(self.path, kwargs['new_path'])
					self.path = kwargs['new_path']
				else:
					self.cbid, self.size = hashfile(self.path)
			elif 'size' in kwargs and kwargs['size'] != None:
				self.size = int(kwargs['size'])
			elif os.path.isfile(self.path):
				statinfo = os.stat(self.path)
				self.size = statinfo.st_size
			elif os.path.isfile(glob.data_file_directory+self.path):
				statinfo = os.stat(glob.data_file_directory+self.path)
				self.size = statinfo.st_size
			elif os.path.isfile(glob.cache_file_directory+self.path):
				statinfo = os.stat(glob.cache_file_directory+self.path)
				self.size = statinfo.st_size
			else:
				print "Can't find the file!!!"


	def __str__( self ):
		obj = dict(type=self.type, cbid=self.cbid, when=self.when)
		if self.dbid: obj['dbid'] = self.dbid
		if self.wfid: obj['wfid'] = self.wfid
		if self.step: obj['step'] = self.step
		if self.meta:
			if isinstance( self.meta, basestring ):
				#obj['meta'] = self.meta
				obj['meta'] = json.loads(self.meta)
			else:
				#obj['meta'] = json.dumps(self.meta, sort_keys=True)
				obj['meta'] = self.meta

		if self.size:
			obj['size'] = self.size
		if self.body:
			if isinstance( self.body, basestring ):
				#obj['body'] = self.body[0:20]+' ... '+self.body[-20:]
				obj['body'] = json.loads(self.body)
			else:
				#obj['body'] = json.dumps(self.body, sort_keys=True)
				obj['body'] = self.body
		elif self.repo:
			obj['repo'] = self.repo
		elif self.path:
			obj['path'] = self.path
		return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n"

	def stream( self, active_stream ):
		obj = dict(type=self.type, cbid=self.cbid, when=self.when)
		summary = ''
		if self.dbid: obj['dbid'] = self.dbid
		if self.wfid: obj['wfid'] = self.wfid
		if self.step: obj['step'] = self.step
		if self.meta: obj['meta'] = self.meta
		if self.size: obj['size'] = self.size
		if self.body:
			obj['body'] = self.body
			active_stream.write( json.dumps(obj, sort_keys=True) + "\n" )
		elif self.repo:
			obj['repo'] = self.repo
			active_stream.write( json.dumps(obj, sort_keys=True) + "\n" )
		elif self.path:
			active_stream.write( json.dumps(obj, sort_keys=True) + "\n" )
			summary = self.stream_content( active_stream )

		return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + summary + "\n"


	def export( self ):
		obj = dict(type=self.type, cbid=self.cbid, when=self.when)
		if self.dbid: obj['dbid'] = self.dbid
		if self.wfid: obj['wfid'] = self.wfid
		if self.step: obj['step'] = self.step
		if self.meta: obj['meta'] = self.meta
		if self.size: obj['size'] = self.size
		if self.body:
			obj['body'] = self.body
		elif self.repo:
			obj['repo'] = self.repo
		elif self.path:
			obj['path'] = self.path
		return obj
	def dump( self ):
		return json.dumps(self.export(), sort_keys=True)

	def stream_content( self, active_stream ):
		if self.body:
			if isinstance( self.body, basestring ):
				wstr = self.body
			else:
				wstr = json.dumps(self.body, sort_keys=True)
			active_stream.write( wstr )
			if len(wstr)>45:
				return wstr[0:20] + ' ... ' + wstr[-20:]
			else:
				return wstr

		elif self.repo:
			print "Stream from repository not implemented..."
			return None
		elif self.path:
			if self.type=='temp':
				pathname = glob.cache_file_directory + self.path
			else:
				pathname = glob.data_file_directory + self.path

			with open( pathname, 'r' ) as f:
				buf = f.read(1024*1024)
				lastbuf = ''
				summary = buf[0:20] + ' ... ' if len(buf)>20 else buf
				while buf:
					active_stream.write( buf )
					lastbuf = buf
					buf = f.read(1024*1024)
				summary = summary + buf[-20:] if len(lastbuf)>20 else summary + buf
			return summary

	def sqlite3_insert( self ):
		keys = ['type','cbid','"when"']
		vals = [self.type, self.cbid, self.when]
		if self.id:
			keys.append( 'id' )
			vals.append( self.id )
		if self.dbid:
			keys.append( 'dbid' )
			vals.append( self.dbid )
		if self.wfid:
			keys.append( 'wfid' )
			vals.append( self.wfid )
		if self.step:
			keys.append( 'step' )
			vals.append( self.step )
		if self.meta:
			keys.append( 'meta' )
			vals.append( json.dumps(self.meta, sort_keys=True) )
		if self.size:
			keys.append( 'size' )
			vals.append( self.size )
		if self.body:
			keys.append( 'body' )
			vals.append( json.dumps(self.body, sort_keys=True) )
		elif self.repo:
			keys.append( 'repo' )
			vals.append( self.repo )
		elif self.path:
			keys.append( 'path' )
			vals.append( self.path )


		qs = ['?'] * len(keys)
		ins = 'INSERT INTO items (%s) VALUES (%s);' % (','.join(keys), ','.join(qs))
		return ins, tuple(vals)


	#def __add__( self, other ):
	#	return str(self) + other
	#def __radd__( self, other ):
	#	return other + str(self)
	def __eq__(self, other):
		return self.cbid == other.cbid
	def __ne__(self, other):
		return not self.__eq__(other)
	def __len__( self ):
		return len(str(self))
