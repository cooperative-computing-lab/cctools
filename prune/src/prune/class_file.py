import json, time, os, shutil
from . import store_local

class File(object):
	__slots__ = ( 'key', 'tree_key', 'when', 'path', 'size', 'body' )
	def __init__( self, obj={}, **kwargs ):
		kwargs.update( obj )
		self.key = kwargs['key']

		if 'tree_key' in kwargs:
			self.tree_key = kwargs['tree_key']
		else:
			self.tree_key = None

		if 'when' in kwargs:
			self.when = kwargs['when']
		else:
			self.when = time.time()

		if 'path' in kwargs:
			self.body = None
			self.path = kwargs['path']
			self.size = int(kwargs['size'])
		elif 'body' in kwargs and kwargs['body']:
			self.body = kwargs['body']
			self.path = None
			self.size = len(self.body)
		else:
			self.body = None
			self.path = None
			self.size = 0
			print 'NO PATH OR BODY TO THIS FILE:', kwargs


	def __str__( self ):
		obj = {'type':'file', 'key':self.key}
		if self.tree_key:
			obj['tree_key'] = self.tree_key
		if self.when:
			obj['when'] = self.when
		if self.path:
			obj['stream_length'] = self.size
			if store_local.primary:
				f = open( store_local.primary.folder + self.path, 'rb' )
			else:
				f = open( self.path, 'rb' )
			data = ''
			buf = f.read(1024)
			while buf:
				data += buf
				buf = f.read(1024)
			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + data
		else:
			obj['stream_length'] = len(self.body)
			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + self.body
	def str_w_file( self, filename ):
		obj = {'type':'file', 'key':self.key}
		if self.tree_key:
			obj['tree_key'] = self.tree_key
		if self.when:
			obj['when'] = self.when
		obj['path'] = filename
		if self.path:
			obj['size'] = self.size
			shutil.move( self.path, store_local.primary.folder + filename )
			self.path = filename
			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
		else:
			obj['size'] = len( self.body )
			f = open( store_local.primary.folder + filename, 'wb' )
			f.write( self.body )
			f.close()
			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))

	def __add__( self, other ):
		return str(self) + other
	def __radd__( self, other ):
		return other + str(self)
	def __eq__(self, other):
		return self.body == other.body
	def __ne__(self, other):
		return not self.__eq__(other)
