import json, time, os, shutil
import glob

class File(object):
	__slots__ = ( 'key', 'tree_key', 'when', 'path', 'size', 'body', 'workflow_id' )
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

		if 'workflow_id' in kwargs:
			self.workflow_id = kwargs['workflow_id']
		else:
			self.workflow_id = None

	def full_str( self ):
		obj = {'type':'file', 'key':self.key, 'workflow_id':self.workflow_id}
		if self.tree_key:
			obj['tree_key'] = self.tree_key
		if self.when:
			obj['when'] = self.when
		if self.path:
			obj['stream_length'] = self.size
			if glob.store_local and glob.store_local.primary and self.path[0] != '/':
				f = open( glob.store_local.primary.folder + self.path, 'rb' )
			else:
				f = open( self.path, 'rb' )
			data = ''
			buf = f.read(1024)
			while buf:
				data += buf
				buf = f.read(1024)

			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + data + "\n"
		else:
			obj['stream_length'] = len(self.body)
			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + self.body + "\n"
	def __str__( self ):
		obj = {'type':'file', 'key':self.key, 'workflow_id':self.workflow_id}
		if self.tree_key:
			obj['tree_key'] = self.tree_key
		if self.when:
			obj['when'] = self.when
		if self.path:
			obj['stream_length'] = self.size
			if glob.store_local and glob.store_local.primary and self.path[0] != '/':
				f = open( glob.store_local.primary.folder + self.path, 'rb' )
			else:
				f = open( self.path, 'rb' )
			buf = f.read(25)
			data = buf + ' ... '
			last_buf = ''
			while buf:
				last_buf = buf
				buf = f.read(1024)
			if len(last_buf)>28:
				data += last_buf[-25:]
			else:
				data += last_buf

			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + data
		else:
			obj['stream_length'] = len(self.body)
			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + self.body

	def str_w_file( self, filename ):
		obj = {'type':'file', 'key':self.key, 'workflow_id':self.workflow_id}
		if self.tree_key:
			obj['tree_key'] = self.tree_key
		if self.when:
			obj['when'] = self.when
		obj['path'] = filename
		if self.path:
			obj['size'] = self.size
			if glob.store_local and glob.store_local.primary:
				shutil.move( self.path, glob.store_local.primary.folder + filename )
			else:
				shutil.move( self.path, filename )
			self.path = filename
			return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
		else:
			obj['size'] = len( self.body )
			if glob.store_local and filename[0] != '/':
				f = open( glob.store_local.primary.folder + filename, 'wb' )
			else:
				f = open( filename, 'wb' )
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

	def __len__( self ):
		return len(str(self))
