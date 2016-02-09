import json, time

class Envi(object):
	__slots__ = ( 'key', 'when', 'body', 'workflow_id' )
	def __init__( self, obj={}, **kwargs ):
		kwargs.update( obj )
		self.key = kwargs['key']

		if 'when' in kwargs:
			self.when = kwargs['when']
		else:
			self.when = time.time()

		if 'body' in kwargs:
			self.body = kwargs['body']
		else:
			self.body = ''

		if 'workflow_id' in kwargs:
			self.workflow_id = kwargs['workflow_id']
		else:
			self.workflow_id = None

	def full_str( self ):
		return str( self ) + "\n"
	def __str__( self ):
		obj = {'type':'envi', 'key':self.key, 'workflow_id':self.workflow_id}
		if self.when:
			obj['when'] = self.when

		return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + json.dumps(self.body, sort_keys=True, indent=2, separators=(',', ': '))

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
