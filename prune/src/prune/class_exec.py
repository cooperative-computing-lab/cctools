import json, time

class Exec(object):
	__slots__ = ( 'key', 'cpu_time', 'duration', 'when', 'body' )
	def __init__( self, obj={}, **kwargs ):
		kwargs.update( obj )
		self.key = kwargs['key']

		if 'cpu_time' in kwargs:
			self.cpu_time = float(kwargs['cpu_time'])
		else:
			self.cpu_time = 0

		if 'duration' in kwargs:
			self.duration = float(kwargs['duration'])
		else:
			self.duration = 0

		if 'when' in kwargs:
			self.when = kwargs['when']
		else:
			self.when = time.time()

		if 'body' in kwargs:
			self.body = kwargs['body']
		else:
			self.body = {}


	def __str__( self ):
		obj = {'type':'exec', 'key':self.key}
		obj['cpu_time'] = self.cpu_time
		obj['duration'] = self.duration
		if self.when:
			obj['when'] = self.when
		res = json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')) + "\n" + json.dumps(self.body, sort_keys=True, indent=2, separators=(',', ': '))
		return res

	def __add__( self, other ):
		return str(self) + other
	def __radd__( self, other ):
		return other + str(self)
	def __eq__(self, other):
		return self.body == other.body
	def __ne__(self, other):
		return not self.__eq__(other)
