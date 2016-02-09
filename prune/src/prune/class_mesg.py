import json
class Mesg(object):
	__slots__ = ('action', 'key', 'flow')
	def __init__(self, obj={}, **kwargs):
		kwargs.update( obj )
		self.action = kwargs['action']
		if 'key' in kwargs:
			self.key = kwargs['key']
		else:
			self.key = None
		if 'flow' in kwargs:
			self.flow = kwargs['flow']
		else:
			self.flow = None

  
	def __str__(self):
		obj = {'action':self.action}
		if self.key:
			obj['key'] = self.key
		if self.flow:
			obj['flow'] = self.flow
		return json.dumps( obj, sort_keys=True, indent=2, separators=(',', ': ') )
	def __add__(self, other):
		return str(self) + other
	def __radd__(self, other):
		return other + str(self)
	def __eq__(self, other):
		if isinstance(other, self.__class__):
			if self.__slots__ == other.__slots__:
				attr_getters = [operator.attrgetter(attr) for attr in self.__slots__]
				return all(getter(self) == getter(other) for getter in attr_getters)
		return False
	def __ne__(self, other):
		return not self.__eq__(other)


