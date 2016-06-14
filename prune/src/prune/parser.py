import json, uuid, io, sys, time

import glob
from utils import uuid

class Parser:
	def __init__( self, base_dir='./', file_threshold=sys.maxint, one_file=True, debug=False ):
		self.base_dir = base_dir
		self.obj_buffer = ''
		self.stream = None
		self.stream_fname = None
		self.stream_size = 0
		self.stream_remain = 0
		self.my_buffer = ''
		self.use_file = False
		self.nospace = 0
		self.file_threshold = file_threshold
		self.one_file = one_file
		self.objects = []
		self.results = []
		self.debug = debug

	def parse(self, new_buffer):
		self.my_buffer += new_buffer
		while len(self.my_buffer) > 0:
			if self.stream and self.stream_remain > 0:
				if self.stream_remain >= len(self.my_buffer):
					if self.debug:
						if self.stream_size<256 or len(self.my_buffer)<10:
							print 'binary:',self.my_buffer,
						else:
							print 'binary:',self.my_buffer[0:10],'.-.',self.my_buffer[-10:],'!!\n',
					self.stream.write( self.my_buffer )
					self.stream_remain -= len(self.my_buffer)
					self.my_buffer = ''
				else:
					if self.debug:
						if self.stream_size<127 or self.stream_remain<10:
							print 'binary:',self.my_buffer,
						else:
							print 'binary:',self.my_buffer[0:10],'._.',self.my_buffer[-10:],'!!!\n',
					self.stream.write( self.my_buffer[0:self.stream_remain] )
					self.my_buffer = self.my_buffer[self.stream_remain+1:]
					self.stream_remain = 0


				if self.stream_remain <= 0:
					if self.use_file:
						obj = self.objects[-1]
						obj['path'] = self.stream_fname
						obj['size'] = self.stream_size
						self.objects.append( None )
						self.stream_fname = None
					else:
						self.objects.append( self.stream.getvalue() )
					self.stream.close()
					self.stream = None


			else:
				pos = self.my_buffer.find( '\n', self.nospace )
				if pos < 0:
					tlen = len(self.my_buffer)
					self.nospace = tlen
					break
				else:
					line = self.my_buffer[0:pos]
					self.my_buffer = self.my_buffer[pos+1:]
					self.nospace = 0
					if self.debug:
						print 'line:',line
					if len(line) == 0:
						self.results.append( self.objects )
						self.objects = []
						if self.debug:
							print 'end message.'

					elif line[0] == '{':
						self.obj_buffer = '{'

					elif line[0] == '}':
						self.obj_buffer += line
						try:
							obj = json.loads( self.obj_buffer )
						except ValueError, e:
							print e
							print "Error reading json:"
							print self.obj_buffer
						if 'stream_length' in obj:
							self.stream_size = self.stream_remain = int(obj['stream_length'])
							del obj['stream_length']
							if (obj['type'] == 'file' and self.stream_size > self.file_threshold) or ('tree_key' in obj and not self.one_file):
								self.use_file = True
								if glob.store:
									self.stream_fname = glob.store.tmp_file_path( ) + uuid()
								else:
									self.stream_fname = 'tmp_' + uuid()

								self.stream = open( self.stream_fname, 'w+' )
							else:
								self.use_file = False
								self.stream = io.BytesIO(b"")
						self.objects.append( obj )
						self.obj_buffer = ''

					else:
						self.obj_buffer += line
			time.sleep(0.001)

		final_results = self.results
		self.results = []
		return final_results
