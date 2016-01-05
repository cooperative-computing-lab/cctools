# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback
import json as jsonlib
import uuid as uuidlib

from class_db_node import DbNode
from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi
from parser import Parser



from json import JSONEncoder
JSONEncoder_olddefault = JSONEncoder.default
def JSONEncoder_newdefault( self, o ):
	if isinstance( o, Entry ):
		return o.__dict__
	return JSONEncoder_olddefault( self, o )
JSONEncoder.default = JSONEncoder_newdefault


class Entry:
	def __init__( self ):
		self.obj = None
		self.lineage = []
		self.progeny = []
		# obj=data, lineage=promisure, progeny=promisures
		# obj=promisure(env,cmd), lineage=ins, progeny=outs
		pass

class Database:
	entries = []
	index = {}
	ready = False
	debug = False
	names = {}
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'


	def __init__( self, drive ):
		#self.debug_level = kwargs['debug_level'] if ('debug_level' in kwargs) else 'all'
		self.drive = drive
		self.file_folder = self.drive.file_folder()
		self.file_path = self.drive.folder + self.file_folder
		self.start()


	def start( self ):
		if not self.ready:
			self.position = 0
			parser = Parser()
			self.open_file = self.drive.open_stream( self.drive.database_file() )
			self.open_file.seek(0)
			for line in self.open_file:
				results = parser.parse( line )
				if results:
					for objects in results:
						i = 0
						while i<len(objects):
							obj = objects[i]
							body = objects[i+1]

							if obj['type'] == 'envi':
								en = Envi( obj, body=body )
								self.index_obj( en )
							elif obj['type'] == 'call':
								cl = Call( obj, body=body )
								self.index_obj( cl )
							elif obj['type'] == 'file':
								fl = File( obj, body=body )
								self.index_obj( fl )
							elif obj['exec'] == 'exec':
								ex = Exec( obj, body=body )
								self.index_obj( ex )

							i += 2

			self.ready = True


	def stop( self ):
		if self.ready:
			self.ready = False
			#self.entries = []
			self.index = {}
			self.sha1sums = {}
			self.names = {}
			self.open_file.close()

	def restart( self ):
		if self.ready:
			self.stop()
		self.start()



	def debug_print( self, *args ):
		if self.debug:
			for arg in args:
				print arg,
			print ''

	def debug_file( self, filename ):
		if self.debug:
			if os.path.isfile( filename ):
				afile = open( filename, 'rb' )
				buf = afile.read( 2048 )
				while len( buf ) > 0:
					print buf,
					buf = afile.read( 2048 )


	def fetch_body( self, hash_index ):
		if not self.ready:
			return None
		if hash_index in self.index:
			if self.index[ hash_index ][6]:
				return self.index[ hash_index ][6]
			else:
				body = ''
				afile = self.read_stream( hash_index )
				buf = afile.read( 2048 )
				while len( buf ) > 0:
					body += buf
					buf = afile.read( 2048 )
				return body

	def fetch( self, _key ):
		if not self.ready:
			return None
		if _key in self.index:
			return self.index[ _key ]
		else:
			return None

	def exists( self, _key ):
		if not self.ready:
			return None
		if _key in self.index:
			return True
		else:
			return None

	def store( self, obj ):
		if not self.ready:
			return False
		stored = False

		node = self.fetch( obj.key )
		if not node:

			if isinstance(obj, File) and (obj.path or obj.tree_key):
				obj_str = obj.str_w_file( self.file_folder + obj.key )
				self.open_file.write( obj_str + "\n" )
				self.open_file.write( "\n" )
				self.open_file.flush()
			else:
				self.open_file.write( obj + "\n" )
				self.open_file.write( "\n" )
				self.open_file.flush()
			stored = True

		self.index_obj( obj )

		if stored:
			return True
	
	def index_obj( self, obj ):
		node = DbNode( obj=obj )
		self.entries.append( node )

		self.index[obj.key] = node
		if isinstance(obj, File) and obj.tree_key:
			self.index[obj.tree_key] = node


	
	def read_stream( self, _key ):
		return self.drive.read_stream( self.file_folder + _key )

	def write_stream( self, _key, stream, length=-1 ):
		return self.drive.write_stream( self.file_folder + _key, stream, length )

	def file_start( self, _key, data='' ):
		return self.drive.file_start( self.file_folder + _key, data )


	def file_append( self, lock, data ):
		return self.drive.file_append( lock, data )

	def file_finish( self, lock, data='' ):
		return self.drive.file_finish( lock, data )

	def put_file( self, src, target ):
		if src == self.nil:
			return self.drive.symlink( src, target )
		node = self.fetch( src )
		if node.obj.path:
			self.drive.symlink( src, target )
		else:
			f = open( target, 'wb' )
			f.write(node.obj.body)
			f.close()

	def index_json( self, meta ):
		header = meta[0:2]
		prefix = meta[2:6]
		body = meta[6]
		location, call_hash = header
		data_type, hash_index, length, packaging = prefix

		self.index[ hash_index ] = meta
		self.entries.append( hash_index )
		if call_hash:
			self.index[ call_hash ] = meta
			self.entries.append( call_hash )



	def lineage( self, _key ):
		meta = self.index[ _key ]
		header = meta[0:2]
		prefix = meta[2:6]
		body = meta[6]
		location, call_hash = header
		data_type, hash_index, length, packaging = prefix

		if len(hash_index)>130:
			return hash_index[0:130]
		else:
			return None

	def progeny( self, _key ):
		meta = self.index[ _key ]
		header = meta[0:2]
		prefix = meta[2:6]
		body = meta[6]
		location, call_hash = header
		data_type, hash_index, length, packaging = prefix

		returns = []
		for x in range(0, len(body['results']) ):
			returns.append(hash_index+str(x))
		return returns

	'''
	def scan(self,start,length=1):
		if length>0:
			return self.entries[ start : start+length ]
		else:
			return []


	def count(self):
		return len(self.entries)
	'''

	def name( self, name, roffset=0, cnt=1, when=None ):
		if name not in self.names:
			return None
		arr = self.names[ name ]
		if not when:
			if roffset >= len( self.names[ name ] ):
				return None
			elif cnt == 1:
				return self.names[ name ][ roffset ]
			else:
				return self.names[ name ][ roffset : roffset+cnt ]
		else:
			#result = None
			for name_info in self.names[ name ]:
				if name_info['when'] > when:
					break
				else:
					result = name_info
			return result

	'''
	def debug( self, *args, **kwargs):
		if self.debug_level=='all':
			for arg in args:
				print arg
			for (key,value) in kwargs.items():
				print key,':',value
	'''


