# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, traceback
import shutil
import uuid as uuidlib
import json as jsonlib

primary = None

class Folder:

	def __init__( self, new_folder ):
		if new_folder[-1] == '/':
			self.folder = new_folder
		else:
			self.folder = new_folder+'/'
		self.start()


	def start( self ):
		
		if not os.path.isdir( self.folder ):
			os.makedirs( self.folder )
		
		if not os.path.isdir( self.folder + 'tmp/' ):
			os.makedirs( self.folder + 'tmp/' )

		self.locks = {}
#		else:
#			if not self.devoted:
#				shutil.rmtree( self.folder )
#				os.makedirs( self.folder )

#		for use in self.uses:
#			if use == 'db_log':
#				self.db = db_log.Database( self.database_filename() )
#			elif use == 'sandboxes':
#				self.sandbox_folder = self.folder + 'sandboxes/'
#				os.makedirs( self.sandbox_folder )


	def stop():
		pass

	def restart( self ):
		self.stop()
		self.start()




	def database_file( self, volatile=False ):
		return self.file_ready( 'db', volatile )
	def database_folder( self, volatile=False ):
		return self.folder_ready( 'db/', volatile )
	def config_file( self, volatile=False ):
		return self.file_ready( 'config', volatile )
	def file_folder( self, volatile=False ):
		return self.folder_ready( 'files/', volatile )
	def sandbox_folder( self, volatile=False ):
		return self.folder_ready( 'sandboxes/', volatile )

	def new_sandbox( self, volatile=True ):
		uuid = uuidlib.uuid4()
		return self.folder+self.folder_ready( 'sandboxes/' + str(uuid) + '/', volatile )



	def folder_ready( self, folder, volatile=False ):
		if os.path.isdir( self.folder + folder ):
			if volatile:
				shutil.rmtree( self.folder + folder )
				os.makedirs( self.folder + folder )
		else:
			os.makedirs( self.folder + folder )
		return folder


	def file_ready( self, filename, volatile=False ):
		if volatile:
			with open( self.folder + filename, 'rw+' ) as f:
				f.truncate()
		elif not os.path.isfile( self.folder + filename ):
			open( self.folder + filename, 'a' ).close()
		return filename




	def open_stream( self, filename ):
		if os.path.isfile( self.folder + filename ):
			return open( self.folder + filename, 'a+' )
		else:
			return None

	def read_stream( self, filename ):
		if os.path.isfile( self.folder + filename ):
			return open( self.folder + filename, 'rb' )
		else:
			return None

	def write_stream( self, filename, stream, length ):
		uuid = uuidlib.uuid4()
		tmpfile = self.folder + 'tmp/' + str(uuid)
		f = open( tmpfile, 'wb+' )
		buf = stream.read(8192)
		while len( buf ) > 0:
			f.write( buf )
			buf = stream.read(8192)
		if os.path.isfile( self.folder + filename ):
			shutil.move( tmpfile, self.folder + filename + '.' + str(uuid) )
		else:
			shutil.move( tmpfile, self.folder + filename )
		return self.folder + filename

	def file_start( self, filename, data='' ):
		lock = uuidlib.uuid4()
		tmpfile = self.folder + 'tmp/' + str(lock)
		open_file = open( tmpfile, 'wb+' )
		self.locks[lock] = [ open_file, filename, tmpfile ]
		if len( data ) > 0:
			open_file.write( data )
		return lock

	def file_append( self, lock, data ):
		open_file, filename, tmpfile = self.locks[lock]
		open_file.write( data )

	def file_finish( self, lock, data ):
		open_file, filename, tmpfile = self.locks[lock]
		open_file.write( data )
		self.locks.pop( lock, None )
		open_file.close()
		if os.path.isfile( self.folder + filename ):
			shutil.move( tmpfile, self.folder + filename + '.' + str(lock) )
		else:
			shutil.move( tmpfile, self.folder + filename )


	def symlink( self, src, target ):
		os.symlink( self.folder + self.file_folder() + src, target )

	def copy( self, src, target ):
		shutil.copy( self.folder + self.file_folder() + src, target )

	def touch( self, src ):
		open( self.folder + self.file_folder() + self.nil, 'a' ).close()



