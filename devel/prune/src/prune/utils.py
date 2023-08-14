# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys
import hashlib
import uuid as uuidlib

import timer

def uuid( ):
	return str( uuidlib.uuid4() )

def hashfile_copy( fname, newfname, blocksize=65536 ):
	timer.start('utils.hashfile_copy')
	key = hashlib.sha1()
	afile = open( fname, 'rb' )
	nfile = open( newfname, 'wb' )
	buf = afile.read( blocksize )
	length = len( buf )
	while len( buf ) > 0:
		nfile.write( buf )
		key.update( buf )
		buf = afile.read( blocksize )
		length += len( buf )
	key = key.hexdigest()
	timer.stop('utils.hashfile_copy')
	return key, length

def hashfile( fname, blocksize=65536 ):
	timer.start('utils.hashfile')
	key = hashlib.sha1()
	afile = open( fname, 'rb' )
	buf = afile.read( blocksize )
	length = len( buf )
	while len( buf ) > 0:
		key.update( buf )
		buf = afile.read( blocksize )
		length += len( buf )
	key = key.hexdigest()
	timer.stop('utils.hashfile')
	return key, length

def hashstring( str ):
	timer.start('utils.hashstring')
	key = hashlib.sha1()
	key.update( str )
	key = key.hexdigest()
	timer.stop('utils.hashstring')
	return key


def d( debug_type, *args, **kwargs ):
	#if debug_type in glob.debug_types:
		#print '_____'+debug_type+'_____'
		print ''
		for arg in args:
			print arg,
		for key in kwargs:
			if key == 'filename':
				filename = kwargs[key]
				if os.path.isfile( filename ):
					afile = open( filename, 'rb' )
					buf = afile.read( 2048 )
					while len( buf ) > 0:
						print buf,
						buf = afile.read( 2048 )
			else:
				print '-----'+key + ': ' + kwargs[key]
	#	print '-----'+debug_type+'-----'


def which( name, flags=os.X_OK ):
	result = []
	exts = filter(None, os.environ.get('PATHEXT', '').split(os.pathsep))
	path = os.environ.get('PATH', None)
	if path is None:
		return []
	for p in os.environ.get('PATH', '').split(os.pathsep):
		p = os.path.join(p, name)
		if os.access(p, flags):
			result.append(p)
		for e in exts:
			pext = p + e
			if os.access(pext, flags):
				result.append(pext)
	return result
