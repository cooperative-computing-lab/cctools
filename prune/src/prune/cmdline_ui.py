# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, traceback
import readline, re
import uuid as uuidlib
import shutil

#from . import abstract
#from . import scanner
#from . import master

CWD = os.getcwd()


class CmdLine:
	master = None
	prompt = 'PRUNE$ '
	
	def __init__( self, master ):
		self.master = master
		self.regex = re.compile( r'(|)' )
		self.re_command = re.compile( r'[ \n\t\r]*([a-zA-Z_][a-zA-Z0-9_]*)' )
		#self.re_args = re.compile('[^\s"\']+|"[^"]*"|\'[^\']*\'')
		self.re_import = re.compile( '([^\s"\']+|"[^"]*"|\'[^\']*\'):?(\$?[^\s]*)?' )
		self.re_export = re.compile( '(\$?[^\s]*):([^\s"\']+|"[^"]*"|\'[^\']*\')' )
		self.re_words = re.compile( '[ \n\t\r]*([^ \n\t\r]*)[ \n\t\r]*' )
		self.callbacks = {}
		self.start()


	def help_message( self ):
		return '''
List of available commands:
EVAL (default,optional),  IMPORT, EXPORT,  USE, WORK,  STATUS,  EXIT, QUIT,  CAT, LS, CUT,  RUN,  RESET
See the manual for more details.
		'''

	def start( self ):
		try:
			while not self.master.terminate:
				self.do_line( raw_input( self.prompt ) )
		except ( KeyboardInterrupt, EOFError ):
			print '\nPrune exiting...'
			self.master.stop()
		except Exception:
			print traceback.format_exc()


	def do_line( self, line ):
		match = self.re_command.search( line )
		if match:
			command = match.group(0).lower()
			try:
				remain = line[len(command):].strip()
				getattr( self, command+'_command' )(remain)
			except AttributeError:
				default_command( line )


	def put_command( self, line ):
		self.import_command( line )
	def import_command( self, line ):
		args = self.re_import.findall( line )
		for ( src, target ) in args:
			if src[0] == '"' or src[0] == "'":
				src = src[1:-1]
			print src, '->', target

			if not target:
				target = uuidlib.uuid4()

			chksum, length = self.hashfile( src )
			msg = { 'type':'data', 'chksum':chksum, 'binary':length, '_key':target }
			self.master.log_client.send( msg, src )
			
					


	def get_command( self, line ):
		self.export_command( line )
	def export_command( self, line ):
		print 'line:',line
		args = self.re_export.findall( line )
		for ( src, target ) in args:
			if target[0] == '"' or target[0] == "'":
				target = target[1:-1]
			print src, '->', target

			msg = { 'action':'export', '_key':src }
			self.master.log_client.send( msg, target )




	def env_command( self, line ):
		self.export_command( line )
	def environment_command( self, line ):
		pass



	def default_command( self, line ):
		print line
		print 'command not understood:',line



	def parse( self, line ):
		print 'parse:', line



	def hashfile( self, fname, blocksize=65536 ):
		key = hashlib.sha1()
		afile = open( fname, 'rb' )
		buf = afile.read( blocksize )
		length = len( buf )
		while len( buf ) > 0:
			key.update( buf )
			buf = afile.read( blocksize )
			length += len( buf )
		return key.hexdigest(), length


'''
KEY starts with $
LITERAL surrounded by "" or ''

Built in functions Import, Export
KEY|NAME...KEY|NAME = IMPORT(LITERAL...LITERAL)
EXPORT(KEY,LITERAL...KEY,LITERAL)

NAME|KEY = IMPORT(NAME|LITERAL|KEY)
[NAME|KEY...NAME|KEY] = IMPORT([NAME|LITERAL|KEY...NAME|LITERAL|KEY])

EXPORT(KEY,LITERAL)
EXPORT([KEY,LITERAL...KEY,LITERAL])

{_key:uuid, data:null, src:invocation_key} #file
{_key:uuid, data:"alskdjf", src:invocation_key} #literal

IMPORT function file...
prune.input(local_file filename)
if file_exists(prune.cwd()+filename)
	prune.output(local_file filename)
else prune.error('File not found')

EXPORT function file...
prune.input(file sandbox_filename, local_file local_filename)
mv sandbox_filename prune.cwd()+local_filename
prune.output()

current working directory?

#IMPORT
{_key:U87436, exec:<import>, inputs:{}, outputs:{U23475:'local_name'}}
{_key:U23475, data:null, src:U87436, length:6453}
...

'''

