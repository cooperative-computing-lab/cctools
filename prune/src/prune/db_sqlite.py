# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback, logging
import shutil

#import dataset
import sqlite3
import uuid as uuidlib

import glob
import timer
from class_item import *
from utils import *


sqlite3.register_converter('PUID', lambda b: uuidlib.UUID(b))
sqlite3.register_adapter(uuidlib.UUID, lambda u: str(u))

dlog = None
clog = None

class Database:
	def __init__( self ):

		while not glob.ready:
			print '.'
			time.sleep(1)

		self.connect()

	def connect( self ):
		global dlog, clog
	
		self.dconn, dlog = self.sqlite3_connect( 
			glob.data_db_pathname, glob.data_log_pathname, 'data_db_logger', glob.data_file_directory )
		
		self.cconn, clog = self.sqlite3_connect( 
			glob.cache_db_pathname, glob.cache_log_pathname, 'cache_db_logger', glob.cache_file_directory )

		self.tconn, self.tlog = self.sqlite3_connect( glob.work_db_pathname, glob.work_log_pathname )


		db_init_str = """
CREATE TABLE IF NOT EXISTS items (
	id INTEGER NOT NULL, type TEXT, cbid TEXT, dbid TEXT, wfid UUID, step TEXT,
	"when" FLOAT, meta TEXT, body TEXT, repo UUID, path TEXT, size INTEGER,
	PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS itcbids ON items(cbid);
CREATE INDEX IF NOT EXISTS itdbids ON items(dbid);

"""

		db_init_todos = """
CREATE TABLE IF NOT EXISTS todos (
	id INTEGER NOT NULL, cbid TEXT, wfid UUID, step TEXT, priority INTEGER DEFAULT 0,
	next_arg TEXT, assigned TEXT,
	PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS tdnext ON todos(next_arg, assigned);

"""
		curs = self.dconn.cursor()
		curs.execute( db_init_str )
		self.dconn.commit()
		
		curs = self.cconn.cursor()
		curs.execute( db_init_str )
		self.cconn.commit()
		
		curs = self.tconn.cursor()
		curs.execute( db_init_todos )
		self.tconn.commit()
		self.tconn.isolation_level = 'EXCLUSIVE'


	def sqlite3_connect( self, db_pathname, log_pathname=None, log_name=None, file_directory=None ):

		tmp_dir = os.path.dirname(db_pathname)
		if not os.path.exists(tmp_dir):
			os.makedirs(tmp_dir)
		if file_directory and not os.path.exists(file_directory):
			os.makedirs(file_directory)

		if log_pathname:
			tmp_dir = os.path.dirname(log_pathname)
			if not os.path.exists(tmp_dir):
				os.makedirs(tmp_dir)
			log = logging.getLogger(log_name)
			hdlr = logging.FileHandler(log_pathname)
			hdlr.setFormatter(logging.Formatter(glob.log_format))
			log.addHandler(hdlr)
			log.setLevel(logging.DEBUG)
			log.info('Connecting to DB: %s', db_pathname)
		else:
			log = None


		def dict_factory(cursor, row):
			d = {}
			for idx, col in enumerate(cursor.description):
				d[col[0]] = row[idx]
			return d

		conn =  sqlite3.connect( db_pathname, detect_types=sqlite3.PARSE_DECLTYPES )
		conn.row_factory = dict_factory
		conn.text_factory = str
		return conn, log




	def insert( self, item ):
		timer.start('db','insert')

		if item.type == 'temp':
			# This is an intermediate file, use the cache
			conn, file_dir, log = (self.cconn, glob.cache_file_directory, clog)
		else:
			# This is not intermediate data, don't use cache
			conn, file_dir, log = (self.dconn, glob.data_file_directory, dlog)
		curs = conn.cursor()

		if item.path:
			new_pathname = file_dir + item.cbid
			shutil.move( item.path, new_pathname )
			item.path = new_pathname

		#res = db.find_one( dbid=item.dbid )
		curs.execute('SELECT * FROM items WHERE cbid=?', (item.cbid,))
		res = curs.fetchone()
		if not res:
			ins, vals = item.sqlite3_insert()
			print ins
			print vals
			curs.execute( ins, vals )
			conn.commit()

			action_taken = 'saved'
			#curs.execute('SELECT * FROM items WHERE cbid=?', (item.cbid,))
			#res = curs.fetchone()
			#item = Item( res )
		else:
			#item = Item( res )
			action_taken = 'exists'
		
		if item.type == 'temp':
			log.info('%s (%s) %s' % (item.cbid, item.dbid, action_taken))
		else:
			log.info('%s %s' % (item.cbid, action_taken))

		timer.stop('db','insert')
		
		if not res:
			return True
		else:
			return False
	


	def find( self, key ):
		timer.start('db','find')

		curs = self.dcurs.cursor()
		curs.execute('SELECT * FROM items WHERE cbid=?', (key,) )
		res = curs.fetchall()
		if len(res)<=0:
			curs = self.ccurs
			curs.execute('SELECT * FROM items WHERE cbid=? OR dbid=?', (key, key,) )
			res = curs.fetchall()

		returns = []
		for r in res:
			returns.append( Item(r) )

		timer.stop('db','find')
		return returns
	


	def find_one( self, key ):
		timer.start('db','find_one')

		curs = self.dcurs.cursor()
		curs.execute('SELECT * FROM items WHERE cbid=?', (key,) )
		res = curs.fetchone()
		if res:
			curs = self.ccurs
			curs.execute('SELECT * FROM items WHERE cbid=? OR dbid=?', (key, key,) )
			res = curs.fetchone()

		timer.stop('db','find_one')
		return Item(res)



	def dump( self, key, pathname ):
		timer.start('db','dump')
		
		obj = self.find_one( key )
		if not obj:
			print 'Need to wait for: %s' % key
		else:
			item = Item( obj )
			with open( pathname, 'w' ) as f:
				item.stream_content( f )

		timer.stop('db','dump')





	def task_add( self, call ):
		timer.start('db','task_add')
		print call


		# Check if is_ready


		conn, log = (self.tconn, self.tlog)
		with conn:			
			curs = conn.cursor()

			ins = 'INSERT INTO todos (cbid, step, priority, next_arg) VALUES (?,?,0,?);'
			next_arg = None
			if 'args' in call.body and len(call.body['args'])>0:
				for arg in call.body['args']:
					if not self.db.find( arg ):
						next_arg = arg
						break
			curs.execute( ins, (call.cbid, call.step, next_arg) )
			conn.commit()

			log.info('%s added' % (call.cbid))
			
			timer.stop('db','task_add')



	def task_claim( self, count=1 ):
		batch = uuid
		timer.start('db','task_claim')

		conn, log = (self.tconn, self.tlog)
		with conn:		
			curs = conn.cursor()
			curs.execute('SELECT cbid FROM todos WHERE next_arg=?, assigned=? LIMIT ?', (None,0,count) )
			res = curs.fetchall()
			
			for r in res:
				cbids.append(r['cbid'])

			upd_str = 'UPDATE todos SET assigned = ? WHERE next_arg=? AND assigned=? AND cbid IN (%s);' % ', '.join('?' for c in cbids)
			conn.execute( upd_str, (batch, None, None, cbids) )

		#log.info('%i tasks assigned' % ( len(res) ))
			
		timer.stop('db','task_claim')
		return batch



	def task_get( self, batch ):
		calls = []
		timer.start('db','task_get')

		conn, log = (self.tconn, self.tlog)
		with conn:
			curs = conn.cursor()
			curs.execute('SELECT cbid FROM todos WHERE assigned=?', (batch) )
			res = curs.fetchall()
			
			for r in res:
				call = self.find( r['cbid'] )
				if call:
					calls.append( call )

		timer.stop('db','task_get')
		return calls


	def task_del( self, cbid ):
		timer.start('db','task_del')

		conn, log = (self.tconn, self.tlog)

		with conn:			
			curs = self.tconn.cursor()

			del_str = 'DELETE FROM todos WHERE cbid = ?;'
			curs.execute( del_str, (cbid) )
			conn.commit()

			log.info('%s deleted' % (cbid))

		timer.stop('db','task_del')


	'''
	def insert( self, item ):
		timer.start('db','insert')

		if item.type == 'temp':
			# This is an intermediate file, use the cache
			db = self.citems
			log = self.clog
		else:
			# This is not intermediate data, don't use cache
			db = self.ditems
			log = self.dlog
		res = db.find_one( dbid=item.dbid )
		if not res:
			db.insert( item.export() )

			log.info('%s (%s) saved in database' % (item.cbid, item.dbid))
			res = db.find_one( dbid=item.dbid )
		else:
			log.info('%s (%s) saved in database' % (item.cbid, item.dbid))
		item = Item( res )

		timer.stop('db','insert')
		return item

	def find( self, item ):
		timer.start('db','find')
		if item.type == 'temp':
			# This is an intermediate file, use the cache
			db = self.citems
			log = self.clog
		else:
			# This is not intermediate data, don't use cache
			db = self.ditems
			log = self.dlog

		res = db.find( cbid=item.cbid )

		timer.stop('db','find')
		return res


	def find_one( self, item ):
		timer.start('db','find_one')
		if item.type == 'temp':
			# This is an intermediate file, use the cache
			db = self.citems
			log = self.clog
		else:
			# This is not intermediate data, don't use cache
			db = self.ditems
			log = self.dlog

		res = db.find_one( cbid=item.cbid )

		timer.stop('db','find_one')
		return res

	def dump( self, key, pathname ):
		timer.start('db','dump')
		
		t = self.citems.find_one( dbid=key )
		if not t:
			t = self.ditems.find_one( cbid=key )
		if not t:
			t = self.citems.find_one( cbid=key )
		if not t:
			print 'Need to wait for: %s' % key
		else:
			item = Item( t )
			with open( pathname, 'w' ) as f:
				item.stream_content( f )

		timer.stop('db','dump')
	'''
