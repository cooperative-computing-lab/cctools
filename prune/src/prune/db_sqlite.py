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

		timer.start('db.connect')

		self.dconn, dlog = self.sqlite3_connect(
			glob.data_db_pathname, glob.data_log_pathname, 'data_db_logger', glob.data_file_directory )

		self.cconn, clog = self.sqlite3_connect(
			glob.cache_db_pathname, glob.cache_log_pathname, 'cache_db_logger', glob.cache_file_directory )

		self.trconn, clog = self.sqlite3_connect(
			glob.trash_db_pathname, glob.trash_log_pathname, 'trash_db_logger', glob.trash_file_directory )

		self.tconn, self.tlog = self.sqlite3_connect( glob.work_db_pathname, glob.work_log_pathname )


		db_init_str = """
CREATE TABLE IF NOT EXISTS items (
	id INTEGER NOT NULL, type TEXT, cbid TEXT, dbid TEXT, wfid UUID, step TEXT,
	"when" FLOAT, meta TEXT, body TEXT, repo UUID, path TEXT, size INTEGER,
	PRIMARY KEY (id)
);

"""

		db_init_todos = """
CREATE TABLE IF NOT EXISTS todos (
	id INTEGER NOT NULL, cbid TEXT, wfid UUID, step TEXT, priority INTEGER DEFAULT 0,
	next_arg TEXT, assigned TEXT, failures INTEGER DEFAULT 0,
	PRIMARY KEY (id)
);

"""
		curs = self.dconn.cursor()
		curs.execute( db_init_str )
		curs.execute( 'CREATE INDEX IF NOT EXISTS itcbids ON items(cbid);' )
		curs.execute( 'CREATE INDEX IF NOT EXISTS itdbids ON items(dbid);' )
		self.dconn.commit()

		curs = self.cconn.cursor()
		curs.execute( db_init_str )
		curs.execute( 'CREATE INDEX IF NOT EXISTS itcbids ON items(cbid);' )
		curs.execute( 'CREATE INDEX IF NOT EXISTS itdbids ON items(dbid);' )
		self.cconn.commit()

		curs = self.trconn.cursor()
		curs.execute( db_init_str )
		curs.execute( 'CREATE INDEX IF NOT EXISTS itcbids ON items(cbid);' )
		curs.execute( 'CREATE INDEX IF NOT EXISTS itdbids ON items(dbid);' )
		self.trconn.commit()

		curs = self.tconn.cursor()
		curs.execute( db_init_todos )
		curs.execute( 'CREATE INDEX IF NOT EXISTS tdnext ON todos(next_arg, assigned);' )
		self.tconn.commit()
		self.tconn.isolation_level = 'EXCLUSIVE'

		timer.stop('db.connect')


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

		conn =  sqlite3.connect( db_pathname, timeout=30, detect_types=sqlite3.PARSE_DECLTYPES )
		conn.row_factory = dict_factory
		conn.text_factory = str
		#conn.execute("PRAGMA busy_timeout = 30000")
		return conn, log




	def insert( self, item ):
		timer.start('db.insert')

		action_taken = 'exists'
		if item.type == 'temp':
			# This is an intermediate file, use the cache
			if not self.exists_temp_dbid( item.dbid ):
				conn, file_dir, log = (self.cconn, glob.cache_file_directory, clog)
				action_taken = 'saved'

		else:
			# This is not intermediate data, don't use cache
			if not self.exists_data( item ):
				conn, file_dir, log = (self.dconn, glob.data_file_directory, dlog)
				action_taken = 'saved'

		if action_taken == 'saved':
			if item.path:
				new_pathname = file_dir + item.cbid
				if not os.path.isfile( new_pathname ):
					shutil.move( item.path, new_pathname )
				item.path = item.cbid
			ins, vals = item.sqlite3_insert()
			#print ins
			#print vals
			while True:
				try:
					curs = conn.cursor()
					curs.execute( ins, vals )
					conn.commit()
				except sqlite3.OperationalError:
					print 'Database (todos) is locked on insert_exec'
					time.sleep(1)
					continue
				break

			self.task_prep( item )


			if item.type == 'temp':
				log.info('%s (%s) %s' % (item.cbid, item.dbid, action_taken))
			else:
				log.info('%s %s' % (item.cbid, action_taken))


		timer.stop('db.insert')

		#if glob.total_quota:
		#	self.keep_quota()

		if action_taken == 'saved':
			return True
		else:
			return False



	def find( self, key ):
		timer.start('db.find')

		curs = self.dconn.cursor()
		curs.execute('SELECT * FROM items WHERE cbid=?', (key,) )
		res = curs.fetchall()
		if len(res)<=0:
			curs = self.cconn.cursor()
			curs.execute('SELECT * FROM items WHERE cbid=? OR dbid=?', (key, key,) )
			res = curs.fetchall()

		returns = []
		for r in res:
			returns.append( Item(r) )

		timer.stop('db.find')
		return returns



	def find_one( self, key ):
		timer.start('db.find_one')

		curs = self.dconn.cursor()
		curs.execute("SELECT * FROM items WHERE cbid=? ORDER BY id DESC LIMIT 1", (key,) )
		res = curs.fetchone()
		if not res:
			curs = self.cconn.cursor()
			curs.execute('SELECT * FROM items WHERE cbid=? OR dbid=? ORDER BY id DESC LIMIT 1', (key, key,) )
			res = curs.fetchone()
		if res:
			res = Item( res )
		timer.stop('db.find_one')
		return res

	def exists_temp_dbid( self, dbid ):
		timer.start('db.exists_temp')

		while True:
			try:
				curs = self.cconn.cursor()
				curs.execute('SELECT "when" FROM items WHERE dbid=?', (dbid,) )
				when = curs.fetchone()

				timer.stop('db.exists_temp')
				return when
			except sqlite3.OperationalError:
				print 'Database (todos) is locked on exists_temp_dbid'
				time.sleep(1)
				continue
			break

	def exists_temp( self, item ):
		timer.start('db.exists_temp')

		while True:
			try:
				curs = self.cconn.cursor()
				curs.execute('SELECT "when" FROM items WHERE cbid=? OR dbid=?', (item.cbid, item.dbid,) )
				when = curs.fetchone()

				timer.stop('db.exists_temp')
				return when
			except sqlite3.OperationalError:
				print 'Database (todos) is locked on exists_temp'
				time.sleep(1)
				continue
			break

	def exists_data( self, item ):
		timer.start('db.exists_data')

		while True:
			try:
				curs = self.dconn.cursor()
				curs.execute('SELECT "when" FROM items WHERE cbid=?', (item.cbid,) )
				when = curs.fetchone()

				timer.stop('db.exists_data')
				return when
			except sqlite3.OperationalError:
				print 'Database (todos) is locked on exists_data'
				time.sleep(1)
				continue
			break


	def keep_quota( self, quota_bytes ):
		timer.start('db.keep_quota')

		consumption = self.quota_status()
		print '%i %i %i'%(time.time(), consumption, quota_bytes)

		if consumption <= quota_bytes:
			pass
			#print 'Under Quota by:', (quota_bytes-consumption)
		else:
			over_consumption = consumption-quota_bytes
			#print 'Over Quota by:', over_consumption
			file_cnt = 0
			while over_consumption>0:
				try:

					curs = self.cconn.cursor()
					trcurs = self.trconn.cursor()
					curs.execute('SELECT * FROM items ORDER BY id LIMIT 25;')
					res = curs.fetchall()
					timer.stop('db.keep_quota')

					cbids = []
					for r in res:
						it = Item(r)
						if it.dbid != '685aa1bae538a9f5dba28a55858467f82f5142a8:0':
							#shutil.copy( glob.cache_file_directory+it.path, glob.trash_file_directory+it.path )
							ins, dat = it.sqlite3_insert()
							trcurs.execute( ins, dat )
							cbids.append( it.cbid )
							over_consumption -= it.size
							if over_consumption <= 0:
								break
					self.trconn.commit()
					print '# %i files put in the trash'%(len(cbids))
					#timer.start('db.keep_quota')

					for cbid in cbids:
						pass
						#print ' -'+cbid
						curs.execute( "DELETE FROM items WHERE cbid=?;", (cbid,) )
					self.cconn.commit()


				except sqlite3.OperationalError:
					print 'Database (cache) is locked on keep_quota'
					time.sleep(1)
					continue
				file_cnt += len(cbids)
			return file_cnt

	def restore_trash( self ):
		timer.start('db.restore_trash')

		while True:
			try:

				trcurs = self.trconn.cursor()

				trcurs.execute('SELECT * FROM items ORDER BY id LIMIT 5;')
				res = trcurs.fetchall()

				cbids = []
				curs = self.cconn.cursor()
				for r in res:
					it = Item(r)
					#shutil.copy( glob.cache_file_directory+it.path, glob.trash_file_directory+it.path )
					ins, dat = it.sqlite3_insert()
					curs.execute( ins, dat )
					cbids.append( it.cbid )
				self.cconn.commit()
				#print '------'
				#timer.start('db.keep_quota')

				for cbid in cbids:
					#print ' -'+cbid
					trcurs.execute( "DELETE FROM items WHERE cbid=?;", (cbid,) )
				self.trconn.commit()

				timer.stop('db.restore_trash')
				if len(cbids)==0:
					break

			except sqlite3.OperationalError:
				print 'Database error on restore_trash'
				time.sleep(1)
				continue
			#break
		timer.stop('db.restore_trash')


	def quota_status( self ):
		timer.start('db.quota_status')

		while True:
			consumption = 0
			try:
				curs = self.cconn.cursor()
				curs.execute('SELECT SUM(size) AS cache FROM items;')
				res = curs.fetchone()
				#print 'cache:', res['cache']
				consumption += res['cache']

				curs = self.dconn.cursor()
				curs.execute('SELECT SUM(size) AS data FROM items;')
				res = curs.fetchone()
				#print 'data:', res['data']
				consumption += res['data']

			except sqlite3.OperationalError:
				print 'Database (cache|data) is locked on quota_status'
				time.sleep(1)
				continue
			break

		statinfo = os.stat(glob.cache_db_pathname)
		size = statinfo.st_size
		#print 'cache_db:', size
		consumption += size

		statinfo = os.stat(glob.data_db_pathname)
		size = statinfo.st_size
		#print 'data_db:', size
		consumption += size


		timer.stop('db.quota_status')
		return consumption


	def dump( self, key, pathname ):
		timer.start('db.dump')

		item = self.find_one( key )
		if not item:
			print 'Not ready yet: %s' % key
		else:
			with open( pathname, 'w' ) as f:
				item.stream_content( f )

		timer.stop('db.dump')






	def task_add( self, call ):
		timer.start('db.task.add')

		while True:
			try:
				conn, log = (self.tconn, self.tlog)
				with conn:
					curs = conn.cursor()

					# Check whether the output files already exist
					outputs_exist = True
					for i in range( 0, len(call.body['returns']) ):
						dbid = call.cbid+':'+str(i)
						if not glob.db.exists_temp_dbid( dbid ):
							outputs_exist = False
							break

					if not outputs_exist:
						# Check whether the task is already queued up
						curs.execute('SELECT cbid FROM todos WHERE cbid=?', (call.cbid,) )
						res = curs.fetchone()
						if not res:
							# Find the first needed argument that is not already available
							next_arg = None
							if 'args' in call.body and len(call.body['args'])>0:
								for arg in call.body['args']:
									if not glob.db.find( arg ):
										next_arg = arg
										break
							ins = 'INSERT INTO todos (cbid, step, priority, next_arg) VALUES (?,?,0,?);'
							curs.execute( ins, (call.cbid, call.step, next_arg) )
							conn.commit()

							log.info('%s added' % (call.cbid))


			except sqlite3.OperationalError:
				print 'Database (todos) is locked on task_add'
				time.sleep(1)
				continue
			break


		timer.stop('db.task.add')

	def task_update( self, call ):
		timer.start('db.task.update')

		while True:
			try:

				conn, log = (self.tconn, self.tlog)
				with conn:
					curs = conn.cursor()

					# Check whether the output files already exist
					outputs_exist = True
					for i in range( 0, len(call.body['returns']) ):
						dbid = call.cbid+':'+str(i)
						if not glob.db.exists_temp_dbid( dbid ):
							outputs_exist = False
							break

					if not outputs_exist:
						# Find the first needed argument that is not already available
						next_arg = None
						if 'args' in call.body and len(call.body['args'])>0:
							for arg in call.body['args']:
								if not glob.db.find( arg ):
									next_arg = arg
									break
						upd = 'UPDATE todos SET next_arg=? WHERE cbid=?;'
						curs.execute( upd, (next_arg, call.cbid) )
						conn.commit()

			except sqlite3.OperationalError:
				print 'Database (todos) is locked on task_update'
				time.sleep(1)
				continue
			break

		timer.stop('db.task.update')

	def task_fail( self, call ):
		timer.start('db.task.fail')

		while True:
			try:

				conn, log = (self.tconn, self.tlog)
				with conn:
					curs = conn.cursor()

					upd = 'UPDATE todos SET assigned=? WHERE cbid=?;'
					curs.execute( upd, ('failed', call.cbid) )
					conn.commit()

			except sqlite3.OperationalError:
				print 'Database (todos) is locked on task_update'
				time.sleep(1)
				continue
			break

		timer.stop('db.task.fail')



	def task_claim( self, count=1 ):
		batch = uuid()
		timer.start('db.task.claim')
		while True:
			try:
				conn, log = (self.tconn, self.tlog)
				with conn:
					curs = conn.cursor()
					if glob.wq_stage:
						curs.execute('SELECT cbid FROM todos WHERE next_arg IS NULL AND assigned IS NULL AND step = ? LIMIT ?', (glob.wq_stage,count) )
					else:
						curs.execute('SELECT cbid FROM todos WHERE next_arg IS NULL AND assigned IS NULL ORDER BY id LIMIT ?', (count,) )

					res = curs.fetchall()

					cbids = []
					for r in res:
						cbids.append(r['cbid'])

					if len(cbids)>0:
						upd_str = 'UPDATE todos SET assigned = ? WHERE next_arg IS NULL AND assigned IS NULL AND cbid IN (%s);' % ', '.join('?' for c in cbids)
						#print upd_str
						#print [batch]+cbids
						curs.execute( upd_str, [batch]+cbids )
						conn.commit()

						#log.info('%i tasks assigned' % ( len(res) ))
						timer.stop('db.task.claim')
						return batch

			except sqlite3.OperationalError:
				print traceback.format_exc()
				print 'Database (todos) is locked on task_claim'
				time.sleep(0.95)
				continue
			break


		#timer.stop('db.task.claim')
		return None



	def task_get( self, batch ):
		calls = []
		timer.start('db.task.get')
		while True:
			try:

				conn, log = (self.tconn, self.tlog)
				with conn:
					curs = conn.cursor()
					curs.execute('SELECT cbid FROM todos WHERE assigned=?', (batch,) )
					res = curs.fetchall()

					for r in res:
						call = self.find_one( r['cbid'] )
						if call:
							calls.append( call )

			except sqlite3.OperationalError:
				print 'Database (todos) is locked on task_get'
				time.sleep(1)
				continue
			break

		timer.stop('db.task.get')
		return calls


	def task_prep( self, item ):
		calls = []
		timer.start('db.task.prep')

		while True:
			try:
				conn, log = (self.tconn, self.tlog)
				with conn:

					# Check if task is already queued
					curs = conn.cursor()
					curs.execute('SELECT cbid FROM todos WHERE next_arg IN (?,?)', (item.cbid, item.dbid,) )
					res = curs.fetchall()

					for r in res:
						call = self.find_one( r['cbid'] )
						if call:
							# Update next_arg for task
							self.task_update( call )

			except sqlite3.OperationalError:
				print 'Database (todos) is locked on task_prep'
				time.sleep(1)
				continue
			break

		timer.stop('db.task.prep')
		return calls


	def task_remain( self, wfid ):
		calls = []
		timer.start('db.task.remain')

		conn, log = (self.tconn, self.tlog)
		with conn:

			try:
				curs = conn.cursor()
				curs.execute('SELECT id FROM todos WHERE wfid = ?', (glob.workflow_id,) )
				res = curs.fetchall()

				timer.stop('db.task.remain')
				return len(res)
			except sqlite3.OperationalError:
				return 1

	def task_cnt( self ):
		calls = []
		timer.start('db.task.count')

		conn, log = (self.tconn, self.tlog)
		with conn:

			try:
				curs = conn.cursor()
				curs.execute('SELECT count(id) as cnt FROM todos WHERE 1')
				res = curs.fetchone()

				timer.stop('db.task.count')
				return int(res['cnt'])
			except sqlite3.OperationalError:
				return 1



	def task_del( self, cbid ):
		timer.start('db.task.del')

		while True:
			try:

				conn, log = (self.tconn, self.tlog)

				with conn:
					curs = self.tconn.cursor()

					del_str = 'DELETE FROM todos WHERE cbid = ?;'
					curs.execute( del_str, (cbid,) )
					conn.commit()

					log.info('%s deleted' % (cbid))

			except sqlite3.OperationalError:
				print 'Database (todos) is locked on task_del'
				time.sleep(1)
				continue
			break

		timer.stop('db.task.del')
