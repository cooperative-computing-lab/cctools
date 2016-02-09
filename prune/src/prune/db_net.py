# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback, logging

import dataset

import glob
from class_item import *


dlog = None
clog = None

class Database:
	def __init__( self, hostname, port ):
		global dlog, clog

		self.read_time = 0
		self.reads = 0
		self.write_time = 0
		self.writes = 0

		while not glob.ready:
			time.sleep(1)

		tmp_dir = os.path.dirname(glob.data_log_pathname)
		if not os.path.exists(tmp_dir):
			os.makedirs(tmp_dir)
		dlog = logging.getLogger('data_db_logger')
		hdlr_1 = logging.FileHandler(glob.data_log_pathname)
		hdlr_1.setFormatter(logging.Formatter(glob.log_format))
		dlog.addHandler(hdlr_1)
		dlog.setLevel(logging.DEBUG)
		self.dlog = dlog

		tmp_dir = os.path.dirname(glob.cache_log_pathname)
		if not os.path.exists(tmp_dir):
			os.makedirs(tmp_dir)
		clog = logging.getLogger('cache_db_logger')
		hdlr_2 = logging.FileHandler(glob.cache_log_pathname)
		hdlr_2.setFormatter(logging.Formatter(glob.log_format))
		clog.addHandler(hdlr_2)
		clog.setLevel(logging.DEBUG)
		self.clog = clog


		self.connect()

	def connect( self ):

		tmp_dir = os.path.dirname(glob.data_file_directory)
		if not os.path.exists(tmp_dir):
			os.makedirs(tmp_dir)
		dlog.info('Connecting to main DB: ', glob.data_db_pathname)
		self.ddb = dataset.connect( glob.data_db_pathname )
		self.ditems = self.ddb['items']

		tmp_dir = os.path.dirname(glob.cache_file_directory)
		if not os.path.exists(tmp_dir):
			os.makedirs(tmp_dir)
		clog.info('Connecting to cache DB: ', glob.cache_db_pathname)
		self.cdb = dataset.connect( glob.cache_db_pathname )
		self.citems = self.cdb['items']


	def insert( self, item ):
		start = time.time()
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

		self.write_time += time.time()-start
		self.writes += 1
		return item

	def find( self, item ):
		start = time.time()
		if item.type == 'temp':
			# This is an intermediate file, use the cache
			db = self.citems
			log = self.clog
		else:
			# This is not intermediate data, don't use cache
			db = self.ditems
			log = self.dlog

		res = db.find( cbid=item.cbid )

		self.read_time += time.time()-start
		self.reads += 1
		return res

	def find_one( self, item ):
		start = time.time()
		if item.type == 'temp':
			# This is an intermediate file, use the cache
			db = self.citems
			log = self.clog
		else:
			# This is not intermediate data, don't use cache
			db = self.ditems
			log = self.dlog

		res = db.find_one( cbid=item.cbid )

		self.read_time += time.time()-start
		self.reads += 1
		return res


