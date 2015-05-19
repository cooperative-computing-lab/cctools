# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time
import string, random, hashlib
import subprocess
import sqlite3, uuid

sqlite3.register_converter('PUID', lambda b: uuid.UUID(b))
sqlite3.register_adapter(uuid.UUID, lambda u: str(u))

creates = []
meta_db = None
meta_data = None
db_pathname = None

read_cnt = write_cnt = 0

def truncate():
	global db_pathname
	subprocess.call('rm -rf '+db_pathname,shell=True)
	print 'Database truncated at:%s'%db_pathname
	initialize(db_pathname)


def initialize(new_db_pathname):
	global creates, meta_db, meta_data, db_pathname

	db_pathname = new_db_pathname

	db_folder = '/'.join(db_pathname.split('/')[0:-1])
	try: 
		os.makedirs(db_folder)
	except OSError:
		if not os.path.isdir(db_folder):
			raise

	meta_data = sqlite3.connect(db_pathname, detect_types=sqlite3.PARSE_DECLTYPES)
	def dict_factory(cursor, row):
	    d = {}
	    for idx, col in enumerate(cursor.description):
	        d[col[0]] = row[idx]
	    return d
	meta_data.row_factory = dict_factory
	meta_db = meta_data.cursor()
	meta_data.text_factory = str


	for create in creates:
		meta_db.execute(create)
	meta_data.commit()
	
	qs = run_get_by_queue('RunningLocally')
	for q in qs:
		run_upd(q['id'],'Run',None,'')

	qs = run_get_by_queue('Running')
	for q in qs:
		run_upd(q['id'],'Run',None,'')



def start_query_cnts():
	global read_cnt, write_cnt
	read_cnt = write_cnt = 0

def get_query_cnts():
	return read_cnt, write_cnt

#dtype is data type (text, list of uids, etc)
#stype is storage type (targz, gz, tar, p7zip)
creates.append('''CREATE TABLE IF NOT EXISTS copies (
												puid PUID,
												form TEXT,
												pack TEXT,
												chksum TEXT,
												length INT,
												store TEXT,
												created_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS copy_store ON copies(store);')
creates.append('CREATE INDEX IF NOT EXISTS copy_puid ON copies(puid);')
creates.append('CREATE INDEX IF NOT EXISTS copy_chksum ON copies(chksum);')

def copy_ins(puid, chksum, length, form='ascii', pack=None, store=None):
	global write_cnt
	write_cnt += 1
	#Always transfer the file before making an entry in the database (no partial files)
	now = time.time()
	ins = 'INSERT INTO copies (puid, chksum, length, form, pack, store, created_at) VALUES (?,?,?,?,?,?,?);'
	vals = (puid, chksum, length, form, pack, store, now)
	meta_db.execute(ins,vals)
	meta_data.commit()

def copies_get_by_chksum(chksum):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM copies WHERE chksum=? ORDER BY created_at;'
	meta_db.execute(sel,[chksum])
	return meta_db.fetchall()

def copies_get(puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM copies WHERE puid=? ORDER BY created_at;'
	meta_db.execute(sel,[puid])
	return meta_db.fetchall()





creates.append('''CREATE TABLE IF NOT EXISTS functions (
												file_puid PUID,
												in_types TEXT,
												out_names TEXT,
												defined_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS function_puid ON functions(file_puid);')

def function_ins(out_names, file_puid, in_types):
	global write_cnt
	write_cnt += 1
	now = time.time()
	ins = 'INSERT INTO functions (file_puid, in_types, out_names, defined_at) VALUES (?,?,?,?);'
	vals = (file_puid, in_types, out_names, now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return file_puid
	#return meta_db.lastrowid

def function_get(file_puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM functions WHERE file_puid=? ORDER BY defined_at DESC LIMIT 1;'
	meta_db.execute(sel,[file_puid])
	return meta_db.fetchone()





creates.append('''CREATE TABLE IF NOT EXISTS repos (
												id INTEGER PRIMARY KEY,
												puid PUID,
												name TEXT
											);''')
creates.append('CREATE UNIQUE INDEX IF NOT EXISTS repo_puid ON repos(puid);')
creates.append('CREATE UNIQUE INDEX IF NOT EXISTS repo_name ON repos(name);')

def repo_ins(puid, name):
	global write_cnt
	write_cnt += 1
	ins = 'INSERT INTO repos (puid, name) VALUES (?,?);'
	vals = (puid, name)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return meta_db.lastrowid

def repo_get(id):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM repos WHERE id=?;'
	meta_db.execute(sel,[id])
	return meta_db.fetchone()

def repo_get_by_puid(puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM repos WHERE puid=?;'
	meta_db.execute(sel,[puid])
	return meta_db.fetchone()

def repo_get_by_name(name):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM repos WHERE name=?;'
	meta_db.execute(sel,[name])
	return meta_db.fetchone()





creates.append('''CREATE TABLE IF NOT EXISTS ops (
												id INTEGER PRIMARY KEY,
												env_puid PUID,
												cmd_id INT,
												chksum TEXT,
												submitted_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS op_env_puid ON ops(env_puid);')
creates.append('CREATE INDEX IF NOT EXISTS op_chksum ON ops(chksum);')

def op_ins(env_puid, env_type, cmd_id=None, chksum=''):
	global write_cnt
	write_cnt += 1
	ins = 'INSERT INTO ops (env_puid, cmd_id, chksum) VALUES (?,?,?);'
	vals = (env_puid, cmd_id, chksum)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return meta_db.lastrowid

def op_get(id):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM ops WHERE id=?;'
	meta_db.execute(sel,[id])
	return meta_db.fetchone()

def op_get_by_chksum(chksum):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM ops WHERE chksum=?;'
	meta_db.execute(sel,[chksum])
	return meta_db.fetchone()





creates.append('''CREATE TABLE IF NOT EXISTS ios (
												op_id TEXT,
												file_puid PUID,
												name TEXT,
												literal TEXT,
												io_type TEXT,
												pos INT
											);''')
#io_types   #I=input, O=output, F=function
creates.append('CREATE INDEX IF NOT EXISTS io_op ON ios(op_id);')
creates.append('CREATE INDEX IF NOT EXISTS io_file_puid ON ios(file_puid);')

def io_ins(op_id, io_type, file_puid, name=None, literal=None, pos=0):
	global write_cnt
	write_cnt += 1
	ins = "INSERT INTO ios (op_id, file_puid, name, literal, io_type, pos) VALUES (?,?,?,?,?,?);"
	meta_db.execute(ins,[op_id, file_puid, name, literal, io_type, pos])
	meta_data.commit()

def ios_get_by_file_puid(puid, io_type=None):
	global read_cnt
	read_cnt += 1
	if io_type:
		sel = 'SELECT * FROM ios WHERE file_puid=? AND io_type=? ORDER BY op_id;'
		meta_db.execute(sel,[puid,io_type])
		return meta_db.fetchall()
	else:
		sel = 'SELECT * FROM ios WHERE file_puid=? ORDER BY op_id;'
		meta_db.execute(sel,[puid])
		return meta_db.fetchall()

def ios_get(op_id):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM ios WHERE op_id=? ORDER BY io_type,pos;'
	meta_db.execute(sel,[op_id])
	return meta_db.fetchall()





creates.append('''CREATE TABLE IF NOT EXISTS runs (
												id INTEGER PRIMARY KEY,
												op_id INT,
												resource TEXT,
												
												queue TEXT,
												updated_at REAL,

												status INT,
												notes TEXT,
												
												total_time REAL,
												sys_time REAL,
												user_time REAL,
												start_time REAL,
												completed_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS run_op_id ON runs(op_id);')
creates.append('CREATE INDEX IF NOT EXISTS run_status ON runs(status);')

def run_get_by_queue(queue, limit=50):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM runs WHERE queue=? ORDER BY updated_at LIMIT ?;'
	meta_db.execute(sel,[queue,limit])
	meta_data.commit()
	return meta_db.fetchall()

def run_queue_cnts():
	global read_cnt
	read_cnt += 1
	sel = 'SELECT queue,COUNT(*) AS cnt FROM runs WHERE 1 GROUP BY queue ORDER BY queue;'
	meta_db.execute(sel)
	meta_data.commit()
	return meta_db.fetchall()

def run_get_id_by_op_id(op_id):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM runs WHERE op_id=? ORDER BY id DESC;'
	meta_db.execute(sel,[op_id])
	res = meta_db.fetchall()
	if res and res.__len__()>0:
		return res[0]['id']
	else:
		return None

def run_get_by_task_id(task_id):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM runs WHERE status=? AND queue=? ORDER BY id DESC;'
	meta_db.execute(sel,[task_id,'Running'])
	return meta_db.fetchone()


def run_end(id, status=None, notes=''):
	global write_cnt
	write_cnt += 1
	now = time.time()
	upd = "UPDATE runs SET queue=?,updated_at=?,status=?,notes=?,total_time=?-start_time,sys_time=?,user_time=?,completed_at=? WHERE id=?;"
	vals = ('Complete', now, status, notes, now, 0, 0, now, id)
	meta_db.execute(upd,vals)
	meta_data.commit()
	return meta_db.fetchall()

def run_upd(id,queue, status=None, notes='', resource=''):
	global write_cnt
	write_cnt += 1
	now = time.time()
	start_time = None
	if queue.find('ing')>0:
		start_time = now
	upd = "UPDATE runs SET queue=?,updated_at=?,resource=?,status=?,notes=?,start_time=? WHERE id=?;"
	vals = (queue, now, resource, status, notes, start_time, id)
	meta_db.execute(upd,vals)
	meta_data.commit()
	return meta_db.fetchall()

def run_upd_by_op_id(op_id,queue, status=None, notes='', resource=''):
	global write_cnt
	write_cnt += 1
	now = time.time()
	start_time = None
	if queue.find('ing')>0:
		start_time = now
	upd = "UPDATE runs SET queue=?,updated_at=?,resource=?,status=?,notes=?,start_time=? WHERE op_id=?;"
	vals = (queue, now, resource, status, notes, start_time, op_id)
	meta_db.execute(upd,vals)
	meta_data.commit()
	return meta_db.fetchall()

def run_ins_transfer(op_id, copy_id):
	global write_cnt
	write_cnt += 1
	now = time.time()
	ins = 'INSERT INTO runs (op_id,copy_id,queue,update_time) VALUES (?,?,?,?);'
	vals = (op_id, copy_id, 'Transfer', now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return meta_db.lastrowid

def run_ins(op_id):
	global write_cnt
	write_cnt += 1
	now = time.time()
	ins = 'INSERT INTO runs (op_id,queue,updated_at) VALUES (?,?,?);'
	vals = (op_id, 'Run', now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return meta_db.lastrowid





creates.append('''CREATE TABLE IF NOT EXISTS vars (
												name TEXT,
												puid PUID,
												set_at REAL,
												gone_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS var_name ON vars(name);')
creates.append('CREATE INDEX IF NOT EXISTS var_puid ON vars(puid);')
creates.append('CREATE INDEX IF NOT EXISTS var_gone ON vars(gone_at);')

def var_get(name):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM vars WHERE name=? AND gone_at=2147389261 ORDER BY set_at DESC;'
	meta_db.execute(sel,[name])
	res = meta_db.fetchall()
	if res and res.__len__()>0:
		return res[0]['puid']
	return None

def var_set(name, puid):
	global write_cnt
	write_cnt += 1
	now = time.time()
	upd = 'REPLACE INTO vars (name,puid,set_at,gone_at) VALUES (?,?,?,?);'
	meta_db.execute(upd,[name,puid,now,2147389261])
	return meta_data.commit()

def var_getAll():
	global read_cnt
	read_cnt += 1
	now = time.time()
	sel = 'SELECT * FROM vars WHERE ?<gone_at ORDER BY set_at DESC;'
	meta_db.execute(sel,[now])
	res = meta_db.fetchall()
	return res

def var_unset(name):
	global write_cnt
	write_cnt += 1
	now = time.time()
	upd = 'UPDATE vars SET gone_at=? WHERE name=?;'
	meta_db.execute(upd,[now,name])
	return meta_data.commit()



creates.append('''CREATE TABLE IF NOT EXISTS cmds (
												id INTEGER PRIMARY KEY,
												command TEXT,
												submitted_at REAL
											);''')

def cmd_ins(command):
	global write_cnt
	write_cnt += 1
	if len(command)>0 and cmd_get_last()!=command:
		now = time.time()
		ins = 'INSERT INTO cmds (command, submitted_at) VALUES (?,?);'
		meta_db.execute(ins,[command, now])
		meta_data.commit()
		return meta_db.lastrowid

def cmd_get_last():
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM cmds WHERE 1 ORDER BY id DESC LIMIT 1;'
	meta_db.execute(sel)
	res = meta_db.fetchone()
	if res:
		return res['command']
	return None

def cmds_get():
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM cmds WHERE 1 ORDER BY id;'
	meta_db.execute(sel)
	return meta_db.fetchall()











def hashfile(fname, hasher=hashlib.sha1(), blocksize=65536):
    afile = open(fname, 'rb')
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()







