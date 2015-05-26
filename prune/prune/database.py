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
		run_upd(q['puid'],'Run',None,'')

	qs = run_get_by_queue('Running')
	for q in qs:
		run_upd(q['puid'],'Run',None,'')



def start_query_cnts():
	global read_cnt, write_cnt
	read_cnt = write_cnt = 0

def get_query_cnts():
	return read_cnt, write_cnt



#form is data type (text, list of uids, etc)
creates.append('''CREATE TABLE IF NOT EXISTS files (
												puid PUID PRIMARY KEY,
												form TEXT,
												chksum TEXT,
												size INT,
												at REAL
											);''')

def file_ins(chksum=None, size=-1):
	global write_cnt
	write_cnt += 1
	now = time.time()
	puid = uuid.uuid4()
	ins = 'INSERT INTO files (puid, form, chksum, size, at) VALUES (?,?,?,?,?);'
	vals = (puid, form, chksum, size, now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return puid

def file_get_by_chksum(chksum):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM files WHERE chksum=?;'
	meta_db.execute(sel,[chksum])
	return meta_db.fetchone()

def file_get(puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM files WHERE puid=?;'
	meta_db.execute(sel,[puid])
	return meta_db.fetchone()



#pack is storage type (targz, gz, tar, p7zip)
creates.append('''CREATE TABLE IF NOT EXISTS copies (
												puid PUID,
												pack TEXT,
												chksum TEXT,
												length INT,
												store TEXT,
												at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS copy_puid ON copies(puid);')
creates.append('CREATE INDEX IF NOT EXISTS copy_chksum ON copies(chksum);')
creates.append('CREATE INDEX IF NOT EXISTS copy_store ON copies(store);')

def copy_ins(puid, chksum, length, pack=None, store=None):
	global write_cnt
	write_cnt += 1
	#Always transfer the file before making an entry in the database (no partial files)
	now = time.time()
	ins = 'INSERT INTO copies (puid, chksum, length, pack, store, at) VALUES (?,?,?,?,?,?);'
	vals = (puid, chksum, length, pack, store, now)
	meta_db.execute(ins,vals)
	meta_data.commit()

def copies_get_by_chksum(chksum):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM copies WHERE chksum=? ORDER BY at;'
	meta_db.execute(sel,[chksum])
	return meta_db.fetchall()

def copies_get(puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM copies WHERE puid=? ORDER BY at;'
	meta_db.execute(sel,[puid])
	return meta_db.fetchall()





creates.append('''CREATE TABLE IF NOT EXISTS functions (
												puid PUID PRIMARY KEY,
												file_puid PUID,
												in_types TEXT,
												out_names TEXT,
												out_types TEXT,
												at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS function_puid ON functions(puid);')

def function_ins(file_puid, in_types, out_names, out_types=''):
	global write_cnt
	write_cnt += 1
	now = time.time()
	puid = uuid.uuid4()
	ins = 'INSERT INTO functions (puid, file_puid, in_types, out_names, out_types, at) VALUES (?,?,?,?,?,?);'
	vals = (puid, file_puid, in_types, out_names, out_types, now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return puid

def function_get(puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM functions WHERE puid=? ORDER BY at DESC;'
	meta_db.execute(sel,[puid])
	return meta_db.fetchone()

def function_get_by_file(file_puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM functions WHERE file_puid=? ORDER BY at DESC;'
	meta_db.execute(sel,[file_puid])
	return meta_db.fetchone()




creates.append('''CREATE TABLE IF NOT EXISTS environments (
												puid PUID PRIMARY KEY,
												file_puid PUID,
												env_type TEXT,
												at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS environment_puid ON environments(puid);')

def environment_ins(file_puid, env_type):
	global write_cnt
	write_cnt += 1
	now = time.time()
	puid = uuid.uuid4()
	ins = 'INSERT INTO environments (puid, file_puid, env_type, at) VALUES (?,?,?,?);'
	vals = (puid, file_puid, env_type, now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return puid

def environment_get(puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM environments WHERE puid=? ORDER BY at DESC;'
	meta_db.execute(sel,[puid])
	return meta_db.fetchone()






creates.append('''CREATE TABLE IF NOT EXISTS ops (
												puid PUID PRIMARY KEY,
												function_puid PUID,
												env_puid PUID,
												chksum TEXT,
												env_chksum TEXT,
												at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS op_chksum ON ops(chksum);')
creates.append('CREATE INDEX IF NOT EXISTS op_env_chksum ON ops(env_chksum);')

def op_ins(function_puid, chksum='', env_chksum='', env_puid=None):
	global write_cnt
	write_cnt += 1
	now = time.time()
	puid = uuid.uuid4()
	ins = 'INSERT INTO ops (puid, function_puid, env_puid, chksum, env_chksum, at) VALUES (?,?,?,?,?,?);'
	meta_db.execute(ins,[puid,function_puid,env_puid,chksum,env_chksum,now])
	meta_data.commit()
	return puid

def op_get(puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM ops WHERE puid=?;'
	meta_db.execute(sel,[puid])
	return meta_db.fetchone()

def op_get_by_chksum(chksum):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM ops WHERE chksum=?;'
	meta_db.execute(sel,[chksum])
	return meta_db.fetchone()

def op_get_by_env_chksum(env_chksum):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM ops WHERE env_chksum=?;'
	meta_db.execute(sel,[env_chksum])
	return meta_db.fetchone()




creates.append('''CREATE TABLE IF NOT EXISTS ios (
												op_puid PUID,
												file_puid PUID,
												display TEXT,
												io_type TEXT,
												pos INT
											);''')
#io_types   #I=input, O=output, F=function
creates.append('CREATE INDEX IF NOT EXISTS io_op ON ios(op_puid);')
creates.append('CREATE INDEX IF NOT EXISTS io_file_puid ON ios(file_puid);')

def io_ins(op_puid, io_type, file_puid=None, display='', pos=0):
	global write_cnt
	write_cnt += 1
	ins = "INSERT INTO ios (op_puid, file_puid, display, io_type, pos) VALUES (?,?,?,?,?);"
	meta_db.execute(ins,[op_puid, file_puid, display, io_type, pos])
	meta_data.commit()

def ios_get_by_file_puid(file_puid, io_type=None):
	global read_cnt
	read_cnt += 1
	if io_type:
		sel = 'SELECT * FROM ios WHERE file_puid=? AND io_type=? ORDER BY op_puid, io_type, pos;'
		meta_db.execute(sel,[file_puid,io_type])
		return meta_db.fetchall()
	else:
		sel = 'SELECT * FROM ios WHERE file_puid=? ORDER BY op_puid, io_type, pos;'
		meta_db.execute(sel,[file_puid])
		return meta_db.fetchall()

def ios_get(op_puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM ios WHERE op_puid=? ORDER BY io_type, pos;'
	meta_db.execute(sel,[op_puid])
	return meta_db.fetchall()





creates.append('''CREATE TABLE IF NOT EXISTS runs (
												puid PUID PRIMARY KEY,
												op_puid PUID,
												resource TEXT,
												
												queue TEXT,
												updated_at REAL,

												status INT,
												notes TEXT,
												
												cpu_time REAL,
												disk_space INT,
												
												start_time REAL,
												completed_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS run_op_puid ON runs(op_puid);')
creates.append('CREATE INDEX IF NOT EXISTS run_status ON runs(status);')

def run_ins(op_puid):
	global write_cnt
	write_cnt += 1
	now = time.time()
	puid = uuid.uuid4()
	ins = 'INSERT INTO runs (puid,op_puid,queue,updated_at) VALUES (?,?,?,?);'
	meta_db.execute(ins,[puid, op_puid, 'Run', now])
	meta_data.commit()
	return puid

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

def run_get_puid_by_op_puid(op_puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM runs WHERE op_puid=? ORDER BY puid DESC;'
	meta_db.execute(sel,[op_puid])
	res = meta_db.fetchall()
	if res and res.__len__()>0:
		return res[0]['puid']
	else:
		return None

def run_upd(puid, queue, status=None, notes='', resource=''):
	global write_cnt
	write_cnt += 1
	now = time.time()
	start_time = None
	if queue.find('ing')>0:
		start_time = now
	upd = "UPDATE runs SET queue=?,updated_at=?,resource=?,status=?,notes=?,start_time=? WHERE puid=?;"
	meta_db.execute(upd,[queue, now, resource, status, notes, start_time, puid])
	meta_data.commit()
	return meta_db.fetchall()

def run_upd_by_op_puid(op_puid, queue, status=None, notes='', resource=''):
	global write_cnt
	write_cnt += 1
	now = time.time()
	start_time = None
	if queue.find('ing')>0:
		start_time = now
	upd = "UPDATE runs SET queue=?,updated_at=?,resource=?,status=?,notes=?,start_time=? WHERE op_puid=?;"
	meta_db.execute(upd,[queue, now, resource, status, notes, start_time, op_puid])
	meta_data.commit()
	return meta_db.fetchall()

def run_get_by_task_id(task_id):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM runs WHERE status=? AND queue=? ORDER BY puid DESC;'
	meta_db.execute(sel,[task_id,'Running'])
	return meta_db.fetchone()

'''


def run_end(id, status=None, notes=''):
	global write_cnt
	write_cnt += 1
	now = time.time()
	upd = "UPDATE runs SET queue=?,updated_at=?,status=?,notes=?,total_time=?-start_time,sys_time=?,user_time=?,completed_at=? WHERE id=?;"
	vals = ('Complete', now, status, notes, now, 0, 0, now, id)
	meta_db.execute(upd,vals)
	meta_data.commit()
	return meta_db.fetchall()

'''









creates.append('''CREATE TABLE IF NOT EXISTS cmds (
												puid PUID PRIMARY KEY,
												command TEXT,
												submitted_at REAL
											);''')

def cmd_ins(command):
	global write_cnt
	write_cnt += 1
	if len(command)>0 and cmd_get_last()!=command:
		now = time.time()
		puid = uuid.uuid4()
		ins = 'INSERT INTO cmds (puid,command, submitted_at) VALUES (?,?,?);'
		meta_db.execute(ins,[puid, command, now])
		meta_data.commit()
		return meta_db.lastrowid

def cmd_get_last():
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM cmds WHERE 1 ORDER BY puid DESC LIMIT 1;'
	meta_db.execute(sel)
	res = meta_db.fetchone()
	if res:
		return res['command']
	return None

def cmds_get():
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM cmds WHERE 1 ORDER BY puid;'
	meta_db.execute(sel)
	return meta_db.fetchall()





creates.append('''CREATE TABLE IF NOT EXISTS calls (
												cmd_puid PUID,
												op_puid PUID
											);''')
creates.append('CREATE INDEX IF NOT EXISTS call_cmd_puid ON calls(cmd_puid);')
creates.append('CREATE INDEX IF NOT EXISTS call_op_puid ON calls(op_puid);')

def call_ins(cmd_puid, op_puid):
	global write_cnt
	write_cnt += 1
	ins = 'INSERT INTO calls (cmd_puid, op_puid) VALUES (?,?);'
	meta_db.execute(ins,[cmd_puid, op_puid])
	meta_data.commit()


def calls_get_by_cmd(cmd_puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM calls WHERE cmd_puid=? ORDER BY op_puid;'
	meta_db.execute(sel,[cmd_puid])
	return meta_db.fetchall()

def calls_get_by_op(op_puid):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM calls WHERE op_puid=? ORDER BY cmd_puid;'
	meta_db.execute(sel,[op_puid])
	return meta_db.fetchall()






creates.append('''CREATE TABLE IF NOT EXISTS tags (
												name TEXT,
												puid PUID,
												set_at REAL,
												gone_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS tag_name ON tags(name);')
creates.append('CREATE INDEX IF NOT EXISTS tag_puid ON tags(puid);')
creates.append('CREATE INDEX IF NOT EXISTS tag_gone ON tags(gone_at);')

def tag_set(name, puid):
	tag_unset(name)
	global write_cnt
	write_cnt += 1
	now = time.time()
	upd = 'INSERT INTO tags (name,puid,set_at,gone_at) VALUES (?,?,?,?);'
	meta_db.execute(upd,[name,puid,now,2147389261])
	return meta_data.commit()

def tag_add(name, puid):
	global write_cnt
	write_cnt += 1
	now = time.time()
	upd = 'INSERT INTO tags (name,puid,set_at,gone_at) VALUES (?,?,?,?);'
	meta_db.execute(upd,[name,puid,now,2147389261])
	return meta_data.commit()

def tag_get(name):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM tags WHERE name=? AND gone_at=2147389261 ORDER BY set_at DESC LIMIT 1;'
	meta_db.execute(sel,[name])
	return meta_db.fetchone()

def tags_get(name):
	global read_cnt
	read_cnt += 1
	sel = 'SELECT * FROM tags WHERE name=? AND gone_at=2147389261 ORDER BY set_at DESC;'
	meta_db.execute(sel,[name])
	return meta_db.fetchall()

def tag_unset(name):
	global write_cnt
	write_cnt += 1
	now = time.time()
	upd = 'UPDATE tags SET gone_at=? WHERE name=?;'
	meta_db.execute(upd,[now,name])
	return meta_data.commit()

def tag_unsetOne(name,puid):
	global write_cnt
	write_cnt += 1
	now = time.time()
	upd = 'UPDATE tags SET gone_at=? WHERE name=? AND puid=?;'
	meta_db.execute(upd,[now,name,puid])
	return meta_data.commit()

def tag_getAll():
	global read_cnt
	read_cnt += 1
	now = time.time()
	sel = 'SELECT * FROM tags WHERE ?<gone_at ORDER BY set_at DESC;'
	meta_db.execute(sel,[now])
	res = meta_db.fetchall()
	return res












def hashfile(fname, hasher=hashlib.sha1(), blocksize=65536):
    afile = open(fname, 'rb')
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()







