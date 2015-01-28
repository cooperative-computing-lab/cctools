#!/usr/bin/env python2.6

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time
import string, random, hashlib
import subprocess, sqlite3

HOME = os.path.expanduser("~")
CWD = os.getcwd()

db_path = '/tmp/prune.db'

def peek(*arguments):
	line = ''
	for arg in arguments:
		line += ' '+str(arg)
	sys.stdout.write(line[1:])
	sys.stdout.flush()

creates = []
meta_db = None

def database_truncate():
	subprocess.call('rm -rf '+db_path,shell=True)
	print 'Database truncated'





creates.append('''CREATE TABLE IF NOT EXISTS files (
												id INTEGER PRIMARY KEY,
												chksum TEXT,
												length INT,
												created_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS file_chksum ON files(chksum);')

def file_upd(file_id,chksum,length):
	upd = 'UPDATE files SET chksum=?, length=? WHERE id=?'
	vals = (chksum,length,file_id)
	return meta_db.execute(upd,vals)

def file_newids(cnt=0):
	now = time.time()
	ids = []
	ins = 'INSERT INTO files (id,created_at) VALUES (?,?);'
	vals = (None,now)
	while cnt>0:
		meta_db.execute(ins,vals)
		meta_data.commit()
		ids.append( meta_db.lastrowid )
		cnt -= 1
	return ids

def file_ins(src_filename, chksum=None):
	now = time.time()
	file_size = os.stat(src_filename).st_size
	if not chksum:
		chksum = hashfile(src_filename)
	ins = 'INSERT INTO files (chksum, length, created_at) VALUES (?,?,?);'
	vals = (chksum, file_size, now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return meta_db.lastrowid

def files_get_by_chksum(chksum):
	sel = 'SELECT * FROM files WHERE chksum=? ORDER BY id;'
	meta_db.execute(sel,[chksum])
	return meta_db.fetchall()

def file_get(file_id):
	sel = 'SELECT * FROM files WHERE id=?;'
	meta_db.execute(sel,[file_id])
	return meta_db.fetchone()

def file_get_id_by_chksum(chksum):
	res = files_get_by_chksum(chksum)
	if res:
		return res[-1]['id']
	else:
		return None



creates.append('''CREATE TABLE IF NOT EXISTS copies (
												id INT,
												file_id INT,
												location TEXT,
												filename TEXT,
												managed TEXT,
												created_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS copy_id ON copies(id);')

def copy_ins(file_id, location, filename, managed=True):
	now = time.time()
	ins = 'INSERT INTO copies (file_id, location, filename, managed, created_at) VALUES (?,?,?,?,?);'
	vals = (file_id, location, filename, managed, now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	copy_id = meta_db.lastrowid

	file_size = os.stat(filename).st_size
	chksum = hashfile(filename)
	file_upd(file_id,chksum,file_size)

	return copy_id


def copy_get(file_id):
	sel = 'SELECT * FROM copies WHERE file_id=? ORDER BY created_at DESC;'
	meta_db.execute(sel,[file_id])
	return meta_db.fetchall()




creates.append('''CREATE TABLE IF NOT EXISTS functions (
												id INT,
												in_types TEXT,
												out_names TEXT
											);''')
creates.append('CREATE INDEX IF NOT EXISTS function_id ON functions(id);')

def function_ins(out_names, file_id, in_types):
	ins = 'INSERT INTO functions (id, in_types, out_names) VALUES (?,?,?);'
	vals = (file_id, in_types, out_names)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return file_id
	#return meta_db.lastrowid

def function_get(file_id):
	sel = 'SELECT * FROM functions WHERE id=?;'
	meta_db.execute(sel,[file_id])
	return meta_db.fetchone()




creates.append('''CREATE TABLE IF NOT EXISTS ops (
												id INTEGER PRIMARY KEY,
												env_id INT,
												func_id INT,
												in_str TEXT,
												out_str TEXT,
												submitted_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS op_env_id ON ops(env_id);')
creates.append('CREATE INDEX IF NOT EXISTS op_func_id ON ops(func_id);')

creates.append('''CREATE TABLE IF NOT EXISTS ios (
												op_id TEXT,
												file_id INT,
												output TEXT,
												pos INT
											);''')  # pos = 1=argument1, -1=outputn-1
creates.append('CREATE INDEX IF NOT EXISTS io_op ON ios(op_id);')
creates.append('CREATE INDEX IF NOT EXISTS io_file_id ON ios(file_id);')

def op_ins(out_cnt, func_id, input_args, input_type_str):
	output_ids = file_newids( out_cnt )
	out_str = ', '.join(str(x) for x in output_ids)

	input_types = input_type_str.split(' ')
		
	env_id = var_get('_ENV')

	now = time.time()
	ins = 'INSERT INTO ops (env_id, func_id, in_str, out_str, submitted_at) VALUES (?,?,?,?,?);'
	meta_db.execute(ins,[env_id, func_id, ' '.join(input_args), out_str, now])
	meta_data.commit()
	op_id = meta_db.lastrowid
	
	ins = "INSERT INTO ios (op_id, file_id, output, pos) VALUES (?,?,?,?);"
	for i, file_id in enumerate(output_ids):
		meta_db.execute(ins,[op_id,file_id,'Y',(-out_cnt+i)])
		meta_data.commit()
	if func_id:
		meta_db.execute(ins,[op_id,func_id,'N',0])
	for i, arg in enumerate(input_args):
		if input_types[i].lower()=='file':
			meta_db.execute(ins,[op_id,arg,'N',i+1])
	meta_data.commit()
	return op_id,output_ids

def ops_get(op_id):
	sel = 'SELECT * FROM ops WHERE id=?;'
	meta_db.execute(sel,[op_id])
	return meta_db.fetchone()

def ios_get(op_id):
	sel = 'SELECT * FROM ios WHERE op_id=? ORDER BY pos;'
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

def run_get_by_queue(queue,limit=50):
	sel = 'SELECT * FROM runs WHERE queue=? ORDER BY updated_at LIMIT ?;'
	meta_db.execute(sel,[queue,limit])
	meta_data.commit()
	return meta_db.fetchall()

def run_queue_cnts():
	sel = 'SELECT queue,COUNT(*) AS cnt FROM runs WHERE 1 GROUP BY queue ORDER BY queue;'
	meta_db.execute(sel)
	meta_data.commit()
	return meta_db.fetchall()

def run_get_id_by_op_id(op_id):
	sel = 'SELECT * FROM runs WHERE op_id=? ORDER BY id DESC;'
	meta_db.execute(sel,[op_id])
	res = meta_db.fetchall()
	if res and res.__len__()>0:
		return res[0]['id']
	else:
		return None

def run_get_by_task_id(task_id):
	sel = 'SELECT * FROM runs WHERE status=? AND queue=? ORDER BY id DESC;'
	meta_db.execute(sel,[task_id,'Running'])
	return meta_db.fetchone()


def run_end(id,total_time,status=None,notes=''):
	now = time.time()
	upd = "UPDATE runs SET queue=?,updated_at=?,status=?,notes=?,total_time=?,sys_time=?,user_time=?,completed_at=? WHERE id=?;"
	vals = (queue, now, status, notes, id)
	meta_db.execute(upd,vals)
	meta_data.commit()
	return meta_db.fetchall()

def run_upd(id,queue,status=None,notes=''):
	now = time.time()
	start_time = None
	if queue.find('ing')>0:
		start_time = now
	upd = "UPDATE runs SET queue=?,updated_at=?,status=?,notes=?,start_time=? WHERE id=?;"
	vals = (queue, now, status, notes, start_time, id)
	meta_db.execute(upd,vals)
	meta_data.commit()
	return meta_db.fetchall()

def run_ins_transfer(op_id,copy_id):
	now = time.time()
	ins = 'INSERT INTO runs (op_id,copy_id,queue,update_time) VALUES (?,?,?,?);'
	vals = (op_id, copy_id, 'Transfer', now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return meta_db.lastrowid

def run_ins(op_id):
	now = time.time()
	ins = 'INSERT INTO runs (op_id,queue,updated_at) VALUES (?,?,?);'
	vals = (op_id, 'Run', now)
	meta_db.execute(ins,vals)
	meta_data.commit()
	return meta_db.lastrowid





creates.append('''CREATE TABLE IF NOT EXISTS vars (
												key TEXT,
												value TEXT,
												set_at REAL
											);''')
creates.append('CREATE INDEX IF NOT EXISTS var_key ON vars(key);')

def var_get(key):
	sel = 'SELECT * FROM vars WHERE key=? ORDER BY set_at DESC;'
	meta_db.execute(sel,[key])
	res = meta_db.fetchall()
	if res and res.__len__()>0:
		return res[0]['value']
	else:
		return None

def var_set(key,value):
	now = time.time()
	upd = 'REPLACE INTO vars (key,value,set_at) VALUES (?,?,?);'
	meta_db.execute(upd,[key,value,now])
	return meta_data.commit()
	#return meta_db.lastrowid




creates.append('''CREATE TABLE IF NOT EXISTS cmds (
												id INTEGER PRIMARY KEY,
												original TEXT,
												op_id TEXT,
												final TEXT,
												submitted_at REAL
											);''')

def cmd_ins(original,final,op_id=None):
	now = time.time()
	ins = 'INSERT INTO cmds (original, op_id, final, submitted_at) VALUES (?,?,?,?);'
	meta_db.execute(ins,[original, op_id, final, now])
	meta_data.commit()












def hashfile(fname, hasher=hashlib.sha1(), blocksize=65536):
    afile = open(fname, 'rb')
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()


def init():
	global creates, meta_db, meta_data

	meta_data = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
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





