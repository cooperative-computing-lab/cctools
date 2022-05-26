# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, hashlib, time, json

from utils import *

ready = False
shutting_down = False


HOME = os.path.expanduser("~")
CWD = os.getcwd()

base_dir = HOME+'/.prune/'

data_file_directory = base_dir+'data/files/'
data_db_pathname = base_dir+'data/_prune.db'
data_log_pathname = base_dir+'logs/data.log'

cache_file_directory = base_dir+'cache/files/'
cache_db_pathname = base_dir+'cache/_prune.db'
cache_log_pathname = base_dir+'logs/cache.log'

trash_file_directory = base_dir+'trash/files/'
trash_db_pathname = base_dir+'trash/_prune.db'
trash_log_pathname = base_dir+'logs/trash.log'

work_db_pathname = base_dir+'_work.db'
work_log_pathname = base_dir+'logs/work.log'

sandbox_directory = base_dir+'sandboxes/'
tmp_file_directory = base_dir+'tmp/'

# Server settings
#server_log_pathname = base_dir+'logs/server.log'
#hostname = '127.0.0.1'
#port = 8073

# Worker settings
worker_log_pathname = base_dir+'logs/worker.log'

timer_log = base_dir+'logs/timing.log'

wq_debug_log_pathname = base_dir+'logs/wq_debug.log'
wq_log_pathname = base_dir+'logs/wq.log'


def set_base_dir( new_base_dir ):
	global base_dir

	global data_file_directory, data_db_pathname, data_log_pathname
	if data_file_directory.startswith(base_dir):
		data_file_directory = new_base_dir+'data/files/'
		data_db_pathname = new_base_dir+'data/_prune.db'
		data_log_pathname = new_base_dir+'logs/data.log'

	global cache_file_directory, cache_db_pathname, cache_log_pathname
	if cache_file_directory.startswith(base_dir):
		cache_file_directory = new_base_dir+'cache/files/'
		cache_db_pathname = new_base_dir+'cache/_prune.db'
		cache_log_pathname = new_base_dir+'logs/cache.log'

	global trash_file_directory, trash_db_pathname, trash_log_pathname
	if trash_file_directory.startswith(base_dir):
		trash_file_directory = new_base_dir+'trash/files/'
		trash_db_pathname = new_base_dir+'trash/_prune.db'
		trash_log_pathname = new_base_dir+'logs/trash.log'

	global work_db_pathname, work_log_pathname
	if work_db_pathname.startswith(base_dir):
		work_db_pathname = new_base_dir+'_work.db'
		work_log_pathname = new_base_dir+'logs/work.log'

	global sandbox_directory, tmp_file_directory
	if sandbox_directory.startswith(base_dir):
		sandbox_directory = new_base_dir+'sandboxes/'
		tmp_file_directory = new_base_dir+'tmp/'

	global worker_log_pathname
	if worker_log_pathname.startswith(base_dir):
		worker_log_pathname = new_base_dir+'logs/worker.log'

	global timer_log
	if timer_log.startswith(base_dir):
		timer_log = new_base_dir+'logs/timing.log'

	global wq_debug_log_pathname, wq_log_pathname
	if wq_debug_log_pathname.startswith(base_dir):
		wq_debug_log_pathname = new_base_dir+'logs/wq_debug.log'
		wq_log_pathname = new_base_dir+'logs/wq.log'

	base_dir = new_base_dir


#total_quota = 30000000000000 # considers data and cache
total_quota = 100000000000 # considers data and cache
exec_local_concurrency = 16


repository_id = uuid()
workflow_id = None
workflow_step = None


wq_port = 0
#wq_name = 'prune_'+uuid()
wq_name = 'prune'
wq_stage = None


log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


cctools_version = 'CCTOOLS_VERSION'
cctools_releasedate = 'CCTOOLS_RELEASE_DATE'

def set_config_file(new_config_file):
	if os.path.isfile(new_config_file):
		with open(new_config_file) as f:
			json_str = f.read(1024*1024)
			body = json.loads(json_str)
			for key in body:
				val = body[key]
				if key=='base_dir':
					set_base_dir( val )
				elif key=='repository_id':
					global repository_id
					repository_id = val
				elif key=='total_quota':
					global total_quota
					total_quota = int(val)
				elif key=='wq_name':
					global wq_name
					wq_name = val
				elif key=='exec_local_concurrency':
					global exec_local_concurrency
					exec_local_concurrency = int(val)
				else:
					print 'Unknown config option:',key, val
	else:
		print 'File not found:',new_config_file


config_file = HOME+'/.prune/config'
if not os.path.isfile(config_file):
	with open(config_file,'w') as f:
		obj = {'base_dir':base_dir, 'repository_id':repository_id,
			'total_quota':total_quota, 'wq_name':wq_name,
			'exec_local_concurrency':exec_local_concurrency}
		f.write(json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ')))
else:
	set_config_file( config_file )


if False:
	obj = {'base_dir':base_dir, 'repository_id':repository_id,
		'total_quota':total_quota, 'wq_name':wq_name,
		'exec_local_concurrency':exec_local_concurrency}
	print json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
