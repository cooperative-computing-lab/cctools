# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, hashlib, time

from utils import *

ready = False
shutting_down = False


HOME = os.path.expanduser("~")
CWD = os.getcwd()

#base_dir = HOME+'/.prune/'
base_dir = '/data/pivie/prune_space6/'

data_file_directory = base_dir+'data/files/'
data_db_pathname = base_dir+'data/_prune.db'
data_log_pathname = base_dir+'logs/data.log'

cache_file_directory = base_dir+'cache/files/'
cache_db_pathname = base_dir+'cache/_prune.db'
cache_log_pathname = base_dir+'logs/cache.log'

cache_quota = 1024*1024 # only considers the cache
total_quota = 1024*1024 # considers data and cache

work_db_pathname = base_dir+'_work.db'
work_log_pathname = base_dir+'logs/work.log'

sandbox_directory = base_dir+'sandboxes/'
tmp_file_directory = base_dir+'tmp/'

repository_id = uuid()
workflow_id = None
workflow_step = None


log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


# Server settings
server_log_pathname = base_dir+'logs/server.log'
hostname = '127.0.0.1'
port = 8073

# Worker settings
worker_log_pathname = base_dir+'logs/worker.log'
exec_local_concurrency = 16


timer_log = base_dir+'logs/timing.log'


wq_port = 0
#wq_name = 'prune_'+uuid()
wq_name = 'prune_census'
wq_log_pathname = base_dir+'logs/wq.log'
wq_stage = None

cctools_version = 'CCTOOLS_VERSION'
cctools_releasedate = 'CCTOOLS_RELEASE_DATE'


'''
ready = {}
def get_ready_handle( name, sleep_time=1.0 ):
	while name not in ready:
		time.sleep( sleep_time )
	return ready[name]
def set_ready_handle( name, obj ):
	print '%s is ready!' % name
	ready[name] = obj

starts = {}
sums = {'exec_loop':0, 'socket_loop1':0, 'socket_loop2':0, 'socket_loop3':0, 'socket_loop4':0, 'exec_local_loop':0, 'exec_local_hash':0, 'exec_local_move':0}
'''


