# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import os, sys, time, traceback
import subprocess, shutil
import StringIO

import glob
import timer
from class_item import Item
from utils import *


class Worker:
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	def __init__( self, call ):
		self.call = call
		self.process = None

		timer.start('work.stage_in')
		self.start = time.time()


		self.sbid = uuid()
		self.sandbox = glob.sandbox_directory+self.sbid+'/'
		self.env_dbg_log = 'ENV_DBG.log'
		self.task_dbg_log = 'TASK_DBG.log'
		self.cmd_dbg_log = 'CMD_DBG.log'
		call_body = self.call.body


		self.all_inputs = []
		self.all_outputs = ['ENV_DBG.log','TASK_DBG.log','CMD_DBG.log']

		# Prep universal sandbox

		os.makedirs( self.sandbox, 0755 );

		for fi, arg in enumerate(call_body['args']):
			param = call_body['params'][fi]
			it = glob.db.find_one( arg )
			directory = os.path.dirname(self.sandbox + param)
			try:
			    os.stat(directory)
			except:
			    os.mkdir(directory)

			if it.path:
				if it.type == 'temp':
					os.symlink( glob.cache_file_directory+it.path, self.sandbox + param )
				else:
					os.symlink( glob.data_file_directory+it.path, self.sandbox + param )
			else:
				with open( self.sandbox + param, 'w' ) as f:
					it.stream_content( f )

			self.all_inputs.append(param)





		with open('%suser_cmd.sh' % (self.sandbox), 'w+') as f:
			f.write( "#!/bin/sh\n\n" )
			f.write( "echo '' > %s\n\n" % (self.cmd_dbg_log) )
			f.write( "find | sort > prune_cmd_files_start.log\n\n")
			f.write( "echo \"----prune_cmd_start|`date '+%%s'`\" >> %s\n\n" % (self.cmd_dbg_log) )
			f.write( call_body['cmd']+"\n\n" )
			f.write( 'RETCODE=$?\n\n' )
			f.write( 'echo $RETCODE > exit.log\n\n' )
			f.write( "echo \"----prune_cmd_end|`date '+%%s'`\" >> %s\n\n" % (self.cmd_dbg_log) )
			f.write( "find | sort > prune_cmd_files_end.log\n\n")
			f.write( 'echo \"CMD return code:${RETCODE}\" >> %s\n\n' % (self.cmd_dbg_log) )
			f.write( 'exit $RETCODE\n\n' )

		self.all_outputs.append('prune_cmd_files_start.log')
		self.all_outputs.append('prune_cmd_files_end.log')
		self.all_outputs.append('exit.log')


		self.all_inputs.append('user_cmd.sh')



		# Create task sandbox

		before_cmds = ["echo '' > %s\n\n" % (self.task_dbg_log)]
		after_cmds = []

		for fn in call_body['params']:
			before_cmds.append("echo \"----prune_task_arg|%s|`du -bL %s | awk -F\"\t\" '{if ($1) print $1}'`\" >> %s" % (fn,fn,self.task_dbg_log))

		for fn in call_body['returns']:
			after_cmds.append("echo \"----prune_task_return|%s|`du -bL %s | awk -F\"\t\" '{if ($1) print $1}'`|`sha1sum %s | awk '{if ($1) print $1}'`\" >> %s" % (fn,fn,fn,self.task_dbg_log))
			self.all_outputs.append(fn)



		# before_cmds.append("find | sort > prune_task_files_start.log")
		# after_cmds.insert(0,"find | sort > prune_task_files_end.log")
		#after_cmds.append("mv `diff -u prune_task_files_start.log prune_task_files_end.log | grep -E \"^\+[^\+]\"| sed 's/^.//'` final_prune_task_output/ 2>/dev/null")

		before_cmds.append("echo 'pwd:'`pwd` >> %s" % (self.task_dbg_log))
		after_cmds.insert(0,"echo 'pwd:'`pwd` >> %s" % (self.task_dbg_log))

		# before_cmds.append("find | sort >> %s" % (self.task_dbg_log))
		# after_cmds.insert(0,"find | sort >> %s" % (self.task_dbg_log))
		before_cmds.append("ls -la >> %s" % (self.env_dbg_log))
		after_cmds.insert(0,"ls -la >> %s" % (self.env_dbg_log))

		# before_cmds.append("more user_cmd.sh >> %s" % (self.task_dbg_log))
		# after_cmds.insert(0,"more user_cmd.sh >> %s" % (self.task_dbg_log))

		before_cmds.append("echo \"----prune_task_start|`date '+%%s'`\" >> %s" % (self.task_dbg_log))
		after_cmds.insert(0,"echo \"----prune_task_end|`date '+%%s'`\" >> %s" % (self.task_dbg_log))

		before_cmds.append("chmod 755 env.sh")
		after_cmds.insert(0,"echo \"ENV return code:${RETCODE}\" >> %s" % (self.task_dbg_log))
		after_cmds.insert(0,"RETCODE=$?")
		after_cmds.insert(0,"./env.sh $1 >> %s 2>&1" % (self.task_dbg_log))



		before_cmds.insert(0,"rm -f *.log 2>/dev/null")
		for fn in call_body['returns']:
			before_cmds.insert(0,"rm -f %s 2>/dev/null" % (fn))

		after_cmds.append("exit $RETCODE")

		with open('%stask.sh' % (self.sandbox), 'w+') as f:
			f.write( "#!/bin/sh\n\n" )
			for cmd in before_cmds:
				f.write(cmd+"\n")
			f.write("\n")
			for cmd in after_cmds:
				f.write(cmd+"\n")

		self.all_inputs.append('task.sh')




		# Put environment in sandbox


		env_key = call_body['env']
		env = self.env = None
		if env_key != self.nil:
			self.env = env = glob.db.find_one( env_key )


		before_cmds = ["echo '' > %s" % (self.env_dbg_log)]
		after_cmds = []

		# before_cmds.append("find | sort >> %s" % (self.env_dbg_log))
		# after_cmds.insert(0,"find | sort >> %s" % (self.env_dbg_log))

		before_cmds.append("ls -la >> %s" % (self.env_dbg_log))
		after_cmds.insert(0,"ls -la >> %s" % (self.env_dbg_log))

		# before_cmds.append("echo 'pwd:'`pwd` >> %s" % (self.env_dbg_log))

		# before_cmds.append("more user_cmd.sh >> %s" % (self.env_dbg_log))
		# after_cmds.insert(0,"more user_cmd.sh >> %s" % (self.env_dbg_log))

		before_cmds.append("echo \"----prune_env_start|`date '+%%s'`\" >> %s" % (self.env_dbg_log))
		after_cmds.insert(0,"echo \"----prune_env_end|`date '+%%s'`\" >> %s" % (self.env_dbg_log))



		if not env or env_key == self.nil:
			self.env_names = ['prune.nil']

			before_cmds.append("chmod 755 user_cmd.sh")
			after_cmds.insert(0,"RETCODE=$((`cat exit.log`+$?))")
			after_cmds.insert(0,"./user_cmd.sh >> %s 2>&1" % (self.env_dbg_log))

		else:
			for i,arg in enumerate(env.body['args']):
				param = env.body['params'][i]
				it = glob.db.find_one( arg )
				directory = os.path.dirname(self.sandbox + param)
				try:
				    os.stat(directory)
				except:
				    os.mkdir(directory)

				if it.path:
					if it.type == 'temp':
						os.symlink( glob.cache_file_directory+it.path, self.sandbox + param )
					else:
						os.symlink( glob.data_file_directory+it.path, self.sandbox + param )
				else:
					with open( self.sandbox + param, 'w' ) as f:
						it.stream_content( f )

				self.all_inputs.append(param)



			if env.body['engine'] == 'wrapper':
				self.env_names = ['wrapper']
				before_cmds.append(env.body['open'])
				after_cmds.insert(0,env.body['close'])

				before_cmds.append("chmod 755 user_cmd.sh")
				after_cmds.insert(0,"RETCODE=$?")
				after_cmds.insert(0,"./user_cmd.sh >> %s 2>&1" % (self.env_dbg_log))


			elif env.body['engine'] == 'umbrella':
				self.env_names = ['umbrella']
				self.virtual_folder = '/tmp/'

				self.all_outputs.append('umbrella.log')
				self.all_outputs.append('umbrella_files.log')




				umbrella_file = which('umbrella')
				filename = self.sandbox + 'UMBRELLA_EXECUTABLE'
				if not os.path.isfile( filename ):
					shutil.copy( umbrella_file[0], filename )
				self.all_inputs.append('UMBRELLA_EXECUTABLE')


				umbrella_spec = 'SPEC.umbrella'

				arg_str = '  %stask.sh=%stask.sh' % (self.virtual_folder, self.sandbox)
				arg_str += ', %suser_cmd.sh=%suser_cmd.sh' % (self.virtual_folder, self.sandbox)

				arg_str2 = '  %stask.sh=${MYWD}/task.sh' % (self.virtual_folder)
				arg_str2 += ', %suser_cmd.sh=${MYWD}/user_cmd.sh' % (self.virtual_folder)

				for i,param in enumerate(env.body['params']):
					realpath = os.path.realpath(self.sandbox+param)
					arg_str += ', %s=%s' % (self.virtual_folder+param, realpath)
					arg_str2 += ', %s=${MYWD}/%s' % (self.virtual_folder+param, param)
				for i,param in enumerate(call_body['params']):
					realpath = os.path.realpath(self.sandbox+param)
					arg_str += ', %s=%s' % (self.virtual_folder+param, realpath)
					arg_str2 += ', %s=${MYWD}/%s' % (self.virtual_folder+param, param)


				if 'http_proxy' in env.body:
					before_cmds.append( "\nexport HTTP_PROXY=%s" % (env.body['http_proxy']) )
					before_cmds.append( "export http_proxy=%s\n" % (env.body['http_proxy']) )

				before_cmds.append( "rm -rf /tmp/prune_umbrella/%s 2>/dev/null\n" % (self.sbid))
				# before_cmds.append( "rm -rf /tmp/prune_umbrella 2>/dev/null\n" ) # Attempt to resolve an error
				before_cmds.append( "export MYWD=\"`pwd`\"\n" )

				before_cmds.append( "echo \"MYWD=${MYWD}\"\n" )

				before_cmds.append( "if [ ! -z \"$1\" ]; then\n" )


				before_cmds.append( "./UMBRELLA_EXECUTABLE \\" )
				before_cmds.append( "--sandbox_mode %s \\" % (env.body['sandbox_mode']) )
				before_cmds.append( "--spec %s \\" % (umbrella_spec) )
				before_cmds.append( "--localdir /tmp/prune_umbrella/%s/ \\" %self.sbid )
				before_cmds.append( '--inputs "%s" \\' % (arg_str2[2:]) )
				before_cmds.append( '--output "/tmp/final_data=/tmp/prune_umbrella/%s/res" \\' % (self.sbid) )
				if 'cvmfs_http_proxy' in env.body:
					before_cmds.append( "--cvmfs_http_proxy %s \\" % (env.body['cvmfs_http_proxy']) )
				if 'cms_siteconf' in env.body:
					before_cmds.append( "--cms_siteconf %s \\" % (env.body['cms_siteconf']) )
				before_cmds.append( "--log %s \\" % (env.body['log']) )
				# before_cmds.append( "run \"ls -la | tee %s\"" % (self.env_dbg_log))
				before_cmds.append( "run \"chmod 755 user_cmd.sh; ./user_cmd.sh; RETCODE=\$?; find /tmp >> umbrella_files.log; mv *.log /tmp/final_data/; exit \$RETCODE\" >> %s 2>&1" % (self.env_dbg_log))
				before_cmds.append( "\nRETCODE=$?")

				before_cmds.append( "\nelse\n" )

				before_cmds.append( "./UMBRELLA_EXECUTABLE \\" )
				before_cmds.append( "--sandbox_mode %s \\" % (env.body['sandbox_mode']) )
				before_cmds.append( "--spec %s \\" % (umbrella_spec) )
				before_cmds.append( "--localdir /tmp/prune_umbrella/%s/ \\" %self.sbid )
				before_cmds.append( '--inputs "%s" \\' % (arg_str[2:]) )
				before_cmds.append( '--output "/tmp/final_data=/tmp/prune_umbrella/%s/res" \\' % (self.sbid) )
				if 'cvmfs_http_proxy' in env.body:
					before_cmds.append( "--cvmfs_http_proxy %s \\" % (env.body['cvmfs_http_proxy']) )
				if 'cms_siteconf' in env.body:
					before_cmds.append( "--cms_siteconf %s \\" % (env.body['cms_siteconf']) )
				before_cmds.append( "--log %s \\" % (env.body['log']) )
				# before_cmds.append( "run \"ls -la | tee %s\"" % (self.env_dbg_log))
				before_cmds.append( "run \"chmod 755 user_cmd.sh; ./user_cmd.sh; export RETCODE=\$?; find /tmp >> umbrella_files.log; mv *.log /tmp/final_data/; exit \$RETCODE\" >> %s 2>&1" % (self.env_dbg_log))
				before_cmds.append( "\nRETCODE=$?")

				before_cmds.append( "\nfi\n" )


				after_cmds.append("ls /tmp/prune_umbrella/%s/res >> %s 2>&1" % (self.sbid,self.env_dbg_log))
				after_cmds.append("mv -v /tmp/prune_umbrella/%s/res/* $MYWD/ >> %s 2>&1" % (self.sbid,self.env_dbg_log))

			else:
				self.env_names = ['NA']

				before_cmds.append("chmod 755 user_cmd.sh")
				after_cmds.insert(0,"./user_cmd.sh >> %s 2>&1" % (self.env_dbg_log))
				after_cmds.append("RETCODE=$?" % (self.env_dbg_log))

		after_cmds.append( "echo \"ENV return code:${RETCODE}\" >> %s" % (self.env_dbg_log))
		after_cmds.append('exit $RETCODE')




		with open('%senv.sh' % (self.sandbox), 'w+') as f:
			f.write( "#!/bin/bash\n\n" )
			for cmd in before_cmds:
				f.write(cmd+"\n")
			f.write("\n")
			for cmd in after_cmds:
				f.write(cmd+"\n")

		self.all_inputs.append('env.sh')




	def finish( self ):
		call_body = self.call.body
		if self.call.step:
			glob.workflow_id = self.call.wfid
			glob.workflow_step = self.call.step
		full_content = True
		results = []
		sizes = []

		timer.start('work.stage_out')

		if os.path.isfile(self.sandbox + self.task_dbg_log):
			with open(self.sandbox + self.task_dbg_log) as f:
				for line in f:
					if line.startswith('----prune_task_return'):
						(a, fname, size, cbid) = line[:-1].split('|')
						if size and len(size)>0:
							sizes.append( int(size) )
						else:
							sizes.append( -1 )
						results.append( cbid )
					elif line.startswith('----prune_task_start'):
						(a, taskstart) = line[:-1].split('|')
					elif line.startswith('----prune_task_end'):
						(a, taskfinish) = line[:-1].split('|')

		if os.path.isfile(self.sandbox + self.env_dbg_log):
			with open(self.sandbox + self.env_dbg_log) as f:
				for line in f:
					if line.startswith('----prune_env_start'):
						(a, envstart) = line[:-1].split('|')
					elif line.startswith('----prune_env_end'):
						(a, envfinish) = line[:-1].split('|')

		if os.path.isfile(self.sandbox + self.cmd_dbg_log):
			with open(self.sandbox + self.cmd_dbg_log) as f:
				for line in f:
					if line.startswith('----prune_cmd_start'):
						(a, cmdstart) = line[:-1].split('|')
					elif line.startswith('----prune_cmd_end'):
						(a, cmdfinish) = line[:-1].split('|')


		for i, src in enumerate( call_body['returns'] ):
			if os.path.isfile( self.sandbox + src ):
				it = Item( type='temp', cbid=results[i], dbid=self.call.cbid+':'+str(i), path=self.sandbox + src, size=sizes[i] )
				glob.db.insert( it )

			else:
				d( 'exec', 'sandbox return file not found: '+self.sandbox + src )
				full_content = False


		timer.stop('work.stage_out')

		if full_content:
			d( 'exec', 'Sandbox success kept at: '+self.sandbox )
			timer.start('work.report')

			cmd_time = float(cmdfinish)-float(cmdstart)
			env_time = (float(envfinish)-float(envstart))-cmd_time
			task_time = (float(taskfinish)-float(taskstart))-cmd_time-env_time
			meta = {'task_time':task_time,'env_time':env_time,'cmd_time':cmd_time}
			it = Item( type='work', cbid=self.call.cbid+'()', meta=meta, body={'results':results, 'sizes':sizes} )
			glob.db.insert( it )

			timer.add( cmd_time, 'work.execution_time' )
			timer.add( env_time, 'work.environment_overhead' )
			timer.add( task_time, 'work.prune_overhead' )

			timer.stop('work.report')

		else:
			self.results = results
			d( 'exec', 'sandbox failure kept: '+self.sandbox )
