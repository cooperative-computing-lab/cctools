#!/usr/bin/env python3

# copyright (C) 2021- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import work_queue as wq
import dill
import os
import sys
import tempfile
import uuid
import traceback
import shutil
import textwrap

##
# \class PyTask
#
# Python PyTask object
#
# this class is used to create a python task
class PyTask(wq.Task):
	
	p_tasks = []
	##
	# Creates a new python task
	#
	# @param self 	Reference to the current python task object
	# @param func	python function to be executed by task
	# @param args	arguments used in function to be executed by task
	def __init__(self, func, *args):
		
		self._id = str(uuid.uuid4())
		self._tmpdir = tempfile.mkdtemp()

		self._func_file = os.path.join(self._tmpdir, 'function_{}.p'.format(self._id))
		self._args_file = os.path.join(self._tmpdir, 'args_{}.p'.format(self._id))
		self._out_file = os.path.join(self._tmpdir, 'out_{}.p'.format(self._id))
		self._wrapper = os.path.join(self._tmpdir, 'pytask_wrapper.py'.format(self._id))
		#self._package_file = os.path.join(self._tmpdir, 'dep.yml')
		self._tar_file  = 'venv.tar.gz'
		self._pp_run = shutil.which('python_package_run')		

		self._command = self.python_function_command(func, *args)
		self.create_wrapper()

		super(PyTask, self).__init__(self._command)
		
		self.specify_IO_files()
		

		PyTask.p_tasks.append(self)

	def __del__(self):
		
		try:
			if self._tmpdir and os.path.exists(self._tmpdir):
				shutil.rmtree(self._tmpdir)

		except Exception as e:
			sys.stderr.write('could not delete {}: {}\n'.format(self._tmpdir, e))
			
				
	##
	# Cretes the command to be executed by task. pickles function and arguments.
	#
	# @param self	reference to the current python task object
	# @param func 	function to be executed by the task
	# @param args	arguments used in function to be executed by task
	def python_function_command(self, func, *args):
			
	
		tb = traceback.extract_stack()
		tb_list = traceback.format_list(tb)
		caller = tb_list[len(tb_list)-3].split()[1]
		caller = caller.replace('"', '')
		caller = caller.replace(',', '')


	#	if not os.path.exists(tar_file):
	#		os.system('python_package_analyze ' + caller + ' ' + package_file)
	#		os.system('python_package_create x.yml ' + self._tar_file)
			
		
		with open(self._func_file, 'wb') as wf:
			dill.dump(func, wf)
		
		with open(self._args_file, 'wb') as wf:
			dill.dump([*args], wf)
		
		command = './{pprun} -e {tar} --unpack-to "$WORK_QUEUE_SANDBOX"/{unpack}-env python {wrapper} {function} {args} {out}'.format(
			pprun=os.path.basename(self._pp_run),
			unpack=os.path.basename(self._tar_file),
			tar=os.path.basename(self._tar_file),
			wrapper=os.path.basename(self._wrapper),
			function=os.path.basename(self._func_file),
			args=os.path.basename(self._args_file),
			out=os.path.basename(self._out_file))


		return command
	
	##
	# specifies input and output files needed to execute the task.
	#
	# @param self	reference to the current python task object
	def specify_IO_files(self):

		self.specify_input_file(self._wrapper, cache=True)
		self.specify_input_file(self._pp_run, cache=True)
		self.specify_input_file(self._tar_file, cache=False)
		self.specify_input_file(self._func_file, cache=False)
		self.specify_input_file(self._args_file, cache=False)
		self.specify_output_file(self._out_file, cache=False)

	##
	# creates the wrapper script which will execute the function. pickles output.
	#
	# @param self	reference to the current python task object
	def create_wrapper(self):
		
		with open(self._wrapper, 'w') as f:
			f.write(textwrap.dedent('''\
				import sys
				import dill
				(fn, args, out) = sys.argv[1], sys.argv[2], sys.argv[3]
				with open (fn , 'rb') as f:
					exec_function = dill.load(f)
				with open(args, 'rb') as f:
					exec_args = dill.load(f)
				try:
					exec_out = exec_function(*exec_args)

				except Exception as e:
					exec_out = e

				with open(out, 'wb') as f:
					dill.dump(exec_out, f)

				print(exec_out)'''))
			 
				
	##
	# returns the result of a python task as a python variable
	#
	# @param self	reference to the current python task object
	def python_result(self):		
		if self.result == wq.WORK_QUEUE_RESULT_SUCCESS:
			try:
				with open(os.path.join(self._tmpdir, 'out_{}.p'.format(self._id)), 'rb') as f:
					func_result = dill.load(f)
			except Exception as e:
				func_result = e
		else:
			func_result = PyTaskNoResult()
		
		return func_result

class PyTaskNoResult(Exception):
	pass
