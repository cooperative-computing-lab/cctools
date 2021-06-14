import work_queue as wq
import dill
import os
import sys
import tempfile
import uuid
import traceback


class PyTask(object):
	
	p_tasks = []

	def __init__(self, func, *args):
		self._id = str(uuid.uuid4())
		self._tmpdir = 'tmp/'
		self._task = self.python_function_command(func, *args)
		PyTask.p_tasks.append(self)
		
	
	def task(self):
		return self._task
	
	def python_function_command(self, func, *args):
			
		func_file = os.path.join(self._tmpdir, 'function_{}.p'.format(self._id))
		args_file = os.path.join(self._tmpdir, 'args_{}.p'.format(self._id))
		out_file = os.path.join(self._tmpdir, 'out_{}.p'.format(self._id))
		package_file = os.path.join(self._tmpdir, 'dep.yml')
		tar_file = os.path.join(self._tmpdir, 'venv.tar.gz')
		pp_run = '/afs/crc.nd.edu/user/b/bslydelg/cctools/python_environments/src/python_package_run'		

	
		tb = traceback.extract_stack()
		tb_list = traceback.format_list(tb)
		caller = tb_list[len(tb_list)-3].split()[1]
		caller = caller.replace('"', '')
		caller = caller.replace(',', '')


		if not os.path.exists(tar_file):
			os.system('python_package_analyze ' + caller + ' ' + package_file)
			os.system('python_package_create ' + package_file + ' ' + tar_file)
			
		
		with open(func_file, 'wb') as wf:
			dill.dump(func, wf)
		
		with open(args_file, 'wb') as wf:
			dill.dump([*args], wf)
		
		command = "./python_package_run -e {tar} python py_wrapper.py {function} {args} {out}".format(
			tar=os.path.basename(tar_file),
			function=os.path.basename(func_file),
			args=os.path.basename(args_file),
			out=os.path.basename(out_file))

		task = wq.Task(command)

		task.specify_input_file('py_wrapper.py', cache=True)
		task.specify_input_file(pp_run, cache=False)
		task.specify_input_file(tar_file, cache=False)
		task.specify_input_file(func_file, cache=False)
		task.specify_input_file(args_file, cache=False)
		task.specify_output_file(out_file, cache=False)

		return task

	@staticmethod
	def python_result(task):		
		
		for t in PyTask.p_tasks:
			if t._task is task:
				p_task = t	
		
		if p_task._task.result == wq.WORK_QUEUE_RESULT_SUCCESS:
			try:
				with open(os.path.join(p_task._tmpdir, 'out_{}.p'.format(p_task._id)), 'rb') as f:
					func_result = dill.load(f)
			except Exception as e:
				func_result = e
		else:
			print('no reusly for task {}, error code {} {}'.format(p_task._task.tag, p_task._task.result, P_task._task.result_str))
			func_result = NoResult()
		
		return func_result

def divide(dividend, divisor):
	return dividend/divisor

def main():
	
	p_tasks  = []
	
	q = wq.WorkQueue(9123)	
	for i in range(1, 16):
		p_task = PyTask(divide, 1, i**2)
		q.submit(p_task.task())
	
	sum = 0
	while not q.empty():
		t = q.wait(5)
		if t:
			x = PyTask.python_result(t)
			sum += x

		print(sum)		
