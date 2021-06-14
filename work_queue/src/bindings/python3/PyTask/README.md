# PyTask

pytask is an addition to Work Queue wich allows users to execute python functions as Work Queue commands. Functions and their arguments are pickled to a file and executed utilizing a wrapper script to execut the function. the output of the executed function is then written to a file as an output file and read when neccesary allowing the user to get the result as a python variable during runtime and manipulated ;ater.

# PyTask

To create a PyTask the user will need to 'import PyTask'. Once the module is imported the user can generate the task by constructing a Pyask object like so 'p_task = PyTask.PyTask(func, args)' where func is the name of the function and args are the arguments needed to execute the function. PyTask contains a WorkQueue task attribute so that tasks can be submitted as follows 'q.submit(p_task.task())'. once the tasktask has completed, users are able to retrive a python value by calling the 'python_result' method like so 'x  = PyTask.python_result(t)' where t is the task retuned by q.wait(). Basic use of the PyTask module should look similar to the code below:

'''
import PyTask as pt
import work_queue as wq

def foo(x, y)
	...
	...
	return x
...
...
...
q = wq.WorkQueue(9123)
p_task = pt.PyTask(foo num1, num2)
q.submit(p_task.task())
while not q.empty():
	t = q.wait()
	if t:
		x = PyTask.python_result(t)
		...
'''

## Description


