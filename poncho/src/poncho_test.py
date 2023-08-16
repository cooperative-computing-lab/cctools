import work_queue as wq


def test_poncho(i):
	import numpy as np
	import os	
	
	total = 0
	x = np.arange(15)
	total += x.sum()
	
	f = open(os.environ['DATA_DIR'] + '/numbers', 'r')
	for ln in f.readlines():
		nums = ln.split(',')
		for num in nums:
			total += int(num)
	total += i
	return total

def main():
	print('submitting tasks...')
	q = wq.WorkQueue(9123)
	for x in range(50):
		t = wq.PythonTask(test_poncho, x)
		t.specify_environment('poncho.tar.gz')
		q.submit(t)
	
	print('waiting for tasks to complete...')
	while not q.empty():
		t = q.wait(5)
		if t:
			x = t.output
			if isinstance(x, wq.PythonTaskNoResult):
				print('error executuing the function {}'.format(t.std_output))
			print(x)


if __name__ == '__main__':
	main()
	

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
