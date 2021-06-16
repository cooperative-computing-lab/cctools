import PyTask as pt
import work_queue

def divide(dividend, divisor):
	return dividend/divisor

def main():
	
	q = work_queue.WorkQueue(9123)	
	for i in range(1, 16):
		p_task = pt.PyTask(divide, 1, i**2)
		q.submit(p_task)
	
	sum = 0
	while not q.empty():
		t = q.wait(5)
		if t:
			x = t.python_result()
			sum += x

		print(sum)		

if __name__ == '__main__':
	main()
