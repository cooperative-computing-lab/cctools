from work_queue import *
import sys


def compose_task(i,j):
	id = (i-1)*20+j
	d_rate = i*0.05
	r_blok = j
	outfile = "results%d.csv" % id
	command = "bash script.sh results%d.csv %f %d" % (id,d_rate,r_blok)

	t = Task(command)
    
	t.specify_file("/usr/bin/bash", "bash", WORK_QUEUE_INPUT, cache=True)
	t.specify_file("env.tar.gz", "env.tar.gz", WORK_QUEUE_INPUT, cache=True)
	t.specify_file("datasets/cifar-10-batches-py", "datasets/cifar-10-batches-py", WORK_QUEUE_INPUT, cache=True)
	t.specify_file("resnet.py", "resnet.py", WORK_QUEUE_INPUT, cache=True)
	t.specify_file("script.sh", "script.sh", WORK_QUEUE_INPUT, cache=True)
	t.specify_file(outfile, outfile, WORK_QUEUE_OUTPUT, cache=False)
	return t



def main():

	try:
		q = WorkQueue(port = WORK_QUEUE_DEFAULT_PORT, debug_log = "my.debug.log")
		q.enable_monitoring()
		q.specify_transactions_log("summary_resnet.log")
		q.specify_name("resnet_hypersweep")
		#q.specify_password("AAAZZZ")
	except:
		print("Instantiation of Work Queue failed.")
		sys.exit(1)

	print("Listening on port %d..." % q.port)

	for i in range(1,20):
		for j in range (1,21):
			t = compose_task(i,j)
			taskid = q.submit(t)
			print("Submitted task (id# %d): %s" % (taskid, t.command))
			with open("hyper_par_to_task.txt", "a") as f:
				f.write("Task id :" + str(taskid)+ " - i, j: "+ str(i)+", "+str(j)+"\n")
	print("waiting for tasks to complete...")
	whitelist = []
	blacklist = []
	while not q.empty():
		t = q.wait(5)
		if t:
			print("task (id# %d) complete: %s (return code %d)" % (t.id, t.command, t.return_status))
			if t.return_status == 0:
				if t.hostname not in whitelist:
					whitelist.append(t.hostname)
			if t.return_status != 0:
				print("stdout:\n{}".format(t.output))
				print("Blacklisting host: %s" % t.hostname)
				q.blacklist(t.hostname)
				blacklist.append(t.hostname)
				q.submit(t)
				print("Resubmitted task (id# %s): %s" % (t.id, t.command))

	print("All tasks complete.")
	print("Whitelist:", whitelist)
	print("Blacklist:", blacklist)

	sys.exit(0)

if __name__ == '__main__':
	main()
