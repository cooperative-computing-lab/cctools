from work_queue import *
import sys

def compose_task(i,j, iteration):
    d_rate = i*0.05
    r_blok = j
    outfile = "results%f_%d_%d.csv" % (d_rate, r_blok, iteration)
    command = "bash script.sh results%f_%d.csv %f %d" % (d_rate,r_blok, d_rate, r_blok)

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
		PORT = 9213
		q = WorkQueue(port = PORT, debug_log = "debug.resnet_100.log")
		q.enable_monitoring("all_summary_100")
		q.specify_transactions_log("summary_resnet_100.log")
		q.specify_name("resnet_100")
	except:
		print("Instantiation of Work Queue failed.")
		sys.exit(1)

	print("Listening on port %d..." % q.port)
	ITERATIONS = 100
	#rate then block
	hyper_choice = [[3,5], [14, 3], [10,1], [16, 16]]
	for iteration in range(ITERATIONS):
		for choice in hyper_choice:
			t = compose_task(choice[0], choice[1], iteration)
			taskid = q.submit(t)
			print("Submitted task with id: %d iteration %d hyper choice : %d %d" % (taskid, iteration, choice[0], choice[1]))
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
