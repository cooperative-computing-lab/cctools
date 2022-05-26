#Ryan Boccabella
#Converts wq logs so that they will have resource reports all together for a single machine

import sys
import re

if len(sys.argv) != 3:
	print("Usage: " + str(sys.argv[0]) + " file_to_converted converted_file_name")
	sys.exit()

f = open(sys.argv[1], 'r')
f_new = open(sys.argv[2], 'w')
if not f:
	print("Could not open " + sys.argv[1] + " for reading")
if not f_new:
	print("Could not open " + sys.argv[2] + " for writing")

resources_lines_dict = dict()
workers_lines = set()
non_worker_resource_lines = set()

resource_regex = re.compile("wq: rx from ([^ ]*) .*: resource (workers|cores|disk|memory|gpus|unlabeled) [0-9]* [0-9]* [0-9]* [0-9]* [0-9]*")
tag_regex = re.compile("wq: rx from ([^ ]*) .*: resource tag -?[0-9]*")
line_no = 1
for line in f:
	matched = resource_regex.search(line)
	if matched:
		if matched.group(2) == "workers":
			workers_lines.add(line_no)

			#machine name is in group 1, map that to an empty list of incoming resources
			resources_lines_dict[matched.group(1)] = []
		else:
			#some other resource, machine will already be in resources_lines_dict as key
			try:
				resources_lines_dict[matched.group(1)].append(line)
				non_worker_resource_lines.add(line_no)
			except:
				print("This is peculiar, keyerror would have occurred. A resource that is not workers came in first")
	else:
		matched = tag_regex.search(line)
		if matched:
			try:
				resources_lines_dict[matched.group(1)].append(line)
				non_worker_resource_lines.add(line_no)
			except:
				print("This is peculiar, keyerror would have occurred. A resource that is not workers came in first")

	line_no += 1

f.seek(0)

starting_re = re.compile("(.*)(: )resource")
line_no = 1
for line in f:
	if line_no not in non_worker_resource_lines:
		#we're moving all these lines elsewhere, skip it unless its the workers line
		f_new.write(line)

	if line_no in workers_lines:
		#regex to find the machine name
		matched = resource_regex.search(line)
		worker_name = matched.group(1)
		for l in resources_lines_dict[worker_name]:
			f_new.write(l)
		#grab date/time/process/machine info
		starting_match = starting_re.search(l)

		print(starting_match.group(1) + starting_match.group(2))
		f_new.write(starting_match.group(1) + starting_match.group(2) + "info end_of_resource_update 0\n")

	line_no += 1
