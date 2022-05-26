import re

resources_all = [["TASK_ID", "core", "memory", "disk", "time"]]

with open("summary_resnet.log", "r") as f:
	Lines = f.readlines()
	for line in Lines:
		resource = []
		if re.search(".*TASK.*DONE SUCCESS  0.*", line):
			#find task id
			task_id = line.split("TASK ")[1].split(" DONE")[0]
			core = line.split("\"cores_avg\":[")[1].split(",\"cores\"],\"cores\":[")[0]
			memory = line.split(",\"memory\":[")[1].split(",\"MB\"],\"virtual_memory")[0]
			disk = line.split(",\"disk\":[")[1].split(",\"MB\"],\"machine")[0]
			time = line.split("\"wall_time\":[")[1].split(",\"s\"],\"cpu_time\":")[0]
			resource = [task_id, core, memory, disk, time]
			resources_all.append(resource)
		else:
			continue
with open("resources_all.txt", "w") as f:
	flag=0
	for resource in resources_all:
		line = resource[0]+" -- "+resource[1]+" -- "+resource[2]+" -- "+resource[3]+" -- "+resource[4]
		if flag == 0:
			line+= " -- "+"drop_out_rate"+" -- "+"num_residual_blocks"
		if flag == 1:
			task_id = int(resource[0])
			i = task_id//20 + 1
			j = task_id%20
			if j == 0:
 				j = 20
			drop_out_rate = i * 0.05
			num_res_block = j * 1
			line += " -- " +str(drop_out_rate)+" -- "+str(num_res_block)
		f.write(str(line) + "\n")
		flag=1
