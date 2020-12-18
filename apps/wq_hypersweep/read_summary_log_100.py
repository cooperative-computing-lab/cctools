import re

resources_all = [["taskid", "core", "memory", "disk", "time"]]

with open("all_summary_100/wq-17489.summaries", "r") as f:
	Lines = f.readlines()
	Lines[0] = Lines[0].split("140722965902.032,\"procs\"]}")[1]
	for line in Lines:
		resource = []
			#find task id
		task_id = line.split('{"task_id":"')[1].split('","category"')[0]
		core = line.split('"cores_avg":[')[1].split(',"cores"],')[0]
		memory = line.split('"procs"],"memory":[')[1].split(",\"MB\"],\"virtual_memory")[0]
		disk = line.split('les"],"disk":[')[1].split(",\"MB\"],\"machine")[0]
		time = line.split("\"wall_time\":[")[1].split(",\"s\"],\"cpu_time\":")[0]
		resource = [task_id, core, memory, disk, time]
		resources_all.append(resource)
		print("task " + task_id + " processed successfully!") 
with open("resources_all_100.txt", "w") as f:
	for resource in resources_all:
		line = resource[0]+" -- "+resource[1]+" -- "+resource[2]+" -- "+resource[3]+" -- "+resource[4]
		f.write(str(line) + "\n")
