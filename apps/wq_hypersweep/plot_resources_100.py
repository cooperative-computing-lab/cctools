import matplotlib.pyplot as plt

def gen_fig(plot_sth, resource_type, hyp):
	print(hyp+resource_type)
	x_axis = []
	y_axis = []
	plot_sth.sort(key=lambda x : x[0])
	for point in plot_sth:
		x_axis.append(point[0])
		y_axis.append(point[1])
	plt.figure()
	plt.scatter(x_axis, y_axis)
	plt.xlabel(hyp)
	plt.ylabel(resource_type)
	plt.ylim(ymin=0)
	plt.savefig(hyp+"_"+resource_type+"_100"+".png")
	plt.close()

resources_all = []
plot_rate_and_core = []
plot_rate_and_mem = []
plot_rate_and_disk = []
plot_rate_and_time = []
plot_block_and_core = []
plot_block_and_mem = []
plot_block_and_disk = []
plot_block_and_time = []

print("Checkpoint 1")
with open("resources_all_100.txt", "r") as f:
	Lines = f.readlines()
	for line in Lines[1:]:
		resource = line.split(" -- ")
		resource[4] = resource[4].split("\n")[0]
		resources_all.append(resource)
		task_id = int(resource[0])
		hyper_choice = [[3,5], [14, 3], [10,1], [16, 16]]
		cal_hyper = task_id % 4
		d_rate = hyper_choice[cal_hyper-1][0] * 0.05
		r_block = hyper_choice[cal_hyper-1][1]
		print(task_id)
		plot_rate_and_core.append([d_rate, round(float(resource[1]), 3)])
		plot_rate_and_mem.append([d_rate,  int(resource[2])])
		plot_rate_and_disk.append([d_rate, int(resource[3])])
		plot_rate_and_time.append([d_rate, round(float(resource[4]), 2)])
		plot_block_and_core.append([r_block, round(float(resource[1]), 3)])
		plot_block_and_mem.append([r_block, int(resource[2])])
		plot_block_and_disk.append([r_block, int(resource[3])])
		plot_block_and_time.append([r_block, round(float(resource[4]), 2)])


print("Checkpoint 2")
gen_fig(plot_rate_and_core, "core", "drop_out_rate")
gen_fig(plot_rate_and_mem, "memory", "drop_out_rate")
gen_fig(plot_rate_and_disk, "disk", "drop_out_rate")
gen_fig(plot_rate_and_time, "time", "drop_out_rate")
gen_fig(plot_block_and_core, "core", "number_of_residual_blocks")
gen_fig(plot_block_and_mem, "memory", "number_of_residual_blocks")
gen_fig(plot_block_and_disk, "disk", "number_of_residual_blocks")
gen_fig(plot_block_and_time, "time", "number_of_residual_blocks")
		

 
