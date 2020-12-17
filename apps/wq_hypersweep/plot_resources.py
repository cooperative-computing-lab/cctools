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
	plt.savefig(hyp+"_"+resource_type+".png")
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
with open("resources_all.txt", "r") as f:
	Lines = f.readlines()
	for line in Lines[1:]:
		resource = line.split(" -- ")
		resource[6] = resource[6].split("\n")[0]
		resources_all.append(resource)
		plot_rate_and_core.append([round(float(resource[5]), 2), round(float(resource[1]), 3)])
		plot_rate_and_mem.append([round(float(resource[5]), 2), int(resource[2])])
		plot_rate_and_disk.append([round(float(resource[5]), 2), int(resource[3])])
		plot_rate_and_time.append([round(float(resource[5]), 2), round(float(resource[4]), 2)])
		plot_block_and_core.append([int(resource[6]), round(float(resource[1]), 3)])
		plot_block_and_mem.append([int(resource[6]), int(resource[2])])
		plot_block_and_disk.append([int(resource[6]), int(resource[3])])
		plot_block_and_time.append([int(resource[6]), round(float(resource[4]), 2)])


print("Checkpoint 2")
gen_fig(plot_rate_and_core, "core", "drop_out_rate")
gen_fig(plot_rate_and_mem, "memory", "drop_out_rate")
gen_fig(plot_rate_and_disk, "disk", "drop_out_rate")
gen_fig(plot_rate_and_time, "time", "drop_out_rate")
gen_fig(plot_block_and_core, "core", "number_of_residual_blocks")
gen_fig(plot_block_and_mem, "memory", "number_of_residual_blocks")
gen_fig(plot_block_and_disk, "disk", "number_of_residual_blocks")
gen_fig(plot_block_and_time, "time", "number_of_residual_blocks")
		

 
