#Author: Ryan Boccabella for CCL, (C) University of Notre Dame 2015
#A tool that reads in debug logs from work_queue and keeps the state of the work_queue
#workers and their communications with the manager in an image format, combining the
#images into a gif video for visual debugging of both a single run of work_queue, as
#well as for looking for hiccups/areas of improvement to work_queue itself

#This will work for all logs created after it is required that worker resource reports be handled all at once by the manager, rather than one line at a time. It banks on the resource report arriving on consecutive lines so that the workers' size is representative of its resources.

from PIL import Image, ImageDraw, ImageFont
import re
import sys
import subprocess
import os
import math

#debug = 1
#show_unused = 1
#count_ips = 1
#tcp_debug = 1
#task_debug = 1
#debug_machine = 1
#task_info_debug = 1
#file_display_debug = 1
gif_debug = 1

#variables in pixels
MACHINE_WIDTH = 40
BUFFER_SPACE = 10
GIF_WIDTH = 1024
GIF_HEIGHT = 512
TEXT_HEIGHT = 32
LEGEND_WIDTH = max(100, min(220, int(GIF_WIDTH / 5.5)))
CONNECTION_WIDTH = 2
MACHINE_BORDER_WIDTH = 2
FULL_FILES_PER_FILEROW = 4
MIN_FILEROW_FILE_WIDTH = 4
NUM_FILE_ROWS_IN_MACHINE = 2
#NUM_TASK_ROWS_IN_MACHINE = 2
RESOURCE_ROW_HEIGHT = MACHINE_WIDTH / 4
LEGEND_SLOT_HEIGHT = 15
LEGEND_SLOT_BUFFER = 8
BASIC_MACHINE_SPACE = 3 * RESOURCE_ROW_HEIGHT

#Other variables
FONT_FILE = "cour.ttf"  #sorry Prof Thain, but it'd be really odd to have this as a command line argument
GIF_APPEND_THRESHOLD = 1000
FRAME_DELAY = 1  #in ms. Probably the fastest that displays will allow if they allow it to be this fast
MANAGER_CORE_SPACE = 2  #give the manager 2 cores worth of height in addition to whatever row height it has

NUM_STARTING_COLORS = 100
WHITE = (255,255,255)
BLACK = (0,0,0)
RED = (200, 0, 0)
BLUE = (0,0,200)
GREEN = (0,200,0)
PURPLE = (100,0,100)
LIGHT_BLUE = (0, 0, 235)

def add_manager_flair(draw, manager):
	start_point = manager.connection_point
	draw.line( (start_point[0], start_point[1], GIF_WIDTH - LEGEND_WIDTH - BUFFER_SPACE, start_point[1]), BLACK, width = CONNECTION_WIDTH)
	(text_x, text_y) = manager.top_left_corner
	text_y = text_y + MACHINE_WIDTH/4
	text_x = text_x + MACHINE_WIDTH/4
	font = ImageFont.truetype(FONT_FILE, MACHINE_WIDTH/2)
	draw.text((text_x, text_y), "M", font=font, fill=RED)

def color_connection(draw, manager, worker, color):
	m_location = manager.connection_point
	w_location = worker.connection_point

	draw_connection_on_image(draw, w_location, color) #find_machine_connection_line_pixel(worker.grid_location), color)

	vert_x = w_location[0] + BUFFER_SPACE
	vert_y_1 = m_location[1]
	vert_y_2 = w_location[1]

	horz_x_1 = m_location[0]
	horz_x_2 = vert_x
	horz_y = vert_y_1

	draw.line((vert_x, vert_y_1, vert_x, vert_y_2), color, width = CONNECTION_WIDTH)
	draw.line((horz_x_1, horz_y, horz_x_2, horz_y), color, width = CONNECTION_WIDTH)

def find_ip(line):
	found_ip = re.search("([0-2]?[0-9]?[0-9]\.){3}[0-2]?[0-9]?[0-9]:[0-9]+",line)
	if(found_ip):
		return found_ip.group(0)  #return the whole regular expression
	else:
		return None

def fill_in_text(draw, font, line):
	action = get_action_from_line(line)
	draw.text((BUFFER_SPACE, GIF_HEIGHT-TEXT_HEIGHT+BUFFER_SPACE), action, font=font, fill=BLACK)

def clear_text_box(draw):
	draw.rectangle( (0, GIF_HEIGHT - TEXT_HEIGHT, GIF_WIDTH, GIF_HEIGHT), fill=WHITE)

def get_file_from_line(line):
	f_hash_name = "file-[0-9a-fA-F]*-([^ ]*)"
	pattern_action = [(": put "+f_hash_name, "getting needed (put)"), (": infile "+f_hash_name, "using as infile (infile)"), (": get " + f_hash_name, "manager will be requesting file (get)"), ("Receiving file ([^ ]*)", "sending file to manager (receiving)"), (": file "+f_hash_name, "manager requesting file (file)"), (": outfile "+f_hash_name, "outfile request by manager (outfile)")]

	fileInf = None
	for item in pattern_action:
		matched = re.search(item[0], line)
		if(matched):
			fileInf = File_Info(matched.group(1), item[1])
			break

	return fileInf

def get_task_from_line(line):
	task_inf = None
	#if looking for name of binary running, try switching task to cmd and regex searching differently
	pattern_action = [(": task ([0-9]*)", "assigned"), (": kill ([0-9]*)", "removed"), (": result [0-9]+ [0-9]+ [0-9]+ [0-9]+ ([0-9]+)", "result")]
	if line.split(" ")[3] == "wq:":
		for item in pattern_action:
			matched = re.search(item[0], line)
			if(matched):
				task_inf = Task_Info(matched.group(1), item[1])
				break

	return task_inf

def get_action_from_line(line):
	line_cause = line.split(" ")[3]   #debug:, wq:, batch:, etc.
	description = line[(line.find(line_cause) + len(line_cause) + 1):]
	return description


def draw_connection_on_image(draw, image_loc, color):
	draw.line((image_loc[0], image_loc[1], image_loc[0] + BUFFER_SPACE, image_loc[1]), color, width = CONNECTION_WIDTH)

#create machine, add it to image, and to image awareness (connections)
def add_machine(draw, machine_type, ip, connections, workers, fileCount, legend=None, resources=None):
	if(machine_type == "manager"):
		manager_top_corner = (BUFFER_SPACE, BUFFER_SPACE)
		this_machine = Machine("Master", "manager", ip, manager_top_corner, fileCount, draw, legend, MACHINE_WIDTH, MACHINE_WIDTH, resources)

	elif(machine_type == "worker"):
		(top_left_corner, width, height) = connections.add(draw, resources)
		if "debug" in globals():
			print((top_left_corner, width, height))
		if( not (top_left_corner, width, height) ):
			return
		worker_name = ""
		this_machine = Machine(worker_name, "worker", ip, top_left_corner, fileCount, draw, legend, width, height, resources)
		workers[ip] = this_machine

	else:
		if "debug" in globals():
			print("tried to add machine of unspecified type "+machine_type)
		sys.exit()


	return this_machine

def pad(num, append_thresh, last_appended_num):
	top_num = last_appended_num + append_thresh   #the highest number frame before we'll convert to a gif and rm
	num_str = str(num)
	top_str = str(top_num)
	pad_on_front = len(top_str) - len(num_str)
	dummy_str = ""
	while len(dummy_str) < pad_on_front:
		dummy_str = dummy_str + "0"

	return dummy_str + num_str

class o_dict(object):
	def __init__(self):
		self.keys = []
		self.dictionary = dict()

	def append(self, key, value):
		if key not in self.dictionary:
			self.keys.append(key)

		self.dictionary[key] = value

	def remove(self, key):
		if key in self.dictionary:
			self.keys.remove(key)
			self.dictionary.pop(key)

	def swap_key_order(self, index_1, index_2):
		try:
			tmp_key = self.keys[index_1]
			self.keys[index_1] = self.keys[index_2]
			self.keys[index_2] = tmp_key
		except:
			#catch out of bounds errors, but don't do anything about them, just don't have them kill the program
			if "debug" in globals():
				print("Illegal swap attempt for o_dict")
class Resources(object):
	def __init__(self):
		self.cores = Resource()
		self.gpus = Resource()
		self.memory = Resource()
		self.disk = Resource()
		self.workers = Resource()
		self.unlabeled = Resource()
		self.tag = None

class Resource(object):
	def __init__(self):
		self.inuse = 0
		self.committed = 0
		self.total = 0
		self.smallest = 0
		self.largest = 0

class FT_Info(object):
	def __init__(self, fname, state, direction, expiration):
		self.state = state
		self.direction = direction
		self.fname = fname
		self.expiration = expiration


def read_resource_report(line, log):
	#assume the line log is at does not contain a resource report
	isReport = False

	general_resource_regex = ": resource ([a-z]*) (-?[0-9]*) (-?[0-9]*) ([-?0-9]*) (-?[0-9]*) (-?[0-9]*)"
	tag_regex = ": resource tag (-?[0-9]*)"
	end_regex = ": info end_of_resource_update 0"
	if "debug" in globals():
		print("First line in read_resource_report is " + line)

	matched = re.search(general_resource_regex, line)
	if(matched):
		isReport = True

	matched = re.search(tag_regex, line)
	if(matched):
		isReport = True

	if not isReport:
		return None

	#otherwise, it is a report, so unread the last line and read the whole report as a single entity
	log.seek(-len(line), 1)
	need_to_read = True
	resources = Resources()

	while need_to_read:
		recent_line = log.readline()
		if "debug" in globals():
			print("In read_resource_report, read: " + recent_line)
		matched = re.search(general_resource_regex, recent_line)
		if matched:
			isResourceObject = True
			resource_str = matched.group(1)
			if(resource_str == "cores"):
				resource = resources.cores
			elif(resource_str == "gpus"):
				resource = resources.gpus
			elif(resource_str == "memory"):
				resource = resources.memory
			elif(resource_str == "disk"):
				resource = resources.disk
			elif(resource_str == "workers"):
				resource = resources.workers
			elif(resource_str == "unlabeled"):
				resource = resources.unlabeled
			else:
				# a new type of resource exists that I haven't coded for, and so there is no resource to set inuse, committed, etc for
				isResourceObject = False

			if(isResourceObject):
				resource.inuse = int(matched.group(2))
				resource.committed = int(matched.group(3))
				resource.total = int(matched.group(4))
				resource.smalleset = int(matched.group(5))
				resource.largest = int(matched.group(6))
			continue

		matched = re.search(tag_regex, recent_line)
		if matched:
			resources.tag = int(matched.group(1))
			continue

		matched = re.search(end_regex, recent_line)
		if matched:
			#resource report is over, stop reading the log file
			need_to_read = False

	return resources

class TaskResources(object):
	def __init__(self):
		self.cores = None
		self.gpus = None
		self.memory = None
		self.disk = None
		self.tag = None

	def set_resource(self, resource_str, value):
		if(resource_str == "cores"):
			self.cores = value
		elif(resource_str == "gpus"):
			self.gpus = value
		elif(resource_str == "memory"):
			self.memory = value
		elif(resource_str == "disk"):
			self.disk = value
		elif(resource_str == "unlabeled"):
			self.unlabeled = value

class Task_Info(object):
	def __init__(self, num=None, status=None):
		self.num = num
		self.status = status
		self.command_num = None
		self.exe = None
		self.command_string = None
		self.infiles = set() #may not need this one
		self.outfiles = set()
		self.resources = TaskResources()

	def set_info(self, log, fileCount, ip, color_list, legend):
		if "task_info_debug" in globals():
			print("-------------------------------------------------\nIn set_info:\n")

		need_to_read = True

		#every task requires one infile, that is the executable
		has_infile = False
		while need_to_read:
			recent_line = log.readline()
			if "task_info_debug" in globals():
				print("Read recent line: " + recent_line)

			matched = re.search(": cmd ([0-9]+)(.*)", recent_line)
			if(matched):
				self.command_num = matched.group(1)
				self.command_string = log.readline().rstrip("\n")
				exe_match = re.search("^([^ ]*)", self.command_string)
				if(exe_match):
					self.exe = exe_match.group(1)
				else:
					self.exe = self.command_string

				if "task_info_debug" in globals():
					print("\ncommand_info:\n\texe: " + self.exe + "\n\tcommand_string: " + self.command_string + "\n\tcommand_num: " + self.command_num + "\n")
				continue

			matched = re.search(": infile (.*)", recent_line)
			if(matched):
				has_infile = True
				infile = matched.group(1).split(" ")[1]
				self.infiles.add(infile)
				if(infile in fileCount):
					fileCount[infile].occur_count = fileCount[infile].occur_count + 1
				else:
					fileCount[infile] = File_Distribution_Info(ip, color_list)

				legend.update(infile)
				if "task_info_debug" in globals():
					print("\ninfile: " + infile + "\n")
				continue

			matched = re.search(": outfile (.*)", recent_line)
			if(matched):
				outfile = matched.group(1).split(" ")[1]
				self.outfiles.add(outfile)
				if(outfile in fileCount):
					fileCount[outfile].occur_count = fileCount[outfile].occur_count + 1
				else:
					fileCount[outfile] = File_Distribution_Info(ip, color_list)

				legend.update(outfile)
				if "task_info_debug" in globals():
					print("\noutfile: " + outfile + "\n")
				continue

			matched = re.search(": tag (-?[0-9]*)", recent_line)
			if(matched):
				self.resources.tag = int(matched.group(1))
				continue

			matched = re.search(": tx .*: (cores|gpus|memory|disk|unlabeled) (-?[0-9]*)", recent_line)
			if(matched):
				if "task_info_debug" in globals():
					print("matched on set info for task resources, resource=" + matched.group(1))

				self.resources.set_resource( matched.group(1), int(matched.group(2)) )


				if "task_info_debug" in globals():
					print("task " + str(self.num) + " requires " + str(matched.group(1)) + " to be: " + str(matched.group(2)))

				continue

			matched = re.search(": end", recent_line)
			if(matched):
				need_to_read = False

		if "task_info_debug" in globals():
			print("Leaving set_info \n----------------------------------------------------------------------\n")

class File_Info(object):
	def __init__(self, name=None, action=None):
		self.name = name
		self.action = action
		self.is_possessed = False


class File_Distribution_Info(object):
	#tells how many times a file has been mentioned by log file
	#which workers it is located in, and the color coding for it
	def __init__(self, ip, color_list):
		self.occur_count = 1
		self.ip_set = set([ip])
		self.color = color_list.pop() #take the first item in the list

class Machine_Task_Display(object):
	def __init__(self, draw, top_left_corner, resources):
		from_corner_to_interior =  MACHINE_BORDER_WIDTH / 2
		self.in_machine_space = MACHINE_WIDTH - 2 * from_corner_to_interior  #width of bar needs integer division, then doubled
		self.draw = draw
		if resources:
			self.total_cores = resources.cores.largest
		else:
			self.total_cores = MANAGER_CORE_SPACE

		self.tasks_to_cores = dict() #key is task no, value is list of cores task uses on this machine, 0-indexed
		self.tasks_to_status = dict() #key is task no, value is "running" or "completed"
		self.tasks_to_exes = dict()
		self.core_in_use = []

		y_offset = RESOURCE_ROW_HEIGHT * NUM_FILE_ROWS_IN_MACHINE# should be * num_file_rows, but this is static for now
		self.top_corner_adjusted = (top_left_corner[0] + from_corner_to_interior, top_left_corner[1] + from_corner_to_interior+ y_offset)
		self.row_height = RESOURCE_ROW_HEIGHT
		if "debug" in globals():
			print("top_y_pixel tasks= " + str(self.top_corner_adjusted[1]))

		i = 0
		while i < self.total_cores:
			self.core_in_use.append(False)
			if(resources):
				self.clear_core(i)
			i = i + 1


	def add_task(self, ip, task_info):
		#might expand to include command or task #, but not yet
		#idealistically process mapping to a color.... see Machine_File_Display object

		self.tasks_to_exes[task_info.num] = task_info.exe
		self.tasks_to_status[task_info.num] = "running"
		needed_cores = max(1, task_info.resources.cores)
		if "debug" in globals():
			print("need a machine with " + str(needed_cores) + " cores to run task " + str(task_info.num))

		cores_to_use = []
		i = 0
		while(needed_cores > 0 and i < self.total_cores):
			if not self.core_in_use[i]:
				cores_to_use.append(i)
				needed_cores = needed_cores - 1
				self.core_in_use[i] = True
			i = i + 1

		if(needed_cores == 0):
			#fulfilled the request
			self.tasks_to_cores[task_info.num] = cores_to_use
			if "debug" in globals():
				print("added task " + str(task_info.num) + " to cores " + str(cores_to_use))
		else:
			if "debug" in globals():
				print("Tried to add task to a core that already has a task running.")
			for core in cores_to_use:
				self.core_in_use[core] = False

		self.show_task(task_info.num)

	def finish_task(self, ip, task_no):
		#should be as easy as saying the task is finished
		if "task_debug" in globals():
			try:
				print(ip + " trying to finish task " + str(task_no) + " on cores " + str(self.tasks_to_cores[task_no]))
			except:
				pass

		if task_no in self.tasks_to_status:
			self.tasks_to_status[task_no] = "finished"
			if "task_debug" in globals():
				print("finished")
				return
		else:
			if "task_debug" in globals():
				print(ip + " failed to finish task " + str(task_no) + " in " + str(row))
		self.show_task(task_no)

	def remove_task(self, ip, task_no):
		if "task_debug" in globals():
			try:
				print(ip + " trying to remove task " + str(task_no) + " from cores " + str(self.tasks_to_cores[task_no]))
			except:
				pass

		if task_no in self.tasks_to_cores:
			freed_cores = self.tasks_to_cores[task_no]
			for core in freed_cores:
				self.core_in_use[core] = False
				self.clear_core(core)
			#remove them from the dictionaries
			self.tasks_to_cores.pop(task_no)
			self.tasks_to_status.pop(task_no)

			if "task_debug" in globals():
				print("removed")
		else:
			if "task_debug" in globals():
				print(ip + " failed to remove task " + str(task_no))

	def clear_core(self, core_number):
		x1 = self.top_corner_adjusted[0]
		y1 = self.top_corner_adjusted[1] + core_number * self.row_height
		#Draw the main of the core blank
		self.draw.rectangle( [x1, y1, x1+self.in_machine_space, y1+self.row_height-1], fill=WHITE )
		#Draw the top core separator
		self.draw.line( [x1, y1, x1+self.in_machine_space, y1], fill=BLACK, width = 1)
		if "task_info_debug" in globals():
			print("clearing core " + str(core_number) + " with edges at y = " + str(y1) + " and y = " + str(y1 + self.row_height-1))

	def show_task(self, task_no):
		if "task_info_debug" in globals():
			print("in show_task for task_no " + str(task_no))
		try:
			cores_used = self.tasks_to_cores[task_no]
			status = self.tasks_to_status[task_no]
			if status == "running":
				color = BLUE
			else:
				color = RED
		except:
			if "task_debug" in globals():
				print("trying to show task that is not in dictionary tasks_to_cores")
			return

		i = 0
		num_cores_used = len(cores_used)
		while i < num_cores_used:
			core = cores_used[i]
			#clear row
			self.clear_core(core)
			self.fill_core_with_task(core, i, num_cores_used, color, task_no)
			i += 1

	def fill_core_with_task(self, core_number, tasks_nth_core, tasks_total_cores, color, task_no):
		process_radius = ( tasks_total_cores * (self.row_height/2) ) - 1
		x1 = self.top_corner_adjusted[0]
		y1 = self.top_corner_adjusted[1] + core_number * self.row_height

		font = ImageFont.truetype(FONT_FILE, int(TEXT_HEIGHT * 1/float(3.5)) )
		if( process_radius > (self.in_machine_space - 2) ) :
			#TODO make this work for an ellipse
			if "task_debug" in globals():
				print("task was too wide for machine, need to make it an oval")
		self.draw.rectangle( [x1, y1, x1+self.in_machine_space, y1+self.row_height-1], fill = color )

		#Draw the top core separator
		self.draw.line( [x1, y1, x1+self.in_machine_space, y1], fill=BLACK, width = 1)
		if "task_debug" in globals():
			print("filling core " + str(core_number) + " " + str(color) + " with edges at y = " + str(y1) + " and y = " + str(y1 + self.row_height))

		self.draw.text( (x1+2*MACHINE_BORDER_WIDTH, y1), self.tasks_to_exes[task_no], font=font, fill=WHITE)

class Machine_File_Display(object):
	def __init__(self, draw, top_left_corner, resources):
		#TODO use resources to determine number of file rows
		from_corner_to_interior =  MACHINE_BORDER_WIDTH / 2
		self.in_machine_space = MACHINE_WIDTH - 2 * from_corner_to_interior  #width of bar needs integer division, then doubled
		self.draw = draw
		self.file_rows = []
		for i in range(NUM_FILE_ROWS_IN_MACHINE):
			self.file_rows.append( (i, o_dict()) )

		self.max_files_in_row = int(self.in_machine_space / MIN_FILEROW_FILE_WIDTH)
		self.max_filebox_width = int(self.in_machine_space / FULL_FILES_PER_FILEROW)
		self.row_height = RESOURCE_ROW_HEIGHT
		self.top_corner_adjusted = (top_left_corner[0] + from_corner_to_interior, top_left_corner[1] + from_corner_to_interior)
		if "task_debug" in globals():
			print("top_y_pixel_files = " + str(self.top_corner_adjusted[1]))
			print("bottom_y_pixel_files = " + str( self.top_corner_adjusted[1] + len(self.file_rows)*self.row_height - 1))

	def add_file(self, fname, color):
		#add to the row with the fewest files showing in it
		if "file_display_debug" in globals():
			print("trying to add " + fname + " with color " + str(color))
		min_files = len(self.file_rows[0][1].dictionary)
		adding_row = self.file_rows[0]
		for row in self.file_rows:
			if fname in row[1].dictionary:
				#file is already displayed on this machine
				return
			if (len(row[1].dictionary) < min_files):
				adding_row = row
				min_files = len(row[1].dictionary)
		if "debug" in globals():
			if min_files >= self.max_files_in_row:
				print("Adding a color box that will not display on machine due to space considerations")

		adding_row[1].append(fname, color)
		self.show_row(adding_row)

	def remove_file(self, fname):
		for row in self.file_rows:
			if (fname in row[1].dictionary):
				row[1].remove(fname)
				self.show_row(row)

	def show_row(self, row):
		complete_to_edge = True
		shown = 0
		if(len(row[1].dictionary) <= FULL_FILES_PER_FILEROW):
			file_box_width = MACHINE_WIDTH / FULL_FILES_PER_FILEROW
			if(len(row[1].dictionary) < FULL_FILES_PER_FILEROW):
				complete_to_edge = False
		else:
			file_box_width = max( ( MACHINE_WIDTH / len(row[1].dictionary) ), MIN_FILEROW_FILE_WIDTH)

		#reset this band to take care of any random bands from uneven division
		x1 = self.top_corner_adjusted[0]
		y1 = self.top_corner_adjusted[1] + row[0] * (self.row_height)
		self.draw.rectangle( [x1, y1, x1+self.in_machine_space, y1+self.row_height-1], fill=WHITE)

		to_show = min(len(row[1].dictionary), self.max_files_in_row)
		for fname in row[1].keys:
			x1 = self.top_corner_adjusted[0]+shown*file_box_width
			self.draw.rectangle( [x1, y1, x1 + file_box_width, y1 + self.row_height-1], fill=row[1].dictionary[fname])
			shown = shown + 1
			if( (shown == to_show) ):
				if(complete_to_edge):
					self.draw.rectangle( [x1 + file_box_width, y1, self.top_corner_adjusted[0] + self.in_machine_space, y1 + self.row_height-1], fill=row[1].dictionary[fname])
				break

	def bubble_files_once(self, fileCount):
		for row in self.file_rows:
			i = 0
			while( (i + 1) < len(row[1].keys) ):
				fname_1 = row[1].keys[i]
				fname_2 = row[1].keys[i + 1]
				if fileCount[fname_1].occur_count < fileCount[fname_2].occur_count:
					row[1].swap_key_order(i, i+1)
				i += 1

class Legend(object):
	def __init__(self, draw, fileCount, manager_top_corner, font):
		self.x_min = GIF_WIDTH - LEGEND_WIDTH
		self.x_max = GIF_WIDTH
		self.y_min = manager_top_corner[1] + BUFFER_SPACE + MACHINE_WIDTH / 2
		self.y_max = GIF_HEIGHT - BUFFER_SPACE - TEXT_HEIGHT
		self.font = ImageFont.truetype(FONT_FILE, int(3 * LEGEND_SLOT_HEIGHT/5))
		#add a buffer on top, then each entry will take up height+buffer to include space after the last
		self.max_slots = int( (self.y_max - self.y_min - LEGEND_SLOT_BUFFER) / (LEGEND_SLOT_HEIGHT+LEGEND_SLOT_BUFFER) )
		self.draw = draw
		self.fileCounter = fileCount
		self.slot_start_pixel = (self.x_min + LEGEND_SLOT_BUFFER, self.y_min + LEGEND_SLOT_BUFFER)
		self.slot_width = self.x_max - self.x_min - 2 * LEGEND_SLOT_BUFFER
		self.file_slots = dict()
		self.reopened_slots = []
		self.shown_files = set()
		self.lowest_file_refs = 0

	def debug_display(self):
		self.draw.rectangle( [self.x_min, self.y_min, self.x_max, self.y_max], fill=RED)
		for i in range(0, self.max_slots):
			y_start = self.slot_start_pixel[1] + i * (LEGEND_SLOT_HEIGHT + LEGEND_SLOT_BUFFER)
			self.draw.rectangle( [self.slot_start_pixel[0], y_start, self.slot_start_pixel[0] + self.slot_width, y_start + LEGEND_SLOT_HEIGHT], fill=BLUE)

	def clear_slot(self, index):
		y_start = self.slot_start_pixel[1] + index * (LEGEND_SLOT_HEIGHT + LEGEND_SLOT_BUFFER)
		#clear this slot
		self.draw.rectangle( [self.slot_start_pixel[0], y_start, self.slot_start_pixel[0] + self.slot_width, y_start + LEGEND_SLOT_HEIGHT], fill=WHITE)


	def update(self, fname):
		if not fname:
			return

		if fname in self.shown_files and fname in self.fileCounter:
			#already displayed, still present on some machine
			return
		elif fname not in self.fileCounter:
			if fname in self.shown_files:
				#displayed, but removed from all machines...
				for index in self.file_slots:
					shown_name = self.file_slots[index]
					if shown_name == fname:
						self.reopened_slots.append(index)
						self.clear_slot(index)
						self.file_slots.pop(index)
						break
				self.shown_files.remove(fname)
				return

			else:
				return

		#if not returned yet, fname is in fileCounter but is not shown
		if len(self.file_slots) < self.max_slots:
			if len(self.reopened_slots) > 0:
				index = self.reopened_slots.pop()
			else:
				index = len(self.file_slots)
			self.file_slots[index] = fname
			self.show_file_in_slot(fname, index)
			self.shown_files.add(fname)
		else:
			#if its not referenced more than our estimate lowest references, don't bother checking against
			#every slot
			new_count = self.fileCounter[fname].occur_count
			if new_count > self.lowest_file_refs:
				#either lowest_file_refs is out of date, or we'll end up replacing a file
				#no matter, lowest_file_refs will be updated
				low_refs_index = list(self.file_slots.keys())[0]
				low_refs_name = self.file_slots[low_refs_index]
				low_refs = self.fileCounter[low_refs_name].occur_count
				for index in self.file_slots:
					shown_name = self.file_slots[index]
					if self.fileCounter[shown_name].occur_count < low_refs:
						low_refs = self.fileCounter[shown_name].occur_count
						(low_refs_name, low_refs_index) = (shown_name, index)

				if new_count > low_refs:
					#replace low_refs file
					self.file_slots[low_refs_index] = fname
					self.show_file_in_slot(fname, low_refs_index)
					self.shown_files.discard(low_refs_name)
					self.shown_files.add(fname)

				#this is fine, although perhaps not perfectly accurate if a replacement occurs
				self.lowest_file_refs = low_refs

	def show_file_in_slot(self, file_name, index):
		self.clear_slot(index)
		fill_color = self.fileCounter[file_name].color

		y_start = self.slot_start_pixel[1] + index * (LEGEND_SLOT_HEIGHT + LEGEND_SLOT_BUFFER)
		self.draw.rectangle([self.slot_start_pixel[0], y_start, self.slot_start_pixel[0] + LEGEND_SLOT_HEIGHT, y_start + LEGEND_SLOT_HEIGHT], fill = fill_color)

		#TODO get this back on the screen
		#write the file name as text, possibly off the screen
		self.draw.text((self.slot_start_pixel[0] + LEGEND_SLOT_HEIGHT, y_start+(LEGEND_SLOT_HEIGHT/5) ), " "+file_name, font=self.font, fill=BLACK)

class Color_List(object):
	def __init__(self, size):
	#	self.color_gen = ColorGen()
		self.colors = [(240, 248, 255), (250, 235, 215), (0, 255, 255), (127, 255, 212), (240, 255, 255), (245, 245, 220), (255, 228, 196), (255, 235, 205), (0, 0, 255), (138, 43, 226), (165, 42, 42), (222, 184, 135), (95, 158, 160), (127, 255, 0), (210, 105, 30), (255, 127, 80), (100, 149, 237), (255, 248, 220), (220, 20, 60), (0, 255, 255), (0, 0, 139), (0, 139, 139), (184, 134, 11), (169, 169, 169), (0, 100, 0), (189, 183, 107), (139, 0, 139), (85, 107, 47), (255, 140, 0), (153, 50, 204), (139, 0, 0), (233, 150, 122), (143, 188, 143), (72, 61, 139), (47, 79, 79), (0, 206, 209), (148, 0, 211), (255, 20, 147), (0, 191, 255), (105, 105, 105), (30, 144, 255), (178, 34, 34), (255, 250, 240), (34, 139, 34), (255, 0, 255), (220, 220, 220), (248, 248, 255), (255, 215, 0), (218, 165, 32), (128, 128, 128), (0, 128, 0), (173, 255, 47), (240, 255, 240), (255, 105, 180), (205, 92, 92), (75, 0, 130), (255, 255, 240), (240, 230, 140), (230, 230, 250), (255, 240, 245), (124, 252, 0), (255, 250, 205), (173, 216, 230), (240, 128, 128), (224, 255, 255), (250, 250, 210), (211, 211, 211), (144, 238, 144), (255, 182, 193), (255, 160, 122), (32, 178, 170), (135, 206, 250), (119, 136, 153), (176, 196, 222), (255, 255, 224), (0, 255, 0), (50, 205, 50), (250, 240, 230), (255, 0, 255), (128, 0, 0), (102, 205, 170), (0, 0, 205), (186, 85, 211), (147, 112, 219), (60, 179, 113), (123, 104, 238), (0, 250, 154), (72, 209, 204), (199, 21, 133), (25, 25, 112), (245, 255, 250), (255, 228, 225), (255, 228, 181), (255, 222, 173), (0, 0, 128), (253, 245, 230), (128, 128, 0), (107, 142, 35), (255, 165, 0), (255, 69, 0), (218, 112, 214), (238, 232, 170), (152, 251, 152), (175, 238, 238), (219, 112, 147), (255, 239, 213), (255, 218, 185), (205, 133, 63), (255, 192, 203), (221, 160, 221), (176, 224, 230), (128, 0, 128), (102, 51, 153), (255, 0, 0), (188, 143, 143), (65, 105, 225), (139, 69, 19), (250, 128, 114), (244, 164, 96), (46, 139, 87), (255, 245, 238), (160, 82, 45), (192, 192, 192), (135, 206, 235), (106, 90, 205), (112, 128, 144), (255, 250, 250), (0, 255, 127), (70, 130, 180), (210, 180, 140), (0, 128, 128), (216, 191, 216), (255, 99, 71), (64, 224, 208), (238, 130, 238), (245, 222, 179), (245, 245, 245), (255, 255, 0), (154, 205, 50)]
		self.length = len(self.colors) #size
		#TODO get rid of hardcoded 10
		self.get_more_size = 10

	def pop(self):
		try:
			color = self.colors.pop(0)
		except:
			color = BLACK
			if "debug" in globals():
				print("Trying to pop empty color list")

		return color

	def append(self, color):
		self.colors.append(color)



class Machine(object):
	def __init__(self, name, machine_type, ip, top_left_corner, fileCount, draw, legend, width, height, resources):
		self.name = name
		self.ip = ip
		self.tasks = dict() #key is task_no, value is Task_Info instance
		self.files = []
		self.last_touched = None
		self.fileCount = fileCount
		self.is_visible = False
		self.machine_type = machine_type
		self.pending_transfer_to_manager = None
		self.pending_transfer_to_worker = None
		self.legend = legend
		self.draw = draw
		self.width = width
		self.height = height
		self.top_left_corner = top_left_corner
		self.connection_point = ( (top_left_corner[0] + self.width), (top_left_corner[1] + self.height/2) )
		self.file_display = Machine_File_Display(draw, top_left_corner, resources)
		self.task_display = Machine_Task_Display(draw, top_left_corner, resources)
		self.resources = resources

	def update_tasks(self, font, line, log, color_list):
		line_task_inf = get_task_from_line(line)
		if(line_task_inf):
			if(line_task_inf.status == "assigned"):
				self.tasks[line_task_inf.num] = line_task_inf
				line_task_inf.set_info(log, self.fileCount, self.ip, color_list, self.legend)
				self.task_display.add_task(self.ip, line_task_inf)
				return True

			#line_task_inf is a partially full Task_Info, so update the status of the one we know
			self.tasks[line_task_inf.num].status = line_task_inf.status

			task_inf = self.tasks[line_task_inf.num]

			if(task_inf.status == "result"):
				#task is done running
				#TODO check for kill before completion
				self.task_display.finish_task(self.ip, task_inf.num)
				for outfile in task_inf.outfiles:
					if "task_display_debug" in globals():
						print("trying to add " + outfile + " to the display")
					#if file is used in task, it will already be in fileCount
					self.file_display.add_file(outfile, self.fileCount[outfile].color)
					self.files.append(outfile)

			elif(task_inf.status == "removed"):
				self.task_display.remove_task(self.ip, task_inf.num)

			return True
		else:
			return False

	def highlight(self, color):
		top_left = self.top_left_corner
		self.draw.line((top_left[0], top_left[1], top_left[0], top_left[1] + self.height), width = MACHINE_BORDER_WIDTH, fill=color)
		self.draw.line((top_left[0], top_left[1]+self.height, top_left[0]+self.width, top_left[1] + self.height), width = MACHINE_BORDER_WIDTH, fill=color)
		self.draw.line((top_left[0]+self.width, top_left[1]+self.height, top_left[0]+self.width, top_left[1]), width = MACHINE_BORDER_WIDTH, fill=color)
		self.draw.line((top_left[0]+self.width, top_left[1], top_left[0], top_left[1]), width = MACHINE_BORDER_WIDTH, fill=color)

	def update_network_communications(self, line, color_list, fileCount):
		f_hash_name = "file-[0-9a-fA-F]*-([^ ]*)"

		pattern_action_direction = [("needs file .* as "+f_hash_name, "(needs file)", "to_worker"), (": put "+f_hash_name, "put", "to_worker"), ("\) received", "received", "to_worker"), (": get " + f_hash_name, "get", "to_manager"), (": file "+f_hash_name, "file", "to_manager"), ("Receiving file ([^ ]*)", "receiving", "to manager"), (": end", "end", "to manager"), ("will try up to ([0-9]*) seconds to transfer", "set timeout", None)] #add looking for timeout into this thing  #also need to add for returning output files because they're different
		timestamp = line.split(" ")[1]

		#patterns catch any important things such as filename if present
		ft_info = None
		for item in pattern_action_direction:
			matched = re.search(item[0], line)
			if(matched):
				#handle what to give FT_Info constructor from matching based on p_a_d[1]
				if(item[1] not in ("end", "set timeout", "received") ):
					ft_info = FT_Info(matched.group(1), item[1], item[2], None)

				elif(item[1] == "set timeout"):
					if(self.pending_transfer_to_manager and not self.pending_transfer_to_worker):
						ft_info = FT_Info(None, item[1], item[2], matched.group(1))
					elif(self.pending_transfer_to_worker and not self.pending_transfer_to_manager):
						ft_info = FT_Info(None, item[1], item[2], matched.group(1))
				else:
					ft_info = FT_Info(None, item[1], item[2], None)

				break

		if(ft_info):
			self.last_touched = ft_info.fname
			if(ft_info.direction == "to_manager"):
				pass

			if(ft_info.direction == "to_worker"):
				pend_trans_to_worker = self.pending_transfer_to_worker
				if(pend_trans_to_worker):
					name = pend_trans_to_worker.fname
					if(name in fileCount):
						fileCount[name].occur_count = fileCount[name].occur_count + 1
					else:
						fileCount[name] = File_Distribution_Info(self.ip, color_list)

					if(ft_info.state == "received"):
						if(ft_info.fname not in self.files):
							self.file_display.add_file(pend_trans_to_worker.fname, fileCount[name].color)
							self.files.append(pend_trans_to_worker.fname)
						self.pending_transfer_to_worker = None
					else:
						if(ft_info.fname):
							if(ft_info.fname != pend_trans_to_worker.fname):
								pass
							else:
								pend_trans_to_worker.fname = ft_info.fname
						if(ft_info.expiration):
							pend_trans_to_worker.expiration = ft_info.expiration

						#state will never be type None
						pend_trans_to_worker.state = ft_info.state

				else:
					#brand new, state should be using
					self.pending_transfer_to_worker = ft_info
					name = self.pending_transfer_to_worker.fname
					if(name):
						if(name in fileCount):
							fileCount[name].occur_count = fileCount[name].occur_count + 1
						else:
							fileCount[name] = File_Distribution_Info(self.ip, color_list)

				return True
			else:
				return False

	def unlink_files(self, line, color_list, fileCount):
		match = re.search(": unlink file-[0-9a-fA-F]*-([^ ]*)", line)
		if(match):
			fname = match.group(1)
			if(fname in self.files):
				#should always be if removing, but could have partial log
				if(fname in fileCount):
					#should always be if removing, but could be partial log
					fileCount[fname].ip_set.remove(self.ip)
					if( len(fileCount[fname].ip_set) <= 0):
						removed_file_distr_info = fileCount.pop(fname)
						color_list.append(removed_file_distr_info.color)
				self.last_touched = fname
				self.files.remove(fname)
				self.file_display.remove_file(fname)
			return True
		else:
			return False

	def initial_name_check(self, line):
		if self.name == None:
			regex_match = re.search("wq: rx from unknown \(.*\): ([^ ]*) ([^ ]*) ([^ ]*) .*", line)
			if(regex_match):
				self.name = regex_match.group(3)
				return True
			else:
				return False
		else:
			#already has a name
			return False

	def update_connection_status(self, line):
		#if the line is tcp or giving the name of the machine, do something about it
		regex_match = re.search("tcp: got connection", line)
		if(regex_match):
			#coloring taken care of in main
			self.is_visible = True
			return True

		regex_match = re.search("tcp: disconnected", line)
		if(regex_match):
			if "tcp_debug" in globals():
				print("expect an invisible machine")
			#coloring taken care of in main
			self.is_visible = False
			return True

		return False

	def display_attributes(self):
		print(self.name + "/" + self.ip)
		for task in self.tasks:
            print("\tTask: {}:{}\n".format(task, self.tasks[task]))
		for f in self.files:
			print("\tFile: {}\n".format(f))

	def get_location(self):
		return self.location

	def bubble_files(self):
		self.file_display.bubble_files_once(self.fileCount)

class Connections(object):
	def __init__(self):
		self.max_vertical_connections = (GIF_WIDTH - BUFFER_SPACE) / (MACHINE_WIDTH + 2 * BUFFER_SPACE)
		self.connections = []
		i = 0;
		while (i < self.max_vertical_connections):
			self.connections.append(Connection(i))
			i = i + 1
		self.connections = tuple(self.connections)

	def add(self, draw, machine_resources):
		added = False
		i = 0
		machine_height = machine_resources.cores.largest * RESOURCE_ROW_HEIGHT + NUM_FILE_ROWS_IN_MACHINE * RESOURCE_ROW_HEIGHT + MACHINE_BORDER_WIDTH
		#TODO find connection with minimum fragmentation to add the machine to, rather than just one it fits in
		while (i < self.max_vertical_connections and not added):
			if self.connections[i].can_add_machine(machine_height):
				top_left_corner = self.connections[i].add(draw, machine_height)
				added = True

			i = i + 1

		if not added and "debug" in globals():
			print("Out of space :(")
			return None
		else:
			return (top_left_corner, MACHINE_WIDTH, machine_height)

class Connection(object):
	def __init__(self, grid_x):
		self.total_connections = 0
		self.x_pixel = grid_x * (MACHINE_WIDTH + 2 * BUFFER_SPACE) + MACHINE_WIDTH + 2 * BUFFER_SPACE
		self.top_y_pixel = 2 * BUFFER_SPACE + MACHINE_WIDTH  #Master and buffer above and below it
		self.bottom_y_pixel = GIF_HEIGHT - (TEXT_HEIGHT) #all added workers will have a length in pixel and then a buffer space below them
		self.free_pixels_list = [(self.top_y_pixel, self.bottom_y_pixel)]
		if "tcp_debug" in globals():
			print("On connection __init__: Free pixels list = " + str(self.free_pixels_list))

	def find_minimum_fragmentation(self, machine_height):
		#find the best fit hole in the list and return the fragmentation in pixels
		#if I can put it somewhere and putting it there causes no hole to shrink below the size of a 1 core 2 file-row machine, do it
		#otherwise minimize external fragmentation by picking the one that leaves the smallest hole
		space_needed = machine_height + BUFFER_SPACE
		minimum_fragmentation = BASIC_MACHINE_SPACE + BUFFER_SPACE + 1  #more than any fragmentation will ever be
		for space in self.free_pixels_list:
			fragmentation = (space[1] - space[0]) - space_needed
			if( fragmentation >= 0 ):
				#it would fit in this free space, how much fragmentation would it cause?
				if(fragmentation >= BASIC_MACHINE_SPACE + BUFFER_SPACE):
					#it would leave a hole large enough for another machine to fit, so no fragmentation yet
					fragmentation = 0

				if(fragmentation < minimum_fragmentation):
					minimum_fragmentation = fragmentation

		return minimum_fragmentation

	def find_minimum_fragmentation_location(self, machine_height):
		#find the best fit hole in the list and return the hole
		#if I can put it somewhere and putting it there causes no hole to shrink below the size of a 1 core 2 file-row machine, do it
		#otherwise minimize external fragmentation by picking the one that leaves the smallest hole
		space_needed = machine_height + BUFFER_SPACE
		minimum_fragmentation = BASIC_MACHINE_SPACE + BUFFER_SPACE + 1  #more than any fragmentation will ever be
		minimum_fragmentation_location = None
		i = 0
		while(i < len(self.free_pixels_list)):
			space = self.free_pixels_list[i]
			fragmentation = (space[1] - space[0]) - space_needed
			if( fragmentation >= 0 ):
				#it would fit in this free space, how much fragmentation would it cause?
				if(fragmentation >= BASIC_MACHINE_SPACE + BUFFER_SPACE):
					#it would leave a hole large enough for another machine to fit, so no fragmentation yet
					fragmentation = 0

				if(fragmentation < minimum_fragmentation):
					minimum_fragmentation = fragmentation
					minimum_fragmentation_location = i
			i = i + 1
		if "tcp_debug" in globals():
			print(minimum_fragmentation_location)

		return minimum_fragmentation_location

	def can_add_machine(self, machine_height):
		space_needed = machine_height + BUFFER_SPACE
		for space in self.free_pixels_list:
			if( (space[1] - space[0]) > space_needed ):
				return True

		return False

	def add(self, draw, machine_height):
		if "tcp_debug" in globals():
			print("On entering connection add(): Free pixels list = " + str(self.free_pixels_list))
		if not self.can_add_machine(machine_height) and ("tcp_debug" in globals()):
			print("Can't add to this connection, overfull")
			return None
		else:
			space_needed = machine_height + BUFFER_SPACE

			#if this connection wire just got its first machine
			if(self.total_connections == 0):
				draw.line((self.x_pixel, self.top_y_pixel, self.x_pixel, self.bottom_y_pixel), BLACK, width = CONNECTION_WIDTH)

			insert_machine_hole_index = self.find_minimum_fragmentation_location(machine_height)
			if "tcp_debug" in globals():
				print("insert_machine_hole_index is: " + str(insert_machine_hole_index))
			old_pixels_hole = self.free_pixels_list[insert_machine_hole_index]
			if "tcp_debug" in globals():
				print("old_pixels_hole is: " + str(old_pixels_hole))
			new_machine_top_y_pixel = old_pixels_hole[0]
			new_machine_left_x_pixel = self.x_pixel - (MACHINE_WIDTH + BUFFER_SPACE)

			#update the free pixels list
			if( (old_pixels_hole[1] - old_pixels_hole[0] - space_needed) == 0):
				if "tcp_debug" in globals():
					print("perfect fit! pop the hole")
				self.free_pixels_list.pop(insert_machine_hole_index)
				if "tcp_debug" in globals():
					print("Connection added: Free pixels list = " + str(self.free_pixels_list))
			else:
				#insert at the top of the hole
				new_pixels_hole = ( (old_pixels_hole[0] + space_needed), old_pixels_hole[1] )
				if "tcp_debug" in globals():
					print("new_pixels_hole is: " + str(new_pixels_hole))
				self.free_pixels_list[insert_machine_hole_index] = new_pixels_hole
				if "tcp_debug" in globals():
					print("Connection added: Free pixels list = " + str(self.free_pixels_list))

			self.total_connections = self.total_connections + 1
			return  (new_machine_left_x_pixel, new_machine_top_y_pixel)  #the slot location
	'''
	#as of now, we are not removing machines because they just become invisible on tcp disconnect
	#if we were to remove machines, we would have to change self.free_pixels_list to extend, and perhaps combine with another hole, one of the holes that the machine was adjacent to if it was adjacent to a hole. If it was not adjacent to a hole, we'd have to create a new hole. The size of pixels added would be the machine's height plus BUFFER_SPACE. Not sure where the handle to the machine's height would be, but we could probably do all of this with a handle on the machine we're removing, as we have the x and y coordinates of its top left corner, and thus the x coordinate of its connection branch. We also know its height.
	def remove(self, slot_number):
		self.occupied_vector = False
		self.slots_remaining = self.slots_remaining + 1

		#if the connection wire is now out of machines
		if(self.slots_remaining == self.num_slots):
			draw.line((self.x_pixel, self.top_y_pixel, self.x_pixel, self.bottom_y_pixel), WHITE, width = CONNECTION_WIDTH)
	'''

def main():

	if(len(sys.argv) != 3):
		print("Usage: "+sys.argv[0]+" <debugFileName> <outputFileName>")
		sys.exit(0)
	try:
		logToRead = open(sys.argv[1], 'r')
	except:
		print("Could not open " + sys.argv[1] + " for reading: ")
		sys.exit(0)

	if os.path.isfile(sys.argv[2]+".gif"):
		print("Could not create " + sys.argv[2] + ".gif to write final movie to, file already exists")
		sys.exit(0)

	dirname = sys.argv[2]+"_frames"


	if(os.system("mkdir "+dirname) != 0):
		print("Could not create a temporary file storage directory " + dirname +".")
		sys.exit()

	numFrames = 0

	currentImage = Image.new('RGB', (GIF_WIDTH,GIF_HEIGHT), WHITE)
	draw = ImageDraw.Draw(currentImage)

	color_list = Color_List(NUM_STARTING_COLORS) #[RED, BLACK, BLUE, GREEN, PURPLE, LIGHT_BLUE] #color_generator.get_new_colors[NUM_STARTING_COLORS]
	workers = dict()  #key is ip, value is worker object
	connections = Connections()
	fileCounter = dict() #key is filename, value is File_Distrib_Info
	workers_needing_resource_reports = set() #workers not yet displayed because they've not yet told us their size

	if "count_ips" in globals():
		ips = set()

	manager = add_machine(draw, "manager", "", connections, workers, fileCounter, None, None)
	manager.highlight(RED)
	add_manager_flair(draw, manager)

	legend = Legend(draw, fileCounter, manager.top_left_corner, FONT_FILE)

	currentImage.save(sys.argv[2]+".gif", "GIF")
	numFrames = numFrames + 1

	last_appended_frame = 0
	append_threshold = GIF_APPEND_THRESHOLD
	font = ImageFont.truetype(FONT_FILE, int(TEXT_HEIGHT * 1/float(3)) )

	while(True):
		line = logToRead.readline()
		if line == '':
			break
		line = line.rstrip('\n')

		ip = find_ip(line)
		if "count_ips" in globals():
			ips.add(ip)
			print("Length of ips is: " + str(len(ips)))

		if(manager.ip == "" and not ip):
			result = re.search("dns: ([^ ]*) is ([0-9\.]*)", line)
			if(result):
				manager.name = result.group(1)
				manager.ip = result.group(2)

		if(ip not in workers and ip != None):
			if ip in workers_needing_resource_reports:
				if "debug" in globals():
					print("about to read resource report,   ip: " + str(ip))
					print("about to read resource report, line: " + line)
				resources = read_resource_report(line, logToRead)
				if "debug" in globals():
					print("read resource report")

				if resources != None:
					workers_needing_resource_reports.remove(ip)
					this_worker = add_machine(draw, "worker", ip, connections, workers, fileCounter, legend, resources)
					this_worker.highlight(BLACK)
					this_worker.is_visible = True
					color_connection(draw, manager, this_worker, BLACK)
			else:
				workers_needing_resource_reports.add(ip)

		if( (ip != None) and (ip in workers) ):
			this_worker = workers[ip]

			line_type = line.split(" ")[3][:-1]
			useful_line = False
			if(line_type == "wq"):
			#check the line for a variety of different, specific regexs to determine if line is useful
				useful_line = this_worker.update_tasks(font, line, logToRead, color_list)
				if (not useful_line):
					useful_line = this_worker.initial_name_check(line)
					if (not useful_line):
						useful_line = this_worker.update_network_communications(line, color_list, fileCounter)
						if(not useful_line):
							useful_line = this_worker.unlink_files(line, color_list, fileCounter)

			if (line_type == "tcp"):
				useful_line = this_worker.update_connection_status(line)

			if(useful_line):
				if "debug_machine" in globals():
					this_worker.display_attributes()

				#referenced once in a useful way, bubble sort a single round
				this_worker.bubble_files()

				color_connection(draw, manager, this_worker, RED)
				this_worker.highlight(RED) #highlight_machine(draw, this_worker, RED)
				fill_in_text(draw, font, line)
				legend.update(this_worker.last_touched)
				padded_nframes = pad(numFrames, append_threshold, last_appended_frame)

				currentImage.save(dirname+"/frame_"+padded_nframes+".gif", "GIF")
				if (numFrames - last_appended_frame  >= append_threshold):
					if "gif_debug" in globals():
						print("gifsicle --delay=" + str(FRAME_DELAY) + " --loop ./"+dirname+"/*.gif > tmp_"+padded_nframes+".gif")
					os.system("gifsicle --delay=" + str(FRAME_DELAY) +" --loop ./"+dirname+"/*.gif > tmp_"+padded_nframes+".gif")
					if "gif_debug" in globals():
						print("gifsicle --batch "+sys.argv[2]+".gif --delay=" + str(FRAME_DELAY) +" --loop --append tmp_"+padded_nframes+".gif")
					os.system("gifsicle --batch "+sys.argv[2]+".gif --delay=" + str(FRAME_DELAY) + " --loop --append tmp_"+padded_nframes+".gif")
					os.system("rm tmp_"+padded_nframes+".gif ./"+dirname+"/*")
					last_appended_frame = numFrames

				numFrames = numFrames + 1
				if(this_worker.is_visible):
					this_worker.highlight(BLACK)#highlight_machine(draw, this_worker, BLACK)
				else:
					if "tcp_debug" in globals():
						print("invisible worker in frame numFrames")
					this_worker.highlight(WHITE)#highlight_machine(draw, this_worker, WHITE)

				color_connection(draw, manager, this_worker, BLACK)

				clear_text_box(draw)
			else:
				if "show_unused" in globals():
					print(line)
		else:
			if "show_unused" in globals():
				print(line)
	if "gif_debug" in globals():
		print("Final gif append")
	os.system("gifsicle --delay=" + str(FRAME_DELAY) + " --loop ./"+dirname+"/*.gif > tmp.gif")
	os.system("gifsicle --batch "+sys.argv[2]+".gif --append tmp.gif")
	os.system("rm ./"+dirname+"/ -r")
	os.system("rm tmp.gif")

if __name__ == "__main__":
	main()
