#Author: Ryan Boccabella for CCL, (C) University of Notre Dame 2015
#A tool that reads in debug logs from work_queue and keeps the state of the work_queue
#workers and their communications with the manager in an image format, combining the
#images into a gif video for visual debugging of both a single run of work_queue, as
#well as for looking for hiccups/areas of improvement to work_queue itself

from PIL import Image, ImageDraw, ImageFont
import re
import sys
import subprocess
import os
import math
#from colorGen import ColorGen
#from collections import namedtuple

#debug = 1
#show_unused = 1
#count_ips = 1
#tcp_debug = 1
#task_debug = 1
#debug_machine = 1
#task_info_debug = 1
#file_display_debug = 1

#variables in pixels
MACHINE_WIDTH = 40
BUFFER_SPACE = 10
GIF_WIDTH = 1024
GIF_HEIGHT = 512
TEXT_HEIGHT = 32
LEGEND_WIDTH = int(GIF_WIDTH / 5.5)
CONNECTION_WIDTH = 2
MACHINE_BORDER_WIDTH = 2
FULL_FILES_PER_FILEROW = 3
MIN_FILEROW_FILE_WIDTH = 5
NUM_FILE_ROWS_IN_MACHINE = 2
NUM_TASK_ROWS_IN_MACHINE = 2
LEGEND_SLOT_HEIGHT = 15
LEGEND_SLOT_BUFFER = 8

#Other variables
FONT_FILE = "cour.ttf"
GIF_APPEND_THRESHOLD = 500
FRAME_DELAY = 1

NUM_STARTING_COLORS = 100
WHITE = (255,255,255)
BLACK = (0,0,0)
RED = (200, 0, 0)
BLUE = (0,0,200)
GREEN = (0,200,0)
PURPLE = (100,0,100)
LIGHT_BLUE = (0, 0, 235)

def add_manager_flair(draw, manager):
	start_point = find_machine_connection_line_pixel(manager.grid_location)
	draw.line( (start_point[0], start_point[1], GIF_WIDTH - LEGEND_WIDTH - BUFFER_SPACE, start_point[1]), BLACK, width = CONNECTION_WIDTH)
	(text_x, text_y) = find_machine_location_pixel(manager.grid_location)
	text_y = text_y + MACHINE_WIDTH/4
	text_x = text_x + MACHINE_WIDTH/4
	font = ImageFont.truetype(FONT_FILE, MACHINE_WIDTH/2)
	draw.text((text_x, text_y), "M", font=font, fill=RED)

def color_connection(draw, manager, worker, color):
	draw_connection_on_image(draw, find_machine_connection_line_pixel(worker.grid_location), color)

	m_location = find_machine_connection_line_pixel(manager.grid_location)
	w_location = find_machine_connection_line_pixel(worker.grid_location)

	vert_x = w_location[0] + BUFFER_SPACE
	vert_y_1 = m_location[1]
	vert_y_2 = w_location[1]

	horz_x_1 = m_location[0]
	horz_x_2 = vert_x
	horz_y = vert_y_1

	draw.line((vert_x, vert_y_1, vert_x, vert_y_2), color, width = CONNECTION_WIDTH)
	draw.line((horz_x_1, horz_y, horz_x_2, horz_y), color, width = CONNECTION_WIDTH)

def find_machine_location_pixel(grid_location):
	x = BUFFER_SPACE + grid_location[0] * (MACHINE_WIDTH + 2 * BUFFER_SPACE)
	y = 2 * BUFFER_SPACE + MACHINE_WIDTH + grid_location[1] * (MACHINE_WIDTH + BUFFER_SPACE)
	return (x, y)

def find_machine_connection_line_pixel(grid_location):
	(x, y) = find_machine_location_pixel(grid_location)
	x = x + MACHINE_WIDTH
	y = y + MACHINE_WIDTH / 2
	return (x, y)

def draw_square(drawObj, top_left, width, length, color):
	drawObj.line((top_left[0], top_left[1], top_left[0], top_left[1] + length), width = width, fill=color)
	drawObj.line((top_left[0], top_left[1]+length, top_left[0]+length, top_left[1] + length), width = width, fill=color)
	drawObj.line((top_left[0]+length, top_left[1]+length, top_left[0]+length, top_left[1]), width = width, fill=color)
	drawObj.line((top_left[0]+length, top_left[1], top_left[0], top_left[1]), width = width, fill=color)

def find_ip(line):
	IP_regex = re.compile("([0-2]?[0-9]?[0-9]\.){3}[0-2]?[0-9]?[0-9]")
	found_ip = IP_regex.search(line)
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

def add_machine_to_image(draw, machine):
	if(machine.machine_type == "manager"):
		color = RED
	else:
		color = BLACK
	draw_square(draw, find_machine_location_pixel(machine.grid_location), MACHINE_BORDER_WIDTH, MACHINE_WIDTH, color)

def highlight_machine(draw, machine, color):
	draw_square(draw, find_machine_location_pixel(machine.grid_location), MACHINE_BORDER_WIDTH, MACHINE_WIDTH, color)


#create machine, add it to image, and to image awareness (connections)
def add_machine(draw, machine_type, ip, connections, workers, fileCount, legend=None):
	if(machine_type == "manager"):
		this_machine = Machine("Master", "manager", ip, (0, -1), fileCount, draw, legend)

	elif(machine_type == "worker"):
		grid_loc = connections.add(draw)
		if(not grid_loc):
			return
		worker_name = ""
		this_machine = Machine(worker_name, "worker", ip, grid_loc, fileCount, draw, legend)
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




class FT_Info(object):
	def __init__(self, fname, state, direction, expiration):
		self.state = state
		self.direction = direction
		self.fname = fname
		self.expiration = expiration

class Task_Info(object):
	def __init__(self, num=None, status=None):
		self.num = num
		self.status = status
		self.command_num = None
		self.exe = None
		self.command_string = None
		self.infiles = set() #may not need this one
		self.outfiles = set()

	def set_info(self, log, fileCount, ip, color_list, legend):
		if "task_info_debug" in globals():
			print("-------------------------------------------------\nIn set_info:\n")

		need_to_read = True
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

			matched = re.search(": end", recent_line)
			if(matched):
				need_to_read = False

			#if we've not continued yet, and we've seen an infile, we're done reading
			#if we've not seen an infile, the line was ": cores", ": disk", ": gpus", or something similar and we want to keep reading
			#if(has_infile):
				need_to_read = False

		#unread that line
		#log.seek(-len(recent_line), 1)
		if "task_info_debug" in globals():
			#re_read = log.readline()
			#print "Unread recent line: " + re_read
			#log.seek(-len(recent_line), 1)
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
	def __init__(self, draw, grid_location):
		from_corner_to_interior =  MACHINE_BORDER_WIDTH / 2
		self.in_machine_space = MACHINE_WIDTH - 2 * from_corner_to_interior  #width of bar needs integer division, then doubled
		self.draw = draw
		self.task_rows = []
		i = 0
		while i < NUM_TASK_ROWS_IN_MACHINE:
			self.task_rows.append( (i, []) )
			i = i + 1

		self.process_radius = int( (MACHINE_WIDTH / 2 - from_corner_to_interior) / (3 * len(self.task_rows) + 1) )
		self.max_small_processes_in_row = int( (self.in_machine_space - self.process_radius) / (3 * self.process_radius) )
		top_corner = find_machine_location_pixel(grid_location)
		self.top_corner_adjusted = (top_corner[0] + from_corner_to_interior, top_corner[1] + from_corner_to_interior+ (MACHINE_WIDTH/2))
		self.row_height = (self.in_machine_space / 2) / NUM_TASK_ROWS_IN_MACHINE

	def add_task(self, ip, task_no):
		#might expand to include command or task #, but not yet
		#idealistically process mapping to a color.... see Machine_File_Display object
		min_tasks = len(self.task_rows[0][1])
		adding_row = self.task_rows[0]
		for row in self.task_rows:
			if(len(row[1]) < min_tasks):
				adding_row = row
				min_tasks = len(row[1])

		if "task_debug" in globals():
			print(ip + " adding task to " + str(adding_row))

		adding_row[1].append( (task_no, "running") )
		if "task_debug" in globals():
			print("The row is now {}\n".format(adding_row))

		self.show_row(adding_row)

	def finish_task(self, ip, task_no):
		for row in self.task_rows:
			if "task_debug" in globals():
				print(ip + " trying to finish task " + str(task_no) + " in " + str(row))
			i = 0
			while i < len(row[1]):
				if task_no == row[1][i][0]:
					row[1][i] = (task_no, "finished")
					self.show_row(row)
					if "task_debug" in globals():
						print("finished")
					return

		if "task_debug" in globals():
			print(ip + " failed to finish task " + str(task_no) + " in " + str(row))

	def remove_task(self, ip, task_no):
		#max_tasks = len(self.task_rows[0][1])
		#removing_row = self.task_rows[0]
		for row in self.task_rows:
			if "task_debug" in globals():
				print(ip + " trying to remove task " + str(task_no) + " from " + str(row))
			i = 0
			while i < len(row[1]):
				if task_no == row[1][i][0]:
					row[1].pop(i)
					self.show_row(row)
					if "task_debug" in globals():
						print("removed")
					return

		if "task_debug" in globals():
			print(ip + " failed to remove task " + str(task_no) + " from " + str(row))

		#	if(len(row[1]) > max_tasks):
		#		removing_row = row
		#		max_tasks = len(row[1])

		#if max_tasks == 0:
		#	if "debug" in globals():
		#		print "tried to remove process from machine that didn't have said process"
		#		return

		#remove 1 process
		#removing_row[1].pop()
		#self.show_row(row)

	def show_row(self, row):
		#clear row
		x1 = self.top_corner_adjusted[0]
		y1 = self.top_corner_adjusted[1] + row[0] * self.row_height
		self.draw.rectangle( [x1, y1, x1+self.in_machine_space, y1+self.row_height], fill=WHITE)

		num_to_draw = len(row[1])
		if len(row[1]) > self.max_small_processes_in_row:
			num_to_draw = self.max_small_processes_in_row

		x1 += self.process_radius
		y1 += self.process_radius
		i = 0
		while i < num_to_draw:
			x = x1 + (i * self.process_radius)
			if(row[1][i][1] == "running"):
				color = BLUE
			else:
				color = RED

			self.draw.pieslice([x, y1, x + 2 * self.process_radius, y1 + 2 * self.process_radius], 0, 360, fill = color)
			i += 1

class Machine_File_Display(object):
	def __init__(self, draw, grid_location):
		from_corner_to_interior =  MACHINE_BORDER_WIDTH / 2
		self.in_machine_space = MACHINE_WIDTH - 2 * from_corner_to_interior  #width of bar needs integer division, then doubled
		self.draw = draw
		self.file_rows = []
		for i in range(NUM_FILE_ROWS_IN_MACHINE):
			self.file_rows.append( (i, o_dict()) )

		self.max_files_in_row = int(self.in_machine_space / MIN_FILEROW_FILE_WIDTH)
		self.max_filebox_width = int(self.in_machine_space / FULL_FILES_PER_FILEROW)
		self.row_height = (self.in_machine_space / 2) / NUM_FILE_ROWS_IN_MACHINE
		top_corner = find_machine_location_pixel(grid_location)
		self.top_corner_adjusted = (top_corner[0] + from_corner_to_interior, top_corner[1] + from_corner_to_interior)

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
		#if "debug" in globals():
		#	print "in show_row, row is: "+str(row)

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
		y1 = self.top_corner_adjusted[1] + row[0] * (self.row_height + 1)
		self.draw.rectangle( [x1, y1, x1+self.in_machine_space, y1+self.row_height], fill=WHITE)

		to_show = min(len(row[1].dictionary), self.max_files_in_row)
		for fname in row[1].keys:
			x1 = self.top_corner_adjusted[0]+shown*file_box_width
			y1 = self.top_corner_adjusted[1]+row[0]*(self.row_height + 1)
			self.draw.rectangle( [x1, y1, x1 + file_box_width, y1 + self.row_height], fill=row[1].dictionary[fname])
			shown = shown + 1
			if( (shown == to_show) ):
				if(complete_to_edge):
					self.draw.rectangle( [x1 + file_box_width, y1, self.top_corner_adjusted[0] + self.in_machine_space, y1 + self.row_height], fill=row[1].dictionary[fname])
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
	def __init__(self, draw, fileCount, manager_loc, font):
		self.x_min = GIF_WIDTH - LEGEND_WIDTH
		self.x_max = GIF_WIDTH
		self.y_min = find_machine_location_pixel(manager_loc)[1] + BUFFER_SPACE + MACHINE_WIDTH / 2
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
		self.colors = [(240, 248, 255), (250, 235, 215), (0, 255, 255), (127, 255, 212), (240, 255, 255), (245, 245, 220), (255, 228, 196), (0, 0, 0), (255, 235, 205), (0, 0, 255), (138, 43, 226), (165, 42, 42), (222, 184, 135), (95, 158, 160), (127, 255, 0), (210, 105, 30), (255, 127, 80), (100, 149, 237), (255, 248, 220), (220, 20, 60), (0, 255, 255), (0, 0, 139), (0, 139, 139), (184, 134, 11), (169, 169, 169), (0, 100, 0), (189, 183, 107), (139, 0, 139), (85, 107, 47), (255, 140, 0), (153, 50, 204), (139, 0, 0), (233, 150, 122), (143, 188, 143), (72, 61, 139), (47, 79, 79), (0, 206, 209), (148, 0, 211), (255, 20, 147), (0, 191, 255), (105, 105, 105), (30, 144, 255), (178, 34, 34), (255, 250, 240), (34, 139, 34), (255, 0, 255), (220, 220, 220), (248, 248, 255), (255, 215, 0), (218, 165, 32), (128, 128, 128), (0, 128, 0), (173, 255, 47), (240, 255, 240), (255, 105, 180), (205, 92, 92), (75, 0, 130), (255, 255, 240), (240, 230, 140), (230, 230, 250), (255, 240, 245), (124, 252, 0), (255, 250, 205), (173, 216, 230), (240, 128, 128), (224, 255, 255), (250, 250, 210), (211, 211, 211), (144, 238, 144), (255, 182, 193), (255, 160, 122), (32, 178, 170), (135, 206, 250), (119, 136, 153), (176, 196, 222), (255, 255, 224), (0, 255, 0), (50, 205, 50), (250, 240, 230), (255, 0, 255), (128, 0, 0), (102, 205, 170), (0, 0, 205), (186, 85, 211), (147, 112, 219), (60, 179, 113), (123, 104, 238), (0, 250, 154), (72, 209, 204), (199, 21, 133), (25, 25, 112), (245, 255, 250), (255, 228, 225), (255, 228, 181), (255, 222, 173), (0, 0, 128), (253, 245, 230), (128, 128, 0), (107, 142, 35), (255, 165, 0), (255, 69, 0), (218, 112, 214), (238, 232, 170), (152, 251, 152), (175, 238, 238), (219, 112, 147), (255, 239, 213), (255, 218, 185), (205, 133, 63), (255, 192, 203), (221, 160, 221), (176, 224, 230), (128, 0, 128), (102, 51, 153), (255, 0, 0), (188, 143, 143), (65, 105, 225), (139, 69, 19), (250, 128, 114), (244, 164, 96), (46, 139, 87), (255, 245, 238), (160, 82, 45), (192, 192, 192), (135, 206, 235), (106, 90, 205), (112, 128, 144), (255, 250, 250), (0, 255, 127), (70, 130, 180), (210, 180, 140), (0, 128, 128), (216, 191, 216), (255, 99, 71), (64, 224, 208), (238, 130, 238), (245, 222, 179), (255, 255, 255), (245, 245, 245), (255, 255, 0), (154, 205, 50)] #[RED, BLACK, BLUE, GREEN, PURPLE, LIGHT_BLUE] #self.color_gen.get_more_colors(size)
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

#		if(self.length == 0):
#			if "debug" in globals():
#				print "Trying to pop empty color list"
#				return BLACK
#			self.colors.join(self.color_gen.get_more_colors(self.get_more_size))
#			self.length += self.get_more_size
#		self.length -= 1
#		color = self.colors.pop()
#		if "debug" in globals():
#			 print "color is: " + str(color)
#		return color

	def append(self, color):
		self.colors.append(color)



class Machine(object):
	def __init__(self, name, machine_type, ip, grid_location, fileCount, draw, legend):
		self.name = name
		self.ip = ip
		self.tasks = dict() #key is task_no, value is Task_Info instance
		self.files = [] #dict()
		self.last_touched = None
		self.fileCount = fileCount
		self.grid_location = grid_location
		self.file_display = Machine_File_Display(draw, grid_location)
		self.task_display = Machine_Task_Display(draw, grid_location)
		self.is_visible = False
		self.machine_type = machine_type
		self.draw = draw
		self.pending_transfer_to_manager = None
		self.pending_transfer_to_worker = None
		self.legend = legend

	def update_tasks(self, font, line, log, color_list):
		line_task_inf = get_task_from_line(line)
		if(line_task_inf):
			if(line_task_inf.status == "assigned"):
				self.tasks[line_task_inf.num] = line_task_inf
				line_task_inf.set_info(log, self.fileCount, self.ip, color_list, self.legend)
				self.task_display.add_task(self.ip, line_task_inf.num)
				return True

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

	def update_network_communications(self, line, color_list, fileCount):
		f_hash_name = "file-[0-9a-fA-F]*-([^ ]*)"

		pattern_action_direction = [("needs file .* as "+f_hash_name, "(needs file)", "to_worker"), (": put "+f_hash_name, "put", "to_worker"), ("\) received", "received", "to_worker"), (": get " + f_hash_name, "get", "to_manager"), (": file "+f_hash_name, "file", "to_manager"), ("Receiving file ([^ ]*)", "receiving", "to manager"), (": end", "end", "to manager"), ("will try up to ([0-9]*) seconds to transfer", "set timeout", None)] #add looking for timeout into this thing  #also need to add for returning output files because they're different
		timestamp = line.split(" ")[1]

		#patterns catch any important things such as filename if present
		ft_info = None
		for item in pattern_action_direction:
			matched = re.search(item[0], line)
			if(matched):
				#if "debug" in globals():
				#	print "line: " + line.rstrip("\n")
				#	print "matched for action " + item[1]
				#handle what to give FT_Info constructor from matching based on p_a_d[1]
				if(item[1] not in ("end", "set timeout", "received") ):
					ft_info = FT_Info(matched.group(1), item[1], item[2], None)
					#if "debug" in globals():
					#	print str(matched.group(1)) + "' was captured"

				elif(item[1] == "set timeout"):
					if(self.pending_transfer_to_manager and not self.pending_transfer_to_worker):
						ft_info = FT_Info(None, item[1], item[2], matched.group(1))
					elif(self.pending_transfer_to_worker and not self.pending_transfer_to_manager):
						ft_info = FT_Info(None, item[1], item[2], matched.group(1))
					#elif(not self.pending_transfer_to_worker and not self.pending_transfer_to_manager):
						#if "debug" in globals():
						#	print "No transfer going either way: " + line
							#sys.exit(0)
					#else:
						#if "debug" in globals():
						#	print "Transfer going both ways: " + line
						#	sys.exit(0)
				else:
					ft_info = FT_Info(None, item[1], item[2], None)

				#if "debug" in globals():
				#	print
				break

		if(ft_info):
			self.last_touched = ft_info.fname
			if(ft_info.direction == "to_manager"):
				pass
				#print "to the manager, nothing doing yet"

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
								1 == 1
								#if "debug" in globals():
								#	print "WEIRD SITUATION, multiple transfers to same worker being tried at once"
								#	sys.exit(1)
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
		#fileInf = get_file_from_line(line)
		#if(fileInf):
		#	name = fileInf.name
		#	act = fileInf.action

		#	if(act == "remove"):
		#		#if "debug" in globals():
		#		#	print "removing file " + name + " from " + str(self.ip)
		#		#remove from my files list and more global fileCount
		#		if(name in self.files):
		#			#should always be if removing, but could have partial log
		#			self.files.pop(name)
		#		if(name in fileCount):
		#			#should always be if removing, but could be partial log
		#			fileCount[name].ip_set.remove(self.ip)
		#			if( len(fileCount[name].ip_set) <= 0):
		#				removed_file_distr_info = fileCount.pop(name)
		#				color_list.append(removed_file_distr_info.color)
		#
		#		self.file_display.remove_file(name)
		#
		#	else:
		#		if(name in fileCount):
		#			fileCount[name].occur_count = fileCount[name].occur_count + 1
		#		else:
		#			fileCount[name] = File_Distribution_Info(set([self.ip]), color_list)
		#
		#		if(name in self.files):
		#			self.files[name].act = act
		#		else:
		#			self.files[name] = FileInfo(name, act)

		#		if(not self.files[name].is_possessed):
		#				self.files[name].is_possessed = True
		#				self.display_file.add_file(name, fileCount[name].color)
		#			#a change in possession due to a completed network communication
		#			#will be handled by update_network_communication
		#
		#	return True
		#else:
		#	return False

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
            print("\tTask: {}:{}".format(task, self.tasks[task]))
		for f in self.files:
			print("\tFile: {}\n".format(f))

	def get_location(self):
		return self.location

	def bubble_files(self):
		self.file_display.bubble_files_once(self.fileCount)

class Connections(object):
	def __init__(self):
		self.max_connections = (GIF_WIDTH - BUFFER_SPACE) / (MACHINE_WIDTH + 2 * BUFFER_SPACE)
		self.connections = []
		i = 0;
		while (i < self.max_connections):
			self.connections.append(Connection(i))
			i = i + 1
		self.connections = tuple(self.connections)

	def add(self, draw):
		added = False
		i = 0
		while (i < self.max_connections and not added):
			if not self.connections[i].full():
				grid_x = i
				grid_y = self.connections[i].add(draw)
				added = True

			i = i + 1

		if not added and "debug" in globals():
			print("Out of space :(")
			return None
		else:
			return (grid_x, grid_y)

class Connection(object):
	def __init__(self, grid_x):
		self.num_slots = (GIF_HEIGHT - (TEXT_HEIGHT + MACHINE_WIDTH + 2 * BUFFER_SPACE)) / (MACHINE_WIDTH + BUFFER_SPACE)
		self.slots_remaining = self.num_slots

		i = 0
		self.occupied_vector = []
		while (i < self.slots_remaining):
			self.occupied_vector.append(False)
			i = i + 1

		grid_loc = find_machine_connection_line_pixel((grid_x, 0))
		self.x_pixel = grid_loc[0] + BUFFER_SPACE
		self.top_y_pixel = grid_loc[1] - (MACHINE_WIDTH + BUFFER_SPACE)
		self.bottom_y_pixel = self.top_y_pixel + self.num_slots * (MACHINE_WIDTH + BUFFER_SPACE)

	def full(self):
		return (self.slots_remaining <= 0)

	def add(self, draw):
		if self.full() and ("debug" in globals()):
			print("Can't add to this connection, overfull")
		else:
			#if this connection wire just got its first machine
			if(self.slots_remaining == self.num_slots):
				draw.line((self.x_pixel, self.top_y_pixel, self.x_pixel, self.bottom_y_pixel), BLACK, width = CONNECTION_WIDTH)

			updated = False
			i = 0
			while(not updated and i < self.num_slots):
				if(not self.occupied_vector[i]):
					self.occupied_vector[i] = True
					updated = True
				else:
					i = i + 1
			if not updated and ("debug" in globals()):
				print("Failed to add to this connection, for some reason")
				sys.exit()
			else:
				self.slots_remaining = self.slots_remaining - 1
				return i   #the slot location

	def remove(self, slot_number):
		self.occupied_vector = False
		self.slots_remaining = self.slots_remaining + 1

		#if the connection wire is now out of machines
		if(self.slots_remaining == self.num_slots):
			draw.line((self.x_pixel, self.top_y_pixel, self.x_pixel, self.bottom_y_pixel), WHITE, width = CONNECTION_WIDTH)


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

	if "count_ips" in globals():
		ips = set()

	manager = add_machine(draw, "manager", "", connections, workers, fileCounter)
	add_machine_to_image(draw, manager)
	add_manager_flair(draw, manager)

	legend = Legend(draw, fileCounter, manager.grid_location, FONT_FILE)

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

		if(ip not in workers and ip != None):
			if(manager.ip == ""):
				result = re.search("dns: ([^ ]*) is ([0-9\.]*)", line)
				manager.name = result.group(1)
				manager.ip = result.group(2)
				continue
			else:
				this_worker = add_machine(draw, "worker", ip, connections, workers, fileCounter, legend)
		if(ip != None):
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
				highlight_machine(draw, this_worker, RED)
				fill_in_text(draw, font, line)
				legend.update(this_worker.last_touched)
				padded_nframes = pad(numFrames, append_threshold, last_appended_frame)

				currentImage.save(dirname+"/frame_"+padded_nframes+".gif", "GIF")
				if (numFrames - last_appended_frame  >= append_threshold):
					if "debug" in globals():
						print("gifsicle --delay=" + str(FRAME_DELAY) + " --loop ./"+dirname+"/*.gif > tmp_"+padded_nframes+".gif")
					os.system("gifsicle --delay=" + str(FRAME_DELAY) +" --loop ./"+dirname+"/*.gif > tmp_"+padded_nframes+".gif")
					if "debug" in globals():
						print("gifsicle --batch "+sys.argv[2]+".gif --delay=" + str(FRAME_DELAY) +" --loop --append tmp_"+padded_nframes+".gif")
					os.system("gifsicle --batch "+sys.argv[2]+".gif --delay=" + str(FRAME_DELAY) + " --loop --append tmp_"+padded_nframes+".gif")
					os.system("rm tmp_"+padded_nframes+".gif ./"+dirname+"/*")
					last_appended_frame = numFrames

				numFrames = numFrames + 1
				if(this_worker.is_visible):
					highlight_machine(draw, this_worker, BLACK)
				else:
					if "tcp_debug" in globals():
						print("invisible worker in frame numFrames")
					highlight_machine(draw, this_worker, WHITE)

				color_connection(draw, manager, this_worker, BLACK)

				clear_text_box(draw)
				if "debug" in globals():
					print("len fileCount: " + str(len(fileCounter)))
			else:
				if "show_unused" in globals():
					print(line)
		else:
			if "show_unused" in globals():
				print(line)
	os.system("gifsicle --delay=" + str(FRAME_DELAY) + " --loop ./"+dirname+"/*.gif > tmp.gif")
	os.system("gifsicle --batch "+sys.argv[2]+".gif --append tmp.gif")
	os.system("rm ./"+dirname+"/ -r")
	os.system("rm tmp.gif")
	#os.system("gifsicle --delay=" + str(FRAME_DELAY)+" --loop ./"+dirname+"/*.gif > " + sys.argv[2]+".gif") #this broke my RAM

if __name__ == "__main__":
	main()
