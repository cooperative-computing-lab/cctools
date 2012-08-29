#! /usr/bin/env python

# replica_exchange.py
#
# Copyright (C) 2011- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#
# This program implements elastic replica exchange using
# the cctools work queue framework and the Protomol molecular
# dynamics package, as described in the following paper:
#
# Dinesh Rajan, Anthony Canino, Jesus A Izaguirre, and Douglas Thain,
# "Converting a High Performance Application to an Elastic Cloud Application",
# The 3rd IEEE International Conference on Cloud Computing Technology
# and Science", November 2011.


# From the cctools python bindings, use work queue.
from work_queue import *

# Get ProtoMol related bindings from protomol_functions.
from protomol_functions import *

# All other dependencies are standard python.
import time
import sys
import os
import pprint
import re
import getopt

#-------------------------------Global Variables----------------------------
protomol_local_install = False
use_barrier = False
generate_xyz = False
generate_dcd = False
debug_mode = False
quart_temp_split = False

mc_step_times = []

#------------------------------Global Data--------------------------------------
replica_id = None
proj_name = None
replicas_running = 0

#------------------------Stat Collection Variables------------------------------
num_replica_exchanges = 0
num_task_resubmissions = 0
replica_temp_execution_list = []
replica_exch_list = []
step_time = 0


#--------------------------------Program Meat-----------------------------------

#Function to drop repetetive values and gather just the unique ones.
def unique(inlist, keep_val=True):
  typ = type(inlist)
  if not typ == list:
    inlist = list(inlist)
  i = 0
  while i < len(inlist):
    try: 
      del inlist[inlist.index(inlist[i], i + 1)]
    except:
      i += 1
  if not typ in (str, unicode):
    inlist = type(inlist)
  else:
    if keep_val:
      inlist = ''.join(inlist)
  return inlist


#Function to check if a given executable exists in PATH.
def locate(executable):
    def check_executable(prog):
        return os.path.exists(prog) and os.access(prog, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        exe = os.path.join(path, executable)
        if check_executable(exe):
            return exe

    return None


#Function to generate execution script that runs simulations of a replica 
# over given number of steps
def generate_execn_script(replica_obj, replica_next_starting_step, replica_next_ending_step):
	#assign script file name based on the replica id.
	execn_script_name = "%s/%s/%s/exec-%d.sh" % (output_path, "simfiles", "runs", replica_obj.id)	
	execn_script_stream = open(execn_script_name, "w")

	#check if protomol comes installed on the remote worker site.
	if protomol_local_install:
		execn_string = "%s" % EXECUTABLE
	else:
		execn_string = "./%s" % EXECUTABLE

	#initialize string that will hold the file strings.
	write_str = ""

	#write protomol execution commands for steps to be run for this replica.
	write_str += "#!/bin/sh\n\n"
	for i in range(replica_next_starting_step, replica_next_ending_step+1):
		write_str += "%s %d-%d.cfg\n" % (execn_string, replica_obj.id, i)

	#Write to file
	execn_script_stream.write(write_str)
	execn_script_stream.close()

	#Make file executable
	os.chmod(execn_script_name, 0755) 

	return execn_script_name

	
#Setup and initalize WorkQueue
def create_wq():
	#Instantiate workqueue instance.	
	wq = WorkQueue()
	wq.specify_algorithm(WORK_QUEUE_SCHEDULE_RAND)
	if proj_name:
		wq.specify_name(proj_name)

	return wq


#Create a new WorkQueue task.
def create_wq_task(replica_id):
	#Task string will be running the execution script.
	task_str = "./exec-%d.sh" % replica_id

	#Create a task using given task string for remote worker to execute.
	task = Task(task_str) 
	task.specify_tag('%s' % replica_id)

	return task


#Assign all the input files for the task (replica).
def assign_task_output_files(task, replica_list, replica_id, replica_next_starting_step, replica_next_ending_step):
	#Find pdb file for current replica
	replica_pdb = "%s.%d" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_id)
	
	#Assign local and remote xyz output files.
	if generate_xyz:
		local_xyz_output_file = "%s/simfiles/%s/%s.%d-%d.xyz" % (output_path, replica_list[replica_id].temp, xyz_file_name, replica_id, replica_next_ending_step) 
		remote_xyz_output_file = "%d.xyz" % (replica_id)
		task.specify_output_file(remote_xyz_output_file, local_xyz_output_file)

	#Assign local and remote dcd output files.
	if generate_dcd:
		local_dcd_output_file = "%s/simfiles/%s/%s.%d-%d.dcd" % (output_path, replica_list[replica_id].temp, dcd_file_name, replica_id, replica_next_ending_step) 
		remote_dcd_output_file = "%d.dcd" % (replica_id)
		task.specify_output_file(remote_dcd_output_file, local_dcd_output_file)

	#Assign local and remote (output) energies files.
	local_energies_file = "%s/simfiles/eng/%d/%d.eng" % (output_path, replica_id, replica_id) 
	remote_energies_file = "%d.eng" % replica_id
	task.specify_output_file(local_energies_file, remote_energies_file, cache=False)

	#Assign local and remote velocity output files.
	local_velocity_output_file = "%s/simfiles/%s/%s-%d.vel" % (output_path, replica_list[replica_id].temp, replica_pdb, replica_next_ending_step+1)
	remote_velocity_output_file = "%s-%d.vel" % (replica_pdb, replica_next_ending_step+1)
	task.specify_output_file(local_velocity_output_file, remote_velocity_output_file, cache=False)
		
	pdb_output_file = "%s/simfiles/%s/%s-%d.pdb" % (output_path, replica_list[replica_id].temp, replica_pdb, replica_next_ending_step+1)	
	task.specify_output_file(pdb_output_file, parse_file_name(pdb_output_file), cache=False)


#Assign all the output files for the task (replica).
def assign_task_input_files(task, replica_list, replica_id, replica_next_starting_step, replica_next_ending_step):
	#Find pdb file for current replica
	replica_pdb = "%s.%d" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_id)

	#Find pdb file for replica that exchanged with current replica in last step
	if (replica_list[replica_id].exchgd_replica_id > -1):
		exchgd_replica_pdb = "%s.%d" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_list[replica_id].exchgd_replica_id)
	else:
		exchgd_replica_pdb = "%s.%d" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_id)
	
	'''Local_file: name for file brought back and stored on local site where this is run.
	   Remote_file: name for file sent to remote worker and used in execution there.'''
	#Assign local and remote execution scripts 
	local_execn_file = "%s/simfiles/%s/exec-%d.sh" % (output_path, "runs", replica_id)
	remote_execn_file = "exec-%d.sh" % (replica_id)
	task.specify_input_file(local_execn_file, remote_execn_file, cache=False)

	#Assign local and remote psf and par inputs 
	task.specify_input_file(psf_file, parse_file_name(psf_file))
	task.specify_input_file(par_file, parse_file_name(par_file))

	#Assign local and remote pdb inputs 
	local_pdb_input_file = "%s/simfiles/%s/%s-%d.pdb" % (output_path, replica_list[replica_id].temp, exchgd_replica_pdb, replica_next_starting_step)
	remote_pdb_input_file = "%s-%d.pdb" % (replica_pdb, replica_next_starting_step)	
	task.specify_input_file(local_pdb_input_file, remote_pdb_input_file, cache=False)

	#Velocity input only required after first step since it is output 
	#of first step.
	if (replica_next_starting_step > 0):	
		#Assign local and remote velocity input files.
		local_velocity_input_file = "%s/simfiles/%s/%s-%d.vel" % (output_path, replica_list[replica_id].temp, exchgd_replica_pdb, replica_next_starting_step)
		remote_velocity_input_file = "%s-%d.vel" % (replica_pdb, replica_next_starting_step)
		task.specify_input_file(local_velocity_input_file, remote_velocity_input_file, cache=False)
		
	for i in range(replica_next_starting_step, replica_next_ending_step+1):
		#Assign local and remote config files.
		local_config_file = "%s/simfiles/config/%d/%d-%d.cfg" % (output_path, replica_id, replica_id, i)
		remote_config_file = "%d-%d.cfg" % (replica_id, i)
		task.specify_input_file(local_config_file, remote_config_file, cache=False) 

		#Call function to generate execution script.
		generate_execn_script(replica_list[replica_id], replica_next_starting_step, replica_next_ending_step)

	#Assign executable that will be run on remote worker to task string.
	if not protomol_local_install:
		local_executable = "%s" % (EXECUTABLE)
		remote_executable = "%s" % (EXECUTABLE)
		task.specify_input_file(local_executable, remote_executable)


# Major replica exchange and scheduling is handled here
def wq_main(wq, replica_list, replicas_to_run):

	#Stat collection variables
	global replicas_running
	global step_time
	
	#Variable that tracks replicas which completed simulations over all MC steps
	num_replicas_completed = 0

	#-------Perform computation for each replica at current monte carlo step--------
	'''Each computation is a task in work queue.
	   Each task will be run on one of the connected workers.'''
	
	while num_replicas_completed < len(replica_list):
			#Iterate through the given set of replicas and start their
			#		 computation for the current monte carlo step.
			for j in replicas_to_run:
				
				if not replica_list[j].running:
					#Initialize step time.
					step_time = time.time()
						
					replica_id = replica_list[j].id
					#Each replica does computation at its current temperature
					replica_temperature = replica_list[j].temp
			
					'''Get the last seen step of replica. The last_seen_step
					is the step at which this replica was brought back and 
					attempted for an exchange.'''
					replica_last_seen_step = replica_list[j].last_seen_step
					
					#record the starting, ending steps for current iteration of 
					#this replica.
					replica_next_starting_step = replica_last_seen_step + 1
					if replica_next_starting_step >= monte_carlo_steps:
						break
			
					if use_barrier:
						#Barrier version, so run one step at a time.
						replica_next_ending_step = replica_next_starting_step
					else:
						#Run all steps until the step where the replica will be
						#chosen to attempt an exchange. 
						if len(replica_list[j].exch_steps) > 0:
							replica_next_ending_step = replica_list[j].exch_steps[0]
						#If there are no more exchange steps for this replica, run the
						#remainder of monte carlo steps.
						else:
							replica_next_ending_step = monte_carlo_steps-1

					#Set the last_seen_step to the next exchange step at which the
					#replica (its output) will be brought back.
					replica_list[j].last_seen_step = replica_next_ending_step

					task = create_wq_task(replica_id)
					assign_task_input_files(task, replica_list, replica_id, replica_next_starting_step, replica_next_ending_step)		
					assign_task_output_files(task, replica_list, replica_id, replica_next_starting_step, replica_next_ending_step)

					#Keep count of replicas that iterated through all MC steps.
					if (replica_next_ending_step == monte_carlo_steps-1):
						num_replicas_completed += 1

					#Submit the task to WorkQueue for execution at remote worker.
					wq.submit(task)
				
					#Submitted for execution. So mark this replica as running.
					replica_list[j].running = 1
					replicas_running += 1

			#Wait for tasks to complete.
			if use_barrier:
				replicas_to_run=wq_wait_barrier(wq, replica_list, replica_next_starting_step, 30)
			else:
				replicas_to_run=wq_wait_nobarrier(wq, replica_list, 30)
				

'''The barrier version where it waits for all replicas to finish a given MC step.
   Returns all the replicas that it waited for.'''
def wq_wait_barrier(wq, replica_list, monte_carlo_step, timeout):

	#Stat collection variables
	global step_time
	global num_task_resubmissions
	global replicas_running

	#Initialize list that contains replicas that will be returned to run next.
	replica_to_run = []

	#Wait for all replicas to finish execution,
	while not wq.empty():
		task = wq.wait(timeout)
		if (task):
			#Get replica id from finished task.
			replica_id = int(task.tag)
			
			# Check if task (replica) failed. If so, resubmit.
			if task.result != 0:
				num_task_resubmissions += 1
				print "Replica failed!"
				time.sleep(3)
				#Resubmit the task.
				wq.submit(task) 
				continue

			#Task was succesful. Update run information.
			replicas_running -= 1
			replica_list[replica_id].running = 0
				
		    #Get potential energy value of the completed replica run.	
			energies_file =  "%s/simfiles/eng/%d/%d.eng" % (output_path, replica_id, replica_id) 
			energies_stream = open(energies_file, "r")
			slist = (energies_stream.readline()).split()
			potential_energy = float(slist[1])
			replica_list[replica_id].potential_energy = potential_energy
			
			#Store temperature and exchanged replica id values from the current run.
			replica_list[replica_id].prev_temp = replica_list[replica_id].temp
			replica_list[replica_id].exchgd_replica_id = replica_id

			#Add this replica to return list.
			replica_to_run.append(replica_id)

	#Get replica exchange pair for current step and attempt exchange.
	cur = replica_exch_list[monte_carlo_step].pop(0)
	next = replica_exch_list[monte_carlo_step].pop(0)
	if debug_mode: 
		print "Replicas %d & %d are attempted for an exchange at step %d" % (cur, next, monte_carlo_step)

	#Attempt exchange between the two.
	attempt_replica_exch(replica_list, cur, next)

	#Update time stats for this MC step.
	step_time = time.time() - step_time
	mc_step_times.append(step_time)

	return replica_to_run


'''The nobarrier version where it receives a finished replica, waits for its 
   exchange partner to finish, attempts an exchange between the two, and continues 
   waiting for the rest similarly.
   Returns the replica pair that finished and was attempted for an exchange.'''
def wq_wait_nobarrier(wq, replica_list, timeout):
	
	#Stat collection variables
	global num_task_resubmissions
	global replicas_running
	
	#Wait for a task to finish execution.
	while not wq.empty():
		task = wq.wait(timeout)
		if (task):
			#Get replica id from finished task.
			replica_id = int(task.tag)
		
			#Check if task (replica) failed. If so, resubmit.
			if task.result != 0:
				num_task_resubmissions += 1
				print "Replica failed!"
				time.sleep(3)
				
				#Resubmit the task.
				wq.submit(task) 
				continue

			#Task was succesful. Update run information.
			replicas_running -= 1
			replica_list[replica_id].running = 0

		   	#Get potential energy value of the completed replica run.	
			energies_file =  "%s/simfiles/eng/%d/%d.eng" % (output_path, replica_id, replica_id) 
			energies_stream = open(energies_file, "r")
			slist = (energies_stream.readline()).split()
			potential_energy = float(slist[1])
			replica_list[replica_id].potential_energy = potential_energy
			
			#Store temperature and exchanged replica id values from the current run.
			replica_list[replica_id].prev_temp = replica_list[replica_id].temp
			replica_list[replica_id].exchgd_replica_id = replica_id

			#Replica should be currently at this step which is its exchange step.
			if len(replica_list[replica_id].exch_steps) > 0:	
				replica_exch_step = replica_list[replica_id].exch_steps.pop(0)
			#Else replica is at the last MC step of this run.
			else:
				replica_exch_step = monte_carlo_steps - 1

			#Find the exchange partner of this replica.
		 	if (replica_id == replica_exch_list[replica_exch_step][0]):
				replica_exch_partner = replica_exch_list[replica_exch_step][1]
			elif (replica_id == replica_exch_list[replica_exch_step][1]):
				replica_exch_partner = replica_exch_list[replica_exch_step][0]
			else:
				if (replica_exch_step != (monte_carlo_steps-1)):
					#If this replica is not part of the exchange pair for this
					#step and is not at the last MC step of the run, something 
					#is amiss..
					print "Replica %d should not be here at step %d" % (replica_id, replica_exch_step)
					sys.exit(1)
				else:
					#If all replicas have completed last MC step, return.
					if replicas_running == 0:
						return
					#If not, loop back to receive other replicas.
					else:
						continue

			#If exchange partner is still running, go back to get other tasks.
			if replica_list[replica_exch_partner].running:
				continue
			#Otherwise check if partner has finished the current exchange step.
			else:
				if (replica_list[replica_exch_partner].last_seen_step < replica_exch_step):
					#Exchange partner is currently behind the exchange step of
					#this replica. So loop back to get other tasks.
					continue
				elif (replica_list[replica_exch_partner].last_seen_step > replica_exch_step):
					#Should never get here. Something went wrong.
					print "Partner of replica %d - replica %d is currently at step %d which is beyond step %d" % (replica_id, replica_exch_partner, replica_list[replica_exch_partner].exch_steps[0], replica_exch_step)
					sys.exit(1)
				else:
					#Make sure the replicas are checked in the same order they were chosen at the start.
					if (replica_exch_partner == replica_exch_list[replica_exch_step][0]):
						replica_1 = replica_exch_partner
						replica_2 = replica_id
					else:
						replica_1 = replica_id
						replica_2 = replica_exch_partner
						
					if debug_mode: 
						print "Replicas %d & %d are attempted for an exchange at step %d" % (replica_1, replica_2, replica_exch_step)
					
					#Attempt exchange between the two.
					attempt_replica_exch(replica_list, replica_1, replica_2)
				
					#Add these two replicas to return list.
					replicas_to_run = [replica_1, replica_2]
					
					return replicas_to_run


#Check if two replicas satisfy the criteria to undergo an exchange.
def attempt_replica_exch(replica_list, replica1, replica2):
	global num_replica_exchanges
	
	#Check for metropolis criteria.
	if (metropolis(replica_list[replica1].potential_energy, replica_list[replica2].potential_energy, replica_list[replica1].temp, replica_list[replica2].temp)):
		#Swap fields of the two replicas being exchanged.
		T = replica_list[replica2].temp
		replica_list[replica2].temp = replica_list[replica1].temp
		replica_list[replica1].temp = T
			
		replica_list[replica1].exchgd_replica_id = replica_list[replica2].id
		replica_list[replica2].exchgd_replica_id = replica_list[replica1].id
			
		replica_temp_execution_list[replica1].append(replica_list[replica1].temp)
		replica_temp_execution_list[replica2].append(replica_list[replica2].temp)
			
		if debug_mode: 
			print "Replicas %d and %d exchanged" % (replica1, replica2)
		
		#Keep count of exchanges.
		num_replica_exchanges += 1


#Function to create directories to hold files from the simulations.
def make_directories(output_path, temp_list, num_replicas):
	count = 0
	#For each temperature value, create a directory with its name.	
	for i in temp_list:
		newdir1 = "%s/simfiles/%s" % (output_path, i)
	
		if not os.path.exists(newdir1):
			os.makedirs(newdir1)

		command = "cp %s %s/simfiles/%s/%s.%d-%d.pdb" % (pdb_file, output_path, i, remove_trailing_dots(parse_file_name(pdb_file)), count, 0)
		print command
		os.system(command)
		count += 1

	#Create eng (energies) directory to hold energy files of replicas.
	for j in range(num_replicas):
		newdir2 = "%s/simfiles/%s/%s" % (output_path, "eng", j)
		if not os.path.exists(newdir2):
			os.makedirs(newdir2)

	#Create config (configuration) directory to hold config files of replicas.
	for j in range(num_replicas):
		newdir3 = "%s/simfiles/%s/%s" % (output_path, "config", j)
		if not os.path.exists(newdir3):
			os.makedirs(newdir3)
	
	#Create directory to hold execution scripts of replicas.
	newdir4 = "%s/simfiles/%s" % (output_path, "runs")
	if not os.path.exists(newdir4):
		os.makedirs(newdir4)


#Function to determine the replica exchange pairs that will be attempted for an
#exchange at each MC step.
def create_replica_exch_pairs(replica_list, num_replicas):
	#Compute random pair (replica, neighbor) for each step to attempt exchange.
	for i in range(monte_carlo_steps):
		replica_1 = random.randint(0, num_replicas-1)
		replica_2 = replica_1 + 1

		if (replica_2 == num_replicas):
			replica_2 = replica_1 - 1
		
		#List that stores replicas attempted for exchange at each step.
		replica_exch_list.append([])
		replica_exch_list[i].append(replica_1)
		replica_exch_list[i].append(replica_2)
	
		#Store the steps at which each replica will be attempted for exchange.
		replica_list[replica_1].exch_steps.append(i)
		replica_list[replica_2].exch_steps.append(i)

		if debug_mode: 
			print "For step %d, exchange will be attempted for replica %d and %d." % (i, replica_1, replica_2)
		

#Main function.
if __name__ == "__main__":

	#Create help string for user.
	usage_str = "Usage: %s <PDB_FILE> <PSF_FILE> <PAR_FILE> <MIN_TEMP> <MAX_TEMP> <NUM_OF_REPLICAS>" % sys.argv[0]
	help_str = "-n		-	specify a project name for using exclusive workers\n"
	help_str += "-x		-	specify the name of the xyz file for output\n"
	help_str += "-d		-	specify the name of the dcd file for output\n"
	help_str += "-m		-	specify the number of monte carlo steps\n"
	help_str += "-s		-	specify the number of mdsteps\n"
	help_str += "-p		-	specify the output_path for the output files generated\n"
	help_str += "-q		-	assign closer temperature values to the first and last quartile of the replicas.\n"
	help_str += "-i		-	assume ProtoMol is installed and available in PATH on worker site\n"
	help_str += "-b		-	use barrier in waiting for all replicas to finish their steps before attempting exchange.\n"
	help_str += "-l		-	print debuging information\n"
	help_str += "-h		-	help"

	#Check to see if there is error in the given command line arguments.
	try:
		opts, args = getopt.getopt(sys.argv[1:], "n:x:d:m:s:p:qiblh", ["help"])
	except getopt.GetoptError, err:
		print str(err) 
		print usage_str
		sys.exit(1)

	#Parse command line arguments.
	for o, a in opts:
		if o in ("-h", "--help"):
			print help_str
			sys.exit(0)
		elif o == "-l":
			debug_mode = True
		elif o in ("-x"):
			generate_xyz = True
			xyz_file_name = a
		elif o in ("-d"):
			generate_dcd = True
			dcd_file_name = a
		elif o in ("-n"):
			proj_name = a
		elif o in ("-p"):
			output_path = a
		elif o in ("-m"):
			monte_carlo_steps = int(a)
		elif o in ("-s"):
			md_steps = int(a)
		elif o == "-q":
			quart_temp_split = True
		elif o == "-i":
			protomol_local_install = True
		elif o == "-b":
			use_barrier = True

	#Check for the 6 mandatory arguments.
	if len(args) != 6:
		print usage_str
		sys.exit(1)

	if debug_mode:
		print "Debug mode on.."
		set_debug_flag("wq")
		set_debug_flag("debug")

	protomol_path = locate(EXECUTABLE)
	if protomol_path:
		command = "cp %s ." % protomol_path
		os.system(command)
	else:
		print "Error: Please add %s to your PATH." % (EXECUTABLE)
		sys.exit(1)
		
	#Begin timing after parsing command line arguments.
	total_run_time = time.time()

	#Parse command line to get input file names,
   	#   	temperature range for simulation,
	#       number of replicas to run.
	pdb_file = args[0]
	psf_file = args[1]
	par_file = args[2]
	min_temp = int(args[3])
	max_temp = int(args[4])
	num_replicas = int(args[5])
	replica_list = []

	#Split up the temperature range for assigning to each replica.
	inc = float(( max_temp -  min_temp)) / float(num_replicas-1)
	temp_list = []

	#Assign temperature to each replica.
	for x in range(num_replicas):
		#Quart split assigns closer temperature values
		#	to the top and bottom 25% of replicas.
		if quart_temp_split:
			if x < math.ceil(0.25 * num_replicas):
				replica_temp =  min_temp + (x * inc / 3)

			elif x >= math.ceil(0.75 * num_replicas):
				replica_temp =  max_temp - (((num_replicas-1) - x) * inc / 3)

			else:
				replica_temp =  min_temp + (x * inc)
		
		#If not quart split, split temperature range uniformly 
		#                    among all replicas.	
		else:
			replica_temp =  min_temp + (x * inc)
	
		#Store the temperature values and replicas.	
		temp_list.append(str(replica_temp))
		replica_list.append(Replica(x, replica_temp))

		#Initialize list for maintaining replica exchange matrix.
		replica_temp_execution_list.append([])
		
	#Add the initial temperature value to the replica exchange matrix.
	for repl in range(num_replicas):
		replica_temp_execution_list[repl].append(replica_list[repl].temp)

	#Create directories for storing data from the run.
	make_directories(output_path, temp_list, num_replicas)

	#Create random replica pairs to check for exchange at each step.
	create_replica_exch_pairs(replica_list, num_replicas)

	#create config files here.
	for i in range(monte_carlo_steps):
		for j in range(num_replicas):
			generate_config(output_path, pdb_file, psf_file, par_file, i, md_steps, output_freq, replica_list[j])

	replicas_to_run = []
	for i in range(num_replicas):
		replicas_to_run.append(i)

	#Instantiate WQ.
	wq = create_wq()

	#Start the run.
	wq_main(wq, replica_list, replicas_to_run)  	

	#Track total run time.
	total_run_time = (time.time() - total_run_time)

	#Print stats on completion.
	print "Total Run Time:                  %f"  % total_run_time
	print "Number of failures:              %d"  % num_task_resubmissions
	print "Replica Exchanges:               %d"  % num_replica_exchanges
	print "Acceptance Rate:                 %f"  % ((num_replica_exchanges * 100) / monte_carlo_steps)
	
	#Write stats to a stats file
	stat_file_name = "%s/%s.stat" % (output_path,  remove_trailing_dots(parse_file_name(pdb_file)))
	stat_file_stream = open(stat_file_name, "w")

	stat_file_stream.write("%s\n" % "Printing replica temperature execution matrix:")
	#Sort and format the replica exchange matrix.
	for itr in range(num_replicas):
		 replica_temp_execution_list[itr].sort()
		 unique(replica_temp_execution_list[itr])
		 stat_file_stream.write("Replica %d: %s\n" % (itr, replica_temp_execution_list[itr]))

	#If barrier version was used, write the MC step times to stats file.
	if use_barrier:
		#Write run time for each step to stats file
		stat_file_stream.write("\n\n%s\n" % "Printing run times for each monte carlo step:")
		count = 1
		for i in mc_step_times:
			stat_file_stream.write("%d %f\n" % (count, i))
			count += 1

	stat_file_stream.close()
	
	sys.exit(0)
