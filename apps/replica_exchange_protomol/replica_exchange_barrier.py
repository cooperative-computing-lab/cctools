#! /usr/bin/env python

# replica_exchange_protomol_barrier
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
generate_xyz = False
generate_dcd = False
debug_mode = False
quart_temp_split = False
vel_file_input = False

mc_step_times = []

#-----------------------------Global Data-------------------------------------
replica_id = None
proj_name = None

#------------------------Stat Collection Variables------------------------------
num_replica_exchanges = 0
total_monte_carlo_step_time = 0
num_task_resubmissions = 0
replica_temp_execution_list = []
replica_exch_list = []


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
    inlist = typ(inlist)
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


# Major replicaexchange and scheduling is handled here
def re_main(replica_list, num_replicas):

	#Stat collection variables
	global num_replica_exchanges
	global total_monte_carlo_step_time
	global num_task_resubmissions

	#Setup WorkQueue and initialize parameters 
	wq = WorkQueue()
	wq.specify_algorithm(WORK_QUEUE_SCHEDULE_RAND)

	if proj_name:
		wq.specify_name(proj_name)

	wq.activate_fast_abort(3.0);

	#Iterate through the given number of monte carlo steps
	for i in range(monte_carlo_steps):
		#Initialize step time.
		step_time = time.time()
		
		#Keep track of number of replicas running
		replicas_running = num_replicas
	
		#-------Perform computation for each replica at current monte carlo step--------
		'''Each computation is a task in the workqueue
		   Each replica will be run on one of the workers instantiated.'''
	
		#Iterate through the given number of replicas and start their
		#		 computation for the current monte carlo step.
		for j in range(num_replicas):
			
			replica_id = replica_list[j].id
			#Each replica does computation at its current temperature
			replica_temperature = replica_list[j].temp
			
			#Find pdb file for current replica
			replica_pdb = "%s.%d" % (remove_trailing_dots(parse_file_name(pdb_file)), j)
			
			#Find pdb file for replica that exchanged with current replica in last step
			if (replica_list[j].exchgd_replica_id > -1):
				exchgd_replica_pdb = "%s.%d" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_list[j].exchgd_replica_id)

			'''Local_file - file that will be brought back and stored on the user computer.
			   Remote_file - file that will be sent to remote worker and used in execution there.'''

			#Assign local and remote config files.
			local_config_file = "%s/simfiles/config/%d/%d-%d.cfg" % (output_path, replica_id, replica_id, i)
			remote_config_file = "%d-%d.cfg" % (replica_id, i)
			
			if (i > 0):	
				vel_file_input = True
				#Assign local and remote velocity input files.
				local_velocity_input_file = "%s/simfiles/%s/%s-%d.vel" % (output_path, replica_list[j].temp, exchgd_replica_pdb, i)
				remote_velocity_input_file = "%s-%d.vel" % (replica_pdb, i)


			#Assign local and remote (output) energies files.
			local_energies_file = "%s/simfiles/eng/%d/%d.eng" % (output_path, replica_id, replica_id) 
			remote_energies_file = "%d.eng" % replica_id
			
			#Assign local and remote velocity output files.
			local_velocity_output_file = "%s/simfiles/%s/%s-%d.vel" % (output_path, replica_list[j].temp, replica_pdb, i+1)
			remote_velocity_output_file = "%s-%d.vel" % (replica_pdb, i+1)
			
			#Assign local and remote xyz output files.
			if generate_xyz:
				local_xyz_output_file = "%s/simfiles/%s/%s.%d-%d.xyz" % (output_path, replica_list[j].temp, xyz_file_name, replica_list[j].id, i) 
				remote_xyz_output_file = "%d.xyz" % (replica_id)

			#Assign local and remote dcd output files.
			if generate_dcd:
				local_dcd_output_file = "%s/simfiles/%s/%s.%d-%d.dcd" % (output_path, replica_list[j].temp, dcd_file_name, replica_list[j].id, i) 
				remote_dcd_output_file = "%d.dcd" % (replica_id)

		    #Create a task using given task string for remote worker to execute.
			#Assign executable that will be run on remote worker to task string.
			if protomol_local_install:
				task_str = "%s %s" % (EXECUTABLE, remote_config_file)
			else:
				local_executable = "%s" % (EXECUTABLE)
				remote_executable = "%s" % (EXECUTABLE)
				task_str = "./%s %s" % (EXECUTABLE, remote_config_file)
		
			task = Task(task_str) 
			task.specify_tag('%s' % replica_id)
				
			if not protomol_local_install:
				task.specify_input_file(local_executable, remote_executable)
	        
			#Set the input files required by the task.
			task.specify_input_file(local_config_file, remote_config_file) 

			if (i > 0):
				local_pdb_input_file = "%s/simfiles/%s/%s-%d.pdb" % (output_path, replica_list[j].temp, exchgd_replica_pdb, i)
			else:
				local_pdb_input_file = "%s/simfiles/%s/%s-%d.pdb" % (output_path, replica_list[j].temp, replica_pdb, i)	
			
			remote_pdb_input_file = "%s-%d.pdb" % (replica_pdb, i)	
			task.specify_input_file(local_pdb_input_file, remote_pdb_input_file)
			task.specify_input_file(psf_file, parse_file_name(psf_file))
			task.specify_input_file(par_file, parse_file_name(par_file))

			if (i > 0):
				task.specify_input_file(local_velocity_input_file, remote_velocity_input_file)
			
			if generate_xyz:
				task.specify_output_file(remote_xyz_output_file, local_xyz_output_file)
			if generate_dcd:
				task.specify_output_file(remote_dcd_output_file, local_dcd_output_file)

			task.specify_output_file(local_energies_file, remote_energies_file)
			task.specify_output_file(local_velocity_output_file, remote_velocity_output_file)

			pdb_output_file = "%s/simfiles/%s/%s-%d.pdb" % (output_path, replica_list[j].temp, replica_pdb, i+1)	
			task.specify_output_file(pdb_output_file, parse_file_name(pdb_output_file))
		
			#Submit the task to WorkQueue for execution at remote worker.
			wq.submit(task)


		#Wait for all replicas to finish execution,
		while not wq.empty():
			task = wq.wait(10)
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

				#Task was succesful. Update all information.
				replicas_running -= 1
				
			    #Get potential energy value of the completed replica run.	
				energies_file =  "%s/simfiles/eng/%d/%d.eng" % (output_path, replica_id, replica_id) 
				energies_stream = open(energies_file, "r")
				slist = (energies_stream.readline()).split()
				potential_energy = float(slist[1])
				replica_list[replica_id].potential_energy = potential_energy
			
				#Store temperature and exchanged replica id values from the current run.
				replica_list[replica_id].prev_temp = replica_list[replica_id].temp
				replica_list[replica_id].exchgd_replica_id = replica_id

	
		cur = replica_exch_list[i].pop(0)
		next = replica_exch_list[i].pop(0)
		
		if debug_mode: print "Replicas %d & %d are attempted for an exchange at step %d" % (cur, next, i)
		attempt_replica_exch(replica_list, cur, next)

		#Update time stats.
		step_time = time.time() - step_time
		total_monte_carlo_step_time += step_time
		mc_step_times.append(step_time)

		#Loop back to the top


def attempt_replica_exch(replica_list, cur, next):
	global num_replica_exchanges
	
	#Check for metropolis criteria with the randomly chosen replica and its immediate neighbor.
	if (metropolis(replica_list[cur].potential_energy, replica_list[next].potential_energy, replica_list[cur].temp, replica_list[next].temp)):
		if debug_mode: print "Replicas %d and %d exchanged" % (cur, next)
		#Swap the fields of the two replicas being exchanged.
		T = replica_list[next].temp
		replica_list[next].temp = replica_list[cur].temp
		replica_list[cur].temp = T
			
		replica_list[cur].exchgd_replica_id = replica_list[next].id
		replica_list[next].exchgd_replica_id = replica_list[cur].id
			
		replica_temp_execution_list[cur].append(replica_list[cur].temp)
		replica_temp_execution_list[next].append(replica_list[next].temp)
			
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

	#Create config (configuration) directory to hold config files of replicas.
	for j in range(num_replicas):
		newdir2 = "%s/simfiles/%s/%s" % (output_path, "config", j)
		if not os.path.exists(newdir2):
			os.makedirs(newdir2)

	#Create eng (energies) directory to hold energy files of replicas.
	for j in range(num_replicas):
		newdir3 = "%s/simfiles/%s/%s" % (output_path, "eng", j)
		if not os.path.exists(newdir3):
			os.makedirs(newdir3)


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

	if debug_mode:
		for i in replica_list:
			print "Replica %d has the following exchange steps: %s." % (i.id, i.exch_steps)


#Main function.
if __name__ == "__main__":

	#Create help string for user.
	usage_str = "Usage: %s <PDB_FILE> <PSF_FILE> <PAR_FILE> <MIN_TEMP> <MAX_TEMP> <NUM_OF_REPLICAS>" % sys.argv[0]
	help_str  = "-h		-	help\n"
	help_str += "-n		-	specify a project name for using exclusive workers\n"
	help_str += "-x		-	specify the name of the xyz file for output\n"
	help_str += "-d		-	specify the name of the dcd file for output\n"
	help_str += "-l		-	print debuging information\n"
	help_str += "-m		-	specify the number of monte carlo steps\n"
	help_str += "-s		-	specify the number of mdsteps\n"
	help_str += "-p		-	specify the output_path for the output files generated\n"
	help_str += "-i		-	assume ProtoMol is installed and available in PATH on worker site\n"
	help_str += "-q		-	assign closer temperature values to the first and last quartile of the replicas.\n"

	#Check to see if there is error in the given command line arguments.
	try:
		opts, args = getopt.getopt(sys.argv[1:], "m:s:n:p:x:d:ihlq", ["--help", "--proj", "dir", "--xyz", "--dcd"])
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
		elif o in ("-x", "--xyz"):
			generate_xyz = True
			xyz_file_name = a
		elif o in ("-d", "--dcd"):
			generate_dcd = True
			dcd_file_name = a
		elif o in ("-n", "--proj"):
			proj_name = a
		elif o in ("-p", "--dir"):
			output_path = a
		elif o in ("-m"):
			monte_carlo_steps = int(a)
		elif o in ("-s"):
			md_steps = int(a)
		elif o == "-q":
			quart_temp_split = True
		elif o == "-i":
			protomol_local_install = True

	if debug_mode:
		print "Debug mode on.."
		set_debug_flag("wq")
		set_debug_flag("debug")

	if len(args) != 6:
		print usage_str
		sys.exit(1)

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
   	#      temperature range for simulation,
	#      number of replicas to run.
	pdb_file = args[0]
	psf_file = args[1]
	par_file = args[2]
	min_temp = int(args[3])
	max_temp = int(args[4])
	num_replicas = int(args[5])

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
		#among all replicas.	
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

	for i in range(monte_carlo_steps):
		for j in range (num_replicas):
			generate_config(output_path, pdb_file, psf_file, par_file, md_steps, output_freq, replica_list[j], i)

	#Start the run.
	re_main(replica_list, num_replicas)  	
	
	#Keep track of the total run time.
	total_run_time = (time.time() - total_run_time)

	#Print stats on completion.
	print "Total Run Time:                  %f"  % total_run_time
	print "Total Monte Carlo Step Time:     %f"  % total_monte_carlo_step_time
	print "Average Monte Carlo Step Time:   %f"  % (total_monte_carlo_step_time /  monte_carlo_steps)
	print "Number of failures:   			%d"  % num_task_resubmissions
	print "Replica Exchanges:               %d"  % num_replica_exchanges
	print "Acceptance Rate:                 %f" % ((num_replica_exchanges * 100) / monte_carlo_steps)

	#Write stats to a stats file
	stat_file_name = "%s/%s.stat" % (output_path,  remove_trailing_dots(parse_file_name(pdb_file)))
	stat_file_stream = open(stat_file_name, "w")
	stat_file_stream.write("%s\n" % "Printing replica temperature execution matrix:")
	
	#Sort and format the replica exchange matrix.
	for itr in range(num_replicas):
		 replica_temp_execution_list[itr].sort()
		 unique(replica_temp_execution_list[itr])
		 stat_file_stream.write("Replica %d: %s\n" % (itr, replica_temp_execution_list[itr]))

	#Write run time for each step to stats file
	stat_file_stream.write("\n\n%s\n" % "Printing run times for each monte carlo step:")
	count = 1
	for i in mc_step_times:
		stat_file_stream.write("%d %f\n" % (count, i))
		count += 1

	stat_file_stream.close()
        
	sys.exit(0)
