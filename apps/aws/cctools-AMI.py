###############################################################################
# Author Ryan Boccabella for the Cooperative Computing Lab at Notre Dame      #
# Copyright (C) Notre Dame 2015                                               #
#                                                                             #
# Script that will, given a version of cctools and a keypair, create a pulbic #
# Amazon Machine Image in all regions that images can be created in for the   #
# account associated with that keypair.                                       #
#                                                                             #
# Keyfile should be in the following format:                                  #
#                                                                             #
# AWSAccessKeyId=AKIAEXAMPLEEXAMPLEEX                                         #
# AWSSecretKey=EXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMP                       #
#                                                                             #
# which is no different than the keyfile you would download from AWS for an   #
# IAM role or rootkey for an account.                                         #
#                                                                             #
# Options on the size of the machine, which ami to use, or where the user     #
# data script are coming from can be changed in the DEFINE VARIABLES block    #
#                                                                             #
###############################################################################

import boto.ec2
import sys
import os
import urllib.request, urllib.error, urllib.parse
import datetime
import time

# Simple usage and file existence checks
if len(sys.argv) != 5:
	print("usage: "+sys.argv[0]+" <cctools-version-number|cctools-tar.gz> <AWS_Keyfile> <userdata_file> (public|private)")
	sys.exit()

### Check if it's a valid version of cctools ###################
#some 2. versions had -src instead of -source in
#the tar ball name, account for that
source_string="source"
tarballTransferred = False
is_tarball = False
try:
	returned = urllib.request.urlopen(urllib.request.Request("http://ccl.cse.nd.edu/software/files/cctools-"+sys.argv[1]+"-"+source_string+".tar.gz"))
	version_is_good = True
except:
	source_string="src"
	try:
		returned = urllib.request.urlopen(urllib.request.Request("http://ccl.cse.nd.edu/software/files/cctools-"+sys.argv[1]+"-"+source_string+".tar.gz"))
		version_is_good = True
	except:
		version_is_good = False

if not version_is_good:
	if (os.path.isfile(sys.argv[1])):
		is_tarball = True
	
	if not is_tarball:
		print(sys.argv[1] + " is not a valid version of cctools and is not an accessible file that can contain cctools")
		sys.exit()

if sys.argv[4] == "public":
	publicize = True
elif sys.argv[4] == "private":
	publicize = False
else:
	print("fourth argmuent must be either 'public' or 'private'")
	sys.exit()

### Valid version check complete ###############################

#### DEFINE VARIABLES ############################################
instance_type = "t2.micro"

#these two are coupled, 
instance_ami = "ami-9eaa1cf6" #Ubuntu 14.04. Coupled with the default region being "us-east-1"
default_region_name = "us-east-1" #If AWS changes their regions, this is the source of your error.

WAIT_LENGTH = 7  #time (s) between calls to aws api through boto

timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
key_pair_name = "cctools-keypair"+timestamp
sec_group_name = "SSH_From_Anywhere"+timestamp
sec_group_description = "Allows ssh access from any IP"
user_data_filename = sys.argv[3]
max_image_copy_attempts = 5
#### END DEFINE VARIABLES ########################################

### DEFINE FUNCTION(S)############################################
def publicize_amis(connection_amiID_pairs):
	while(len(connection_amiID_pairs) > 0):
		for (connection, ami_id) in connection_amiID_pairs:
			try:
				connection.modify_image_attribute(ami_id, attribute="launchPermission", operation="add", groups=["all"])
				print("Image "+ ami_id  +" should be publicly available in "+connection.region.name)
				connection_amiID_pairs.remove((connection, ami_id))
			except:
				print("Image not ready for publicity in " + connection.region.name)
		time.sleep(WAIT_LENGTH)

	print("Image should be publicly available in all regions we copied it to.")


def move_tarball(key_pair, dns):
	transfer_done = False
	while (not transfer_done):
		os.system("scp -i ./"+key_pair+".pem -oStrictHostKeyChecking=no ./"+sys.argv[1]+" ubuntu@"+dns+":/home/ubuntu/")
#		print "scp -i ./"+key_pair+".pem -oStrictHostKeyChecking=no ./"+sys.argv[1]+" ubuntu@"+dns+":/home/ubuntu/"
		os.system("scp -i ./"+key_pair+".pem -oStrictHostKeyChecking=no ubuntu@"+dns+":/home/ubuntu/GOT_IT .")
		if os.path.isfile("./GOT_IT"):
			os.system("rm ./GOT_IT")
			print("Tarball successfully scp'd")
			transfer_done = True
		else:
			print("Still transferring tarball")
			time.sleep(WAIT_LENGTH)

### END DEFINE FUNCTION(S) #######################################

### Read in AWSAccessKeyID and AWsSecretKey ######################
keyfile = open(sys.argv[2]).read();
(first_line, second_line) = keyfile.split("\n")
access_key_id = first_line.split("=")[1]
secret_key = second_line.split("=")[1]
### Successfully read keys #######################################


###Connect to a region opening an ec2 connection##################
all_regions = boto.ec2.regions()
region_info = boto.ec2.get_region(default_region_name) #all_regions[0] #Do it in whatever the first region

reg_connect = boto.ec2.connect_to_region(region_info.name, aws_access_key_id = access_key_id, aws_secret_access_key = secret_key)
if not reg_connect:
	print("connection failed")
	sys.exit()
###Connection made################################################

###Create key pair in that region, save to local machine ########
new_key_pair = reg_connect.create_key_pair(key_pair_name)
new_key_pair.save(".")
####New key pair created

###Create security group in that region###########################
#security group allows ssh access on port 22 to anyone, but if
#used with an instance that requires a key pair, ssh will require
#that key pair
sec_group = reg_connect.create_security_group(name = sec_group_name, description = sec_group_description)
sec_group.authorize("tcp", 22, 22, "0.0.0.0/0")
###Security group created#########################################

###Create user_data for the instances#############################
#user_data is something passed to the instance to run when it is
#booted the first time after it is spun
#Our user_data will be a script to get and install the appropriate
#version of cctools
try:
	user_data = open(user_data_filename).read()
except:
	print("\nfile {} could not be opened for reading, so the EC2 instance will not be able to run the installation commands.\n".format(user_data_filename))
	sys.exit()

user_data = user_data.replace("VERSION_NUMBER", sys.argv[1])
user_data = user_data.replace("SRC_STRING", source_string)
user_data = user_data.replace("WAIT_ONCE", str(WAIT_LENGTH))
user_data = user_data.replace("WAIT_THRICE", str(3*WAIT_LENGTH + 1))
###User data has been created!!###################################

### Create EC2 instance ##########################################
reservation = reg_connect.run_instances(image_id = instance_ami, key_name=key_pair_name, security_groups = [sec_group.name], user_data = user_data, instance_type = instance_type)
instance_id = reservation.instances[0].id  #should be the first and only instance in the reservation

#when you "create_instances", can create more than one
#so you have a reservation with a list of its instances
#we only have one instance
waiting_on_ec2instance = True
while waiting_on_ec2instance:
	time.sleep(WAIT_LENGTH)
	#rerequest the instance each time so that its attributes can be checked each time.
	#instance attributes would never change if we didn't keep asking Amazon about the instance
	instances = reg_connect.get_only_instances(instance_ids=[instance_id])
	new_instance = instances[0]
	#also, we are the only instance using this key pair, so len(instances)==1 should be true
	dns_name = new_instance.public_dns_name

	print("dns of just started instance is: " + dns_name)
	if len(dns_name) > 0:
		if(is_tarball and not tarballTransferred):
			move_tarball(key_pair_name, dns_name)
			tarballTransferred = True

		os.system("scp -i ./"+key_pair_name+".pem -oStrictHostKeyChecking=no ubuntu@"+dns_name+":/home/ubuntu/IM_DONE .")
		if os.path.isfile("./IM_DONE"):
			os.system("rm ./IM_DONE")
			os.system("rm ./"+key_pair_name+".pem")
			print("Initialization script is finished, waiting for flag to be deleted before snapshotting.")
			waiting_on_ec2instance = False
		else:
			print("Initialization script on new instance is still running")
###EC2 Instance Created and user_data script finished##############

###Set up for and take the machine image##########################

#wait for IM_DONE to be removed from ~ on machine
#cannot use "while try sleep" pattern here, as
#the image could be taken when the script is running 
for i in range(3):
	time.sleep(WAIT_LENGTH)
	print("Still waiting...")

time.sleep(1)

print("Attempting to take image")

if(is_tarball):
	AMI_name_string = sys.argv[1]+" Starter AMI "+timestamp
	AMI_description = "This AMI contains, i nthe ubuntu user's home director, cctools as installed from " + sys.argv[1]
else:
	AMI_name_string = "cctools-"+sys.argv[1]+"-"+source_string+" Starter AMI "+timestamp
	AMI_description = "This AMI contains, in the ubuntu user's home directory, cctools version " + sys.argv[1]

new_ami_id = new_instance.create_image(AMI_name_string, description="This AMI contains, in the ubuntu user's home directory, cctools version " + sys.argv[1])
### Image taken ##################################################

###Make the ami we just created is public ami that can be started on AWS website####################
if publicize:
	publicize_amis([(reg_connect, new_ami_id)])
else:
	print("not publicizing")
###ami is now public in the region we created the instance in#######################################

###Terminate the actual instance, we have the AMI#################
print("Image taken. Trying to terminate instance")
terminated_instances = []
terminated_instances = reg_connect.terminate_instances([new_instance.id])
while(len(terminated_instances) <= 0):
	time.sleep(WAIT_LENGTH)
	terminated_instances = reg_connect.terminate_instances([new_instance.id])

print("Instance terminated.")
###Instance terminated ###########################################

###we need to copy this public ami to all other regions except the one we created it in#############
regions_to_distribute_to = all_regions  
regions_to_distribute_to.pop(0)
num_to_copy = len(regions_to_distribute_to)
#connect to each region to copy to
connections_list=[]
copy_list=[]
copy_dict = {}
for region in regions_to_distribute_to:
	new_connection = boto.ec2.connect_to_region(region.name, aws_access_key_id = access_key_id, aws_secret_access_key = secret_key)
	if not new_connection:
		print("UH OH! No new connection")
	else:
		print("Connected to " + region.name + " for image copying")
		connections_list.append(new_connection)
		copy_list.append(new_connection)
		copy_dict[new_connection.region.name] = 0

to_make_public = []
while (len(copy_list) > 0):
	for connection in copy_list:
		try:
			new_copied_ami_info = connection.copy_image(region_info.name, new_ami_id)
			print("new_copied_ami_id = " + str(new_copied_ami_info.image_id) + " for region " + str(connection.region.name))
			to_make_public.append((connection, new_copied_ami_info.image_id))
			copy_list.remove(connection)
		except:
			attempts=copy_dict[connection.region.name]
			copy_dict[connection.region.name] = attempts + 1
			print("Failed to copy image to " + str(connection.region.name) + " on attempt " + str(attempts) +", connections left: " + str(copy_list))
			if attempts >= max_image_copy_attempts:
				print("Region " + str(connection.region.name) + " is being uncooperative/does not want our public ami, so it won't get it")
				copy_list.remove(connection)
		time.sleep(WAIT_LENGTH)

if publicize:
	publicize_amis(to_make_public)
else:
	print("not publicizing")


