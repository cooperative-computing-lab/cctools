#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6
"""
Umbrella is a tool for specifying and materializing comprehensive execution environments, from the hardware all the way up to software and data.  A user simply invokes Umbrella with the desired task, and Umbrella determines the minimum mechanism necessary to run the task, whether it be direct execution, a system container, a local virtual machine, or submission to a cloud or grid environment.  We present the overall design of Umbrella and demonstrate its use to precisely execute a high energy physics application and a ray-tracing application across many platforms using a combination of Parrot, Chroot, Docker, VMware, Condor, and Amazon EC2.

Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.

Implementation Logics of Different Execution Engines:
	If the sandbox type is Parrot, create the mountlist file and set PARROT_MOUNT_FILE; set PATH; set PARROT_LDSO_PATH if a separate OS image is needed; parrotize the user's command into `parrot_run user_cmd`.
	If the sandbox type is Docker, transfer the OS image into a Docker image; use volume to mount all the software and data dependencies into a container; set PATH; dockerize the user's command into `docker run user_cmd`. To use Docker, a separate OS image is needed.
	If the sandbox type is chroot, create mountpoints for software and data dependencies inside the OS image directory and mount software and data into the OS image, set PATH, chrootize the user's command into `chroot user_cmd`.

Implementation Logic of Dependency Sources:
	HTTP/HTTPS: Download the dependency into Umbrella local cache.
	CVMFS: check whether the mountpoint already exists on the execution node, if yes, do not need to set mountpoint for this dependency and directly process the next dependency; if no, parrot will be used to deliver cvmfs for the application.
		If Parrot is needed to access cvmfs, and the sandbox type is Parrot,
			Do all the work mentioned above for Parrot execution engine + add SITEINFO into mountlist file.
		If Parrot is needed to access cvmfs, and the sandbox type is Docker,
			Do all the work mentioned above for Docker execution engine + add SITEINFO into mountlist file + parrotize the user's command. First parrotize the user's command, then dockerize the user's command.
		If Parrot is needed to access cvmfs, and the sandbox type is chroot,
			Do all the work mentioned above for chroot execution engine + add SITEINFO into mountlist file + parrotize the user's command. First parrotize the user's command, then chrootize the user's command.
	ROOT: If the user expects the root file to be access at runtime without downloading. Umbrella does nothing if a ROOT file through ROOT protocol is needed, because ROOT supports data access during runtime without downloading first. Inside the umbrella specification file, the user only needs to specify the mount_env attribute.
		If the user expects the root file to be downloaded first, then the user needs to specify both the mount_env and mountpoint attributes inside the umbrella specification.

	Git: If the user's application needs git to do `git clone <repo_url>; git checkout <branch_name/commit_id>`, then the user does not need to specify mountpoint attribute inside the umbrella specification.
		If the user's application does not explicitly require git, but umbrella tries to pull some dependencies from a remote git repository, then the user needs to specify both the mount_env and mountpoint attributes inside the umbrella specification.

mount_env and mountpoint:
If only mountpoint is set to A in a specification, the dependency will be downloaded into the umbrella local cache with the file path of D, and a new mountpoint will be added into mount_dict (mount_dict[A] = D).
If only mount_env is set to B in a specification, the dependency will not be downloaded, meta_search will be executed to get one remote storage location, C, of the dependency, a new environment variable will be set (env_para_dict[B] = C).
If mountpoint is set to A and mount_env is set to B in a specification, the dependency will be downloaded into the umbrella local cache with the file path of D, and a new mountpoint will be added into mount_dict (mount_dict[A] = D) and a new environment variable will also be set (env_para_dict[B] = A).

Local path inside the umbrella local cache:
Case 1: the dependency is delivered as a git repository through http/https/git protocol.
        dest = os.path.dirname(sandbox_dir) + "/cache/" + git_commit + '/' + repo_name
	Note: git_commit is optional in the metadata database. If git_commit is not specified in the metadata database, then:
        dest = os.path.dirname(sandbox_dir) + "/cache/" + repo_name
Case 2: the dependency is delivered not as a git repository through http/https protocol.
        dest = os.path.dirname(sandbox_dir) + "/cache/" + checksum + "/" + name
	Note: checksum is required to be specified in the metadata database. If it is not specified, umbrella will complain and exit.
Case 3: SITECONF info necessary for CVMFS cms repository access through Parrot. For this case, we use a hard-coded path.
        dest = os.path.dirname(sandbox_dir) + "/cache/" + checksum + "/SITECONF"
"""

import sys
from stat import *
from pprint import pprint
import subprocess
import platform
import re
import tarfile
import StringIO
from optparse import OptionParser
import os
import hashlib
import difflib
import sqlite3
import shutil
import datetime
import time
import getpass
import grp
import logging
import multiprocessing
import resource
import tempfile

if sys.version_info >= (3,):
	import urllib.request as urllib2
	import urllib.parse as urlparse
else:
	import urllib2
	import urlparse

if sys.version_info > (2,6,):
	import json
else:
	import simplejson as json #json module is introduce in python 2.4.3

#Replace the version of cctools inside umbrella is easy: set cctools_binary_version.
cctools_binary_version = "5.2.0"
cctools_dest = ""

#set cms_siteconf_url to be the url for the siteconf your application depends
#the url and format settings here should be consistent with the function set_cvmfs_cms_siteconf
cms_siteconf_url = "http://ccl.cse.nd.edu/research/data/hep-case-study/2efd5cbb3424fe6b4a74294c84d0fb43/SITECONF.tar.gz"
cms_siteconf_format = "tgz"

tempfile_list = [] #a list of temporary file created by umbrella and need to be removed before umbrella ends.
tempdir_list = [] #a list of temporary dir created by umbrella and need to be removed before umbrella ends.

def subprocess_error(cmd, rc, stdout, stderr):
	"""Print the command, return code, stdout, and stderr; and then directly exit.

	Args:
		cmd: the executed command.
		rc: the return code.
		stdout: the standard output of the command.
		stderr: standard error of the command.

	Returns:
		directly exit the program.
	"""
	cleanup(tempfile_list, tempdir_list)
	sys.exit("`%s` fails with the return code of %d, \nstdout: %s, \nstderr: %s\n" % (cmd, rc, stdout, stderr))

def func_call(cmd):
	""" Execute a command and return the return code, stdout, stderr.

	Args:
		cmd: the command needs to execute using the subprocess module.

	Returns:
		a tuple including the return code, stdout, stderr.
	"""
	logging.debug("Start to execute command: %s", cmd)
	p = subprocess.Popen(cmd, stdout = subprocess.PIPE, shell = True)
	(stdout, stderr) = p.communicate()
	rc = p.returncode
	logging.debug("returncode: %d\nstdout: %s\nstderr: %s", rc, stdout, stderr)
	return (rc, stdout, stderr)

def func_call_withenv(cmd, env_dict):
	""" Execute a command with a special setting of the environment variables and return the return code, stdout, stderr.

	Args:
		cmd: the command needs to execute using the subprocess module.
		env_dict: the environment setting.

	Returns:
		a tuple including the return code, stdout, stderr.
	"""
	logging.debug("Start to execute command: %s", cmd)
	logging.debug("The environment variables for executing the command is:")
	logging.debug(env_dict)

	p = subprocess.Popen(cmd, env = env_dict, stdout = subprocess.PIPE, shell = True)
	(stdout, stderr) = p.communicate()
	rc = p.returncode
	logging.debug("returncode: %d\nstdout: %s\nstderr: %s", rc, stdout, stderr)
	return (rc, stdout, stderr)

def which_exec(name):
	"""The implementation of shell which command

	Args:
		name: the name of the executable to be found.

	Returns:
		If the executable is found, returns its fullpath.
		If PATH is not set, directly exit.
		Otherwise, returns None.
	"""
	if not os.environ.has_key("PATH"):
		cleanup(tempfile_list, tempdir_list)
		logging.critical("The environment variable PATH is not set!")
		sys.exit("The environment variable PATH is not set!")
	for path in os.environ["PATH"].split(":"):
		fullpath = path + '/' + name
		if os.path.exists(fullpath) and os.path.isfile(fullpath):
			return fullpath
	return None

def md5_cal(filename, block_size=2**20):
	"""Calculate the md5sum of a file

	Args:
		filename: the name of the file
		block_size: the size of each block

	Returns:
		If the calculation fails for any reason, directly exit.
		Otherwise, return the md5 value of the content of the file
	"""
	try:
		with open(filename, 'rb') as f:
			md5 = hashlib.md5()
			while True:
				data = f.read(block_size)
				if not data:
					break
				md5.update(data)
			return md5.hexdigest()
	except:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("Computing the checksum of %s fails.", filename)
		sys.exit("md5_cal(" + filename + ") failed.\n")

def url_download(url, dest):
	""" Download url into dest

	Args:
		url: the url needed to be downloaded.
		dest: the path where the content from the url should be put.

	Returns:
		If the url is downloaded successfully, return None;
		Otherwise, directly exit.
	"""
	logging.debug("Start to download %s ....", url)
	cmd = "wget -O %s %s>/dev/null 2>&1" % (dest, url)
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		subprocess_error(cmd, rc, stdout, stderr)

def dependency_download(name, url, checksum, checksum_tool, dest, format_remote_storage, action):
	"""Download a dependency from the url and verify its integrity.

	Args:
		name: the file name of the dependency. If its format is plain text, then filename is the same with the archived name. If its format is tgz, the filename should be the archived name with the trailing .tgz/.tar.gz removed.
		url: the storage location of the dependency.
		checksum: the checksum of the dependency.
		checksum_tool: the tool used to calculate the checksum, such as md5sum.
		dest: the destination of the dependency where the downloaded dependency will be put.
		format_remote_storage: the file format of the dependency, such as .tgz.
		action: the action on the downloaded dependency. Options: none, unpack. "none" leaves the downloaded dependency at it is. "unpack" uncompresses the dependency.

	Returns:
		If the url is a broken link or the integrity of the downloaded data is bad, directly exit.
		Otherwise, return None.
	"""
	print "Download software from %s into the umbrella local cache (%s)" % (url, dest)
	logging.debug("Download software from %s into the umbrella local cache (%s)", url, dest)
	dest_dir = os.path.dirname(dest)
	dest_uncompress = dest #dest_uncompress is the path of the uncompressed-version dependency

	if format_remote_storage == "plain":
		filename = name
	elif format_remote_storage == "tgz":
		filename = "%s.tar.gz" % name

	dest = os.path.join(dest_dir, filename) #dest is the path of the compressed-version dependency

	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)

	if not os.path.exists(dest):
		#download the dependency from the url
		#this method currently will fail when the data size is larger than the memory size, use subprocess + wget can solve it
		url_download(url, dest)
		#if it exists, the uncompressed-version directory will be deleted first
		if action == "unpack" and format_remote_storage != 'plain' and os.path.exists(dest_uncompress):
			shutil.rmtree(dest_uncompress)
			logging.debug("the uncompressed-version directory exists already, first delete it")

	#calculate the checkusm of the compressed-version dependency
	if checksum_tool == "md5sum":
		local_checksum = md5_cal(dest)
		logging.debug("The checksum of %s is: %s", dest, local_checksum)
		if not local_checksum == checksum:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("The version of %s is incorrect! Please first delete it and its unpacked directory!!", dest)
			sys.exit("the version of " + dest + " is incorrect! Please first delete it and its unpacked directory!!\n")
	elif not checksum_tool:
		logging.debug("the checksum of %s is not provided!", url)
	else:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("%s is not supported currently!", checksum_tool)
		sys.exit(checksum_tool + "is not supported currently!")

	#if the uncompressed-version dependency does not exist, uncompress the dependency
	if action == "unpack" and (not os.path.exists(dest_uncompress)) and format_remote_storage == "tgz":
		logging.debug("Uncompressing %s into %s ....", dest, dest_uncompress)
		tfile = tarfile.open(dest, "r:gz")
		tfile.extractall(dest_dir)

def extract_tar(src, dest, format):
	"""Extract a tgz file from src to dest

	Args:
		src: the location of a tgz file
		dest: the location where the uncompressed data will be put
		format: the format the tarball. Such as: tar, tgz

	Returns:
		None
	"""
	if format == "tar":
		tfile = tarfile.open(src, "r")
	elif format == "tgz":
		tfile = tarfile.open(src, "r:gz")
	tfile.extractall(dest)

def meta_search(meta_json, name, id=None):
	"""Search the metadata information of an dependency in the meta_json
	First find all the items with the required name in meta_json.
	Then find the right one whose id satisfied the requirement.
	If no id parameter is problem, then the first matched one will be returned.

	Args:
		meta_json: the json object including all the metadata of dependencies.
		name: the name of the dependency.
		id: the id attribute of the dependency. Defaults to None.

	Returns:
		If one item is found in meta_json, return the item, which is a dictionary.
		If no item satisfied the requirement on meta_json, directly exit.
	"""
	if meta_json.has_key(name):
		if not id:
			for item in meta_json[name]:
				return meta_json[name][item]
		else:
			if meta_json[name].has_key(id):
				return meta_json[name][id]
			else:
				cleanup(tempfile_list, tempdir_list)
				logging.debug("meta_json does not has <%s> with the id <%s>", name, id)
				sys.exit("meta_json does not has <%s> with the id <%s>" % (name, id))
	else:
		cleanup(tempfile_list, tempdir_list)
		logging.debug("meta_json does not include %s", name)
		sys.exit("meta_json does not include %s\n" % name)

def attr_check(item, attr, check_len = 0):
	"""Check and obtain the attr of an item.

	Args:
		item: an item from the metadata database
		attr: an attribute
		check_len: if set to 1, also check whether the length of the attr is > 0; if set to 0, ignore the length checking.

	Returns:
		If the attribute check is successful, directly return the attribute.
		Otherwise, directly exit.
	"""
	logging.debug("check the %s attr of the following item:", attr)
	logging.debug(item)
	if item.has_key(attr):
		if check_len == 1:
			if len(item[attr]) <= 0:
				cleanup(tempfile_list, tempdir_list)
				logging.debug("The %s attr of the item is empty.", attr)
				sys.exit("The %s attr of the item (%s) is empty." % (item, attr))
			else:
				return item[attr][0]
		else:
			return item[attr]
	else:
		cleanup(tempfile_list, tempdir_list)
		logging.debug("This item doesn not have %s attr!", attr)
		sys.exit("the item (%s) does not have %s attr!" % (item, attr))


def cctools_download(sandbox_dir, meta_json, hardware_platform, linux_distro, action):
	"""Download cctools

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		meta_json: the json object including all the metadata of dependencies.
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		linux_distro: the linux distro.  For Example: redhat6, centos6.
		action: the action on the downloaded dependency. Options: none, unpack. "none" leaves the downloaded dependency at it is. "unpack" uncompresses the dependency.

	Returns:
		the path of the downloaded cctools in the umbrella local cache. For example: /tmp/umbrella_test/cache/d19376d92daa129ff736f75247b79ec8/cctools-4.9.0-redhat6-x86_64
	"""
	name = "cctools-%s-%s-%s" % (cctools_binary_version, hardware_platform, linux_distro)
	source = "http://ccl.cse.nd.edu/software/files/%s.tar.gz" % name
	global cctools_dest
	cctools_dest = os.path.dirname(sandbox_dir) + "/cache/" + name
	dependency_download(name, source, None, None, cctools_dest, "tgz", "unpack")
	return cctools_dest

def set_cvmfs_cms_siteconf(sandbox_dir):
	"""Download cvmfs SITEINFO and set its mountpoint.

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.

	Returns:
		cvmfs_cms_siteconf_mountpoint: a string in the format of '/cvmfs/cms.cern.ch/SITECONF/local <SITEINFO dir in the umbrella local cache>/local'
	"""
	dest = os.path.dirname(sandbox_dir) + "/cache/cms_siteconf/SITECONF"
	dependency_download("SITECONF.tar.gz", cms_siteconf_url, "", "", dest, cms_siteconf_format, "unpack")
	cvmfs_cms_siteconf_mountpoint = '/cvmfs/cms.cern.ch/SITECONF/local %s/local' % dest
	return cvmfs_cms_siteconf_mountpoint

def is_dir(path):
	"""Judge whether a path is directory or not.
	If the path is a dir, directly return. Otherwise, exit directly.

	Args:
		path: a path

	Returns:
		None
	"""
	if os.path.isdir(path):
		pass
	else:
		cleanup(tempfile_list, tempdir_list)
		logging.debug("%s is not a directory!", path)
		sys.exit("%s is not a directory!" % path)

def git_dependency_download(repo_url, dest, git_branch, git_commit):
	"""Prepare a dependency from a git repository.
	First check whether dest exist or not: if dest exists, then checkout to git_branch and git_commit;
	otherwise, git clone url, and then checkout to git_branch and git_commit.

	Args:
		repo_url: the url of the remote git repository
		dest: the local directory where the git repository will be cloned into
		git_branch: the branch name of the git repository
		git_commit: the commit id of the repository

	Returns:
		dest: the local directory where the git repository is
	"""
	dest = remove_trailing_slashes(dest)

	scheme, netloc, path, query, fragment = urlparse.urlsplit(repo_url)
	repo_name = os.path.basename(path)
	if repo_name[-4:] == '.git':
		repo_name = repo_name[:-4]

	dest = dest + '/' + repo_name
	if os.path.exists(dest):
		is_dir(dest)
	else:
		dir = os.path.dirname(dest)
		if os.path.exists(dir):
			is_dir(dir)
		else:
			os.makedirs(dir)
		os.chdir(dir)

		if dependency_check('git') == -1:
			cleanup(tempfile_list, tempdir_list)
			sys.exit("Git is not found!")
		cmd = "git clone %s" % repo_url
		rc, stdout, stderr = func_call(cmd)
		if rc != 0:
			subprocess_error(cmd, rc, stdout, stderr)

	os.chdir(dest)
	if git_branch:
		cmd = "git checkout %s" % git_branch
		rc, stdout, stderr = func_call(cmd)
		if rc != 0:
			subprocess_error(cmd, rc, stdout, stderr)

	if git_commit:
		cmd = "git checkout %s" % git_commit
		rc, stdout, stderr = func_call(cmd)
		if rc != 0:
			subprocess_error(cmd, rc, stdout, stderr)
	return dest

def git_dependency_parser(item, repo_url, sandbox_dir):
	"""Parse a git dependency

	Args:
		item: an item from the metadata database
		repo_url: the url of the remote git repository
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.

	Returns:
		dest: the path of the downloaded data dependency in the umbrella local cache.
	"""
	logging.debug("This dependency is stored as a git repository: ")
	logging.debug(item)
	git_branch = ''
	if item.has_key("branch"):
		git_branch = item["branch"]
	git_commit = ''
	if item.has_key("commit"):
		git_commit = item["commit"]
	dest = os.path.dirname(sandbox_dir) + "/cache/" + git_commit
	dest = git_dependency_download(repo_url, dest, git_branch, git_commit)
	return dest

def data_dependency_process(name, id, meta_json, sandbox_dir, action):
	"""Download a data dependency

	Args:
		name: the item name in the data section
		id: the id attribute of the processed dependency
		meta_json: the json object including all the metadata of dependencies.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		action: the action on the downloaded dependency. Options: none, unpack. "none" leaves the downloaded dependency at it is. "unpack" uncompresses the dependency.

	Returns:
		dest: the path of the downloaded data dependency in the umbrella local cache.
	"""
	item = meta_search(meta_json, name, id)
	source = attr_check(item, "source", 1)
	if source[:4] == 'git+':
		dest = git_dependency_parser(item, source[4:], sandbox_dir)
	else:
		checksum = attr_check(item, "checksum")
		format = attr_check(item, "format")
		dest = os.path.dirname(sandbox_dir) + "/cache/" + checksum + "/" + name
		dependency_download(name, source, checksum, "md5sum", dest, format, action)
	return dest

def check_cvmfs_repo(repo_name):
	""" Check whether a cvmfs repo is installed on the host or not

	Args:
		repo_name: a cvmfs repo name. For example: "/cvmfs/cms.cern.ch".

	Returns:
		If the cvmfs repo is installed,  returns the string including the mountpoint of cvmfs cms repo. For example: "/cvmfs/cms.cern.ch".
		Otherwise, return an empty string.
	"""
	logging.debug("Check whether a cvmfs repo is installed on the host or not")
	cmd = "df -h|grep '^cvmfs'|grep "+ "'" + repo_name + "'" + "|rev| cut -d' '  -f1|rev"
	rc, stdout, stderr = func_call(cmd)
	if rc == 0:
		return stdout
	else:
		return ''

def dependency_process(name, id, action, meta_json, sandbox_dir):
	""" Process each explicit and implicit dependency.

	Args:
		name: the item name in the software section
		id: the id attribute of the processed dependency
		action: the action on the downloaded dependency. Options: none, unpack. "none" leaves the downloaded dependency at it is. "unpack" uncompresses the dependency.
		meta_json: the json object including all the metadata of dependencies.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.

	Returns:
		mount_value: the actual storage path of one dependency.
	"""
	mount_value = ''

	item = meta_search(meta_json, name, id)
	source = attr_check(item, "source", 1)
	logging.debug("%s is chosen to deliver %s", source, name)

	if source[:4] == "git+":
		dest = git_dependency_parser(item, source[4:], sandbox_dir)
		mount_value = dest
		cleanup(tempfile_list, tempdir_list)
		sys.exit("this is git source, can not support")
	if source[:5] == "cvmfs":
		pass
	else:
		checksum = attr_check(item, "checksum")
		format = attr_check(item, "format")
		dest = os.path.dirname(sandbox_dir) + "/cache/" + checksum + "/" + name
		dependency_download(name, source, checksum, "md5sum", dest, format, action)
		mount_value = dest
	return mount_value

def env_parameter_init(hardware_spec, kernel_spec, os_spec):
	""" Set the environment parameters according to the specification file.

	Args:
		hardware_spec: the hardware section in the specification for the user's task.
		kernel_spec: the kernel section in the specification for the user's task.
		os_spec: the os section in the specification for the user's task.

	Returns:
		a tuple including the requirements for hardware, kernel and os.
	"""
	hardware_platform = attr_check(hardware_spec, "arch").lower()

	cpu_cores = 1
	if hardware_spec.has_key("cores"):
		cpu_cores = hardware_spec["cores"].lower()

	memory_size = "1GB"
	if hardware_spec.has_key("memory"):
		memory_size = hardware_spec["memory"].lower()

	disk_size = "1GB"
	if hardware_spec.has_key("disk"):
		disk_size = hardware_spec["disk"].lower()

	kernel_name = attr_check(kernel_spec, "name").lower()
	kernel_version = attr_check(kernel_spec, "version").lower()
	kernel_version = re.sub('\s+', '', kernel_version).strip()

	distro_name = attr_check(os_spec, "name").lower()
	distro_version = attr_check(os_spec, "version").lower()

	os_id = ''
	if os_spec.has_key("id"):
		os_id = os_spec["id"]

	index = distro_version.find('.')
	linux_distro = distro_name + distro_version[:index] #example of linux_distro: redhat6
	return (hardware_platform, cpu_cores, memory_size, disk_size, kernel_name, kernel_version, linux_distro, distro_name, distro_version, os_id)

def compare_versions(v1, v2):
	""" Compare two versions, the format of version is: X.X.X

	Args:
		v1: a version.
		v2: a version.

	Returns:
		0 if v1 == v2; 1 if v1 is newer than v2; -1 if v1 is older than v2.
	"""
	list1 = v1.split('.')
	list2 = v2.split('.')
	for i in range(len(list1)):
		list1[i] = int(list1[i])
	for i in range(len(list2)):
		list2[i] = int(list2[i])
	if list1[0] == list2[0]:
		if list1[1] == list2[1]:
			if list1[2] == list2[2]:
				return 0
			elif list1[2] > list2[2]:
				return 1
			else:
				return -1
		elif list1[1] > list2[1]:
			return 1
		else:
			return -1

	elif list1[0] > list2[0]:
		return 1
	else:
		return -1

def	verify_kernel(host_kernel_name, host_kernel_version, kernel_name, kernel_version):
	""" Check whether the kernel version of the host machine matches the requirement.
	The kernel_version format supported for now includes: >=2.6.18; [2.6.18, 2.6.32].

	Args:
		host_kernel_name: the name of the OS kernel of the host machine.
		host_kernel_version: the version of the kernel of the host machine.
		kernel_name: the name of the required OS kernel (e.g., linux). Not case sensitive.
		kernel_version: the version of the required kernel (e.g., 2.6.18).

	Returns:
		If the kernel version of the host machine matches the requirement, return None.
		If the kernel version of the host machine does not match the requirement, directly exit.
	"""
	if host_kernel_name != kernel_name:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("The required kernel name is %s, the kernel name of the host machine is %s!", kernel_name, host_kernel_name)
		sys.exit("The required kernel name is %s, the kernel name of the host machine is %s!\n" % (kernel_name, host_kernel_name))
	if kernel_version[0] == '[':
		list1 = kernel_version[1:-1].split(',')
		if compare_versions(host_kernel_version, list1[0]) >= 0 and compare_versions(host_kernel_version, list1[1]) <= 0:
			logging.debug("The kernel version matches!")
		else:
			cleanup(tempfile_list, tempdir_list)
			logging.debug("The required kernel version is %s, the kernel version of the host machine is %s!", kernel_version, host_kernel_version)
			sys.exit("The required kernel version is %s, the kernel version of the host machine is %s!\n" % (kernel_version, host_kernel_version))
	elif kernel_version[0] == '>':
		if compare_versions(host_kernel_version, kernel_version[2:]) >= 0:
			logging.debug("The kernel version matches!")
		else:
			cleanup(tempfile_list, tempdir_list)
			logging.debug("The required kernel version is %s, the kernel version of the host machine is %s!", kernel_version, host_kernel_version)
			sys.exit("The required kernel version is %s, the kernel version of the host machine is %s!\n" % (kernel_version, host_kernel_version))
	elif kernel_version[0] == '<':
		if compare_versions(host_kernel_version, kernel_version[2:]) <= 0:
			logging.debug("The kernel version matches!")
		else:
			cleanup(tempfile_list, tempdir_list)
			logging.debug("The required kernel version is %s, the kernel version of the host machine is %s!", kernel_version, host_kernel_version)
			sys.exit("The required kernel version is %s, the kernel version of the host machine is %s!\n" % (kernel_version, host_kernel_version))
	else: #the kernel version is a single value
		if compare_versions(host_kernel_version, kernel_version[2:]) == 0:
			logging.debug("The kernel version matches!")
		else:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("The required kernel version is %s, the kernel version of the host machine is %s!", kernel_version, host_kernel_version)
			sys.exit("The required kernel version is %s, the kernel version of the host machine is %s!\n" % (kernel_version, host_kernel_version))

def env_check(sandbox_dir, sandbox_mode, hardware_platform, cpu_cores, memory_size, disk_size, kernel_name, kernel_version):
	""" Check the matching degree between the specification requirement and the host machine.
	Currently check the following item: sandbox_mode, hardware platform, kernel, OS, disk, memory, cpu cores.
	Other things needed to check: software, and data??

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		sandbox_mode: the execution engine.
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		cpu_cores: the number of required  cpus (e.g., 1).
		memory_size: the memory size requirement (e.g., 2GB). Not case sensitive.
		disk_size: the disk size requirement (e.g., 2GB). Not case sensitive.
		kernel_name: the name of the required OS kernel (e.g., linux). Not case sensitive.
		kernel_version: the version of the required kernel (e.g., 2.6.18).

	Returns:
		host_linux_distro: the linux distro of the host machine. For Example: redhat6, centos6.
	"""
	print "Execution environment checking ..."

	if sandbox_mode not in ["docker", "chroot", "parrot"]:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("Currently local execution engine only support three sandbox techniques: docker, chroot or parrot!")
		sys.exit("Currently local execution engine only support three sandbox techniques: docker, chroot or parrot!\n")

	uname_list = platform.uname() #format of uname_list: (system,node,release,version,machine,processor)

	logging.debug("Hardware platform checking ...")
	if hardware_platform != uname_list[4].lower():
		cleanup(tempfile_list, tempdir_list)
		logging.critical("The specification requires %s, but the local machine is %s", hardware_platform, uname_list[4].lower())
		sys.exit("The specification requires " + hardware_platform + ", but the local machine is " + uname_list[4].lower() + "!\n")

	logging.debug("CPU cores checking ...")
	cpu_cores = int(cpu_cores)
	host_cpu_cores = multiprocessing.cpu_count()
	if cpu_cores > host_cpu_cores:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("The specification requires %d cpu cores, but the local machine only has %d cores!", cpu_cores, host_cpu_cores)
		sys.exit("The specification requires %d cpu cores, but the local machine only has %d cores!\n" % (cpu_cores, host_cpu_cores))

	logging.debug("Memory size checking ...")
	memory_size = re.sub('\s+', '', memory_size).strip()
	memory_size = float(memory_size[:-2])

	cmd = "free -tg|grep Total|sed 's/\s\+/ /g'|cut -d' ' -f2"
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		logging.critical("The return code is %d, memory check fail!", rc)
	else:
		host_memory_size = float(stdout)
		if memory_size > host_memory_size:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("The specification requires %.2f GB memory space, but the local machine only has %.2f GB free memory space!", memory_size, host_memory_size)
			sys.exit("The specification requires %.2f GB memory space, but the local machine only has %.2f GB free memory space!" % (memory_size, host_memory_size))

	logging.debug("Disk space checking ...")
	disk_size = re.sub('\s+', '', disk_size).strip()
	disk_size = float(disk_size[:-2])
	st = os.statvfs(sandbox_dir)
	free_disk = float(st.f_bavail * st.f_frsize) / (1024*1024*1024)
	if disk_size > free_disk:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("The specification requires %.2f GB disk space, but the local machine only has %.2f GB free disk space!", disk_size, free_disk)
		sys.exit("The specification requires %.2f GB disk space, but the local machine only has %.2f GB free disk space!" % (disk_size, free_disk))

	#check kernel
	logging.debug("Kernel checking ...")
	host_kernel_name = uname_list[0].lower()
	index = uname_list[2].find('-')
	host_kernel_version = uname_list[2][:index]
	verify_kernel(host_kernel_name, host_kernel_version, kernel_name, kernel_version)

	dist_list = platform.dist()
	logging.debug("The hardware information of the local machine:")
	logging.debug(dist_list)

	#set host_linux_distro. Examples: redhat6, centos6.
	#potential problem: maybe in the future, we need a finer control about the host_linux_distro, like redhat6.5, centos6.5.
	arch_index = uname_list[2].find('ARCH')
	if arch_index != -1:
		host_linux_distro = 'arch'
	else:
		redhat_index = uname_list[2].find('el')
		centos_index = uname_list[2].find('centos')
		if redhat_index != -1:
			dist_version = uname_list[2][redhat_index + 2]
			if centos_index != -1 or dist_list[0].lower() == 'centos':
				host_linux_distro = 'centos' + dist_version
			else:
				host_linux_distro = 'redhat' + dist_version
	logging.debug("The OS distribution information of the local machine: %s", host_linux_distro)

	return host_linux_distro

def parrotize_user_cmd(user_cmd, sandbox_dir, cwd_setting, linux_distro, hardware_platform, meta_json, cvmfs_http_proxy):
	"""Modify the user's command into `parrot_run + the user's command`.
	The cases when this function should be called: (1) sandbox_mode == parrot; (2) sandbox_mode != parrot and cvmfs is needed to deliver some dependencies not installed on the execution node.

	Args:
		user_cmd: the user's command.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		cwd_setting: the current working directory for the execution of the user's command.
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		linux_distro: the linux distro. For Example: redhat6, centos6.
		meta_json: the json object including all the metadata of dependencies.
		cvmfs_http_proxy: HTTP_PROXY environmetn variable used to access CVMFS by Parrot

	Returns:
		the modified version of the user's cmd.
	"""
	#Here we use the cctools meta from the local cache (which includes all the meta including cvmfs, globus, fuse and so on). Even if the user may install cctools by himself on the machine, the configuration of the local installation may be not what we want. For example, the user may just configure like this `./configure --prefix ~/cctools`.
	#4.4 and 4.4 does not support --no-set-foreground feature.
	#user_cmd[0] = dest + "/bin/parrot_run --no-set-foreground /bin/sh -c 'cd " + cwd_setting + "; " + user_cmd[0] + "'"
	if cvmfs_http_proxy:
		user_cmd[0] = "export HTTP_PROXY=" + cvmfs_http_proxy + "; " + cctools_dest + "/bin/parrot_run --no-set-foreground /bin/sh -c 'cd " + cwd_setting + "; " + user_cmd[0] + "'"
	else:
		user_cmd[0] = cctools_dest + "/bin/parrot_run --no-set-foreground /bin/sh -c 'cd " + cwd_setting + "; " + user_cmd[0] + "'"
	logging.debug("The parrotized user_cmd: %s" % user_cmd[0])
	return user_cmd

def chrootize_user_cmd(user_cmd, cwd_setting):
	"""Modify the user's command when the sandbox_mode is chroot. This check should be done after `parrotize_user_cmd`.
	The cases when this function should be called: sandbox_mode == chroot

	Args:
		user_cmd: the user's command.
		cwd_setting: the current working directory for the execution of the user's command.

	Returns:
		the modified version of the user's cmd.
	"""
	#By default, the directory of entering chroot is /. So before executing the user's command, first change the directory to the $PWD environment variable.
	user_cmd[0] = 'chroot / /bin/sh -c "cd %s; %s"' %(cwd_setting, user_cmd[0])
	return user_cmd

def software_install(mount_dict, env_para_dict, software_spec, meta_json, sandbox_dir):
	""" Installation each software dependency specified in the software section of the specification.

	Args:
		mount_dict: a dict including each mounting item in the specification, whose key is the access path used by the user's task; whose value is the actual storage path.
		env_para_dict: the environment variables which need to be set for the execution of the user's command.
		software_spec: the software section of the specification
		meta_json: the json object including all the metadata of dependencies.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.

	Returns:
		None.
	"""

	print "Installing software dependencies ..."

	for item in software_spec:
		# always first check whether the attribute is set or not inside the umbrella specificiation file.
		id = ''
		if software_spec[item].has_key('id'):
			id = software_spec[item]['id']
		mountpoint = ''
		if software_spec[item].has_key('mountpoint'):
			mountpoint = software_spec[item]['mountpoint']
		mount_env = ''
		if software_spec[item].has_key('mount_env'):
			mount_env = software_spec[item]['mount_env']
		action = 'unpack'
		if software_spec[item].has_key('action'):
			action = software_spec[item]['action'].lower()

		if mount_env and not mountpoint:
			result = meta_search(meta_json, item, id)
			env_para_dict[mount_env] =attr_check(result, "source", 1)
		else:
			mount_value = dependency_process(item, id, action, meta_json, sandbox_dir)
			if len(mount_value) > 0:
				logging.debug("Add mountpoint (%s:%s) into mount_dict", mountpoint, mount_value)
				mount_dict[mountpoint] = mount_value

			if mount_env and mountpoint:
				env_para_dict[mount_env] = mountpoint

def data_install(data_spec, meta_json, sandbox_dir, mount_dict, env_para_dict):
	"""Process data section of the specification.
	At the beginning of the function, mount_dict only includes items for software and os dependencies. After this function is done, all the items for data dependencies will be added into mount_dict.

	Args:
		data_spec: the data section of the specification.
		meta_json: the json object including all the metadata of dependencies.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		mount_dict: a dict including each mounting item in the specification, whose key is the access path used by the user's task; whose value is the actual storage path.
		env_para_dict: the environment variables which need to be set for the execution of the user's command.

	Returns:
		None
	"""
	print "Installing data dependencies ..."
	for item in data_spec:
		id = ''
		if data_spec[item].has_key('id'):
			id = data_spec[item]['id']
		mountpoint = ''
		if data_spec[item].has_key('mountpoint'):
			mountpoint = data_spec[item]['mountpoint']
		mount_env = ''
		if data_spec[item].has_key('mount_env'):
			mount_env = data_spec[item]['mount_env']
		action = 'unpack'
		if data_spec[item].has_key('action'):
			action = data_spec[item]['action']

		if mount_env and not mountpoint:
			result = meta_search(meta_json, item, id)
			env_para_dict[mount_env] = attr_check(result, "source", 1)
		else:
			mount_value = data_dependency_process(item, id, meta_json, sandbox_dir, action)
			logging.debug("Add mountpoint (%s:%s) into mount_dict", mountpoint, mount_value)
			mount_dict[mountpoint] = mount_value
			if mount_env and mountpoint:
				env_para_dict[mount_env] = mountpoint

def get_linker_path(hardware_platform, os_image_dir):
	"""Return the path of ld-linux.so within the downloaded os image dependency

	Args:
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		os_image_dir: the path of the OS image inside the umbrella local cache.

	Returns:
		If the dynamic linker is found within the OS image, return its fullpath.
		Otherwise, returns None.
	"""
	#env_list is directly under the directory of the downloaded os image dependency
	if hardware_platform == "x86_64":
		p = os_image_dir + "/lib64/ld-linux-x86-64.so.2"
		if os.path.exists(p):
			return p
		else:
			return None
	else:
		return None

def construct_docker_volume(input_dict, mount_dict, output_f_dict, output_d_dict):
	"""Construct the docker volume parameters based on mount_dict.

	Args:
		input_dict: the setting of input files specified by the --inputs option.
		mount_dict: a dict including each mounting item in the specification, whose key is the access path used by the user's task; whose value is the actual storage path.

	Returns:
		volume_paras: all the `-v` options for the docker command.
	"""
	if "/" in mount_dict:
		del mount_dict["/"] #remove "/" from the mount_dict to avoid messing the root directory of the host machine
	volume_paras = ""
	for key in mount_dict:
		volume_paras = volume_paras + " -v " + mount_dict[key] + ":" + key + " "

	for key in input_dict:
		volume_paras = volume_paras + " -v " + input_dict[key] + ":" + key + " "

	for key in output_f_dict:
		volume_paras = volume_paras + " -v " + output_f_dict[key] + ":" + key + " "

	for key in output_d_dict:
		volume_paras = volume_paras + " -v " + output_d_dict[key] + ":" + key + " "

	return volume_paras

def obtain_path(os_image_dir, sw_mount_dict):
	"""Get the path environment variable from envfile and add the mountpoints of software dependencies into it
	the envfile here is named env_list under the OS image.

	Args:

		os_image_dir: the path of the OS image inside the umbrella local cache.
		sw_mount_dict: a dict only including all the software mounting items.

	Returns:
		path_env: the new value for PATH.
	"""
	path_env = ''

	if os.path.exists(os_image_dir + "/env_list") and os.path.isfile(os_image_dir + "/env_list"):
		with open(os_image_dir + "/env_list", "rb") as f:
			for line in f:
				if line[:5] == 'PATH=':
					path_env = line[5:-1]
					break
	else:
		path_env = '.:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'

	for key in sw_mount_dict:
		path_env = key + "/bin:" + path_env
	return path_env

def transfer_env_para_docker(env_para_dict):
	"""Transfer the env_para_dict into the docker `-e` options.

	Args:
		env_para_dict: the environment variables which need to be set for the execution of the user's command.

	Returns:
		env_options: the docker `-e` options constructed from env_para_dict.
	"""
	env_options = ''
	for key in env_para_dict:
		if key:
			env_options = env_options + ' -e "' + key + '=' + env_para_dict[key] + '" '
	return env_options

def collect_software_bin(host_cctools_path, sw_mount_dict):
	"""Construct the path environment from the mountpoints of software dependencies.
	Each softare meta has a bin subdir containing all its executables.

	Args:
		host_cctools_path: the path of cctools under the umbrella local cache.
		sw_mount_dict: a dict only including all the software mounting items.

	Returns:
		extra_path: the paths which are extracted from sw_mount_dict and host_cctools_path, and needed to be added into PATH.
	"""
	extra_path = ""
	for key in sw_mount_dict:
		if key != '/':
			extra_path += '%s/bin:' % key
	if host_cctools_path:
		extra_path += '%s/bin:' % host_cctools_path
	return extra_path

def in_local_passwd():
	"""Judge whether the current user exists in /etc/passwd.

	Returns:
		If the current user is inside /etc/passwd, returns 'yes'.
		Otherwise, returns 'no'.
	"""
	user_name = getpass.getuser()
	with open('/etc/passwd') as f:
		for line in f:
			if line[:len(user_name)] == user_name:
				logging.debug("%s is included in /etc/passwd!", user_name)
				return 'yes'
	logging.debug("%s is not included in /etc/passwd!", user_name)
	return 'no'

def in_local_group():
	"""Judge whether the current user's group exists in /etc/group.

	Returns:
		If the current user's group exists in /etc/group, returns 'yes'.
		Otherwise, returns 'no'.
	"""
	group_name = grp.getgrgid(os.getgid())[0]
	with open('/etc/group') as f:
		for line in f:
			if line[:len(group_name)] == group_name:
				logging.debug("%s is included in /etc/group!", group_name)
				return 'yes'
	logging.debug("%s is not included in /etc/group!", group_name)
	return 'no'

def create_fake_mount(os_image_dir, sandbox_dir, mount_list, path):
	"""For each ancestor dir B of path (including path iteself), check whether it exists in the rootfs and whether it exists in the mount_list and
	whether it exists in the fake_mount directory inside the sandbox.
	If B is inside the rootfs or the fake_mount dir, do nothing. Otherwise, create a fake directory inside the fake_mount.
	Reason: the reason why we need to guarantee any ancestor dir of a path exists somehow is that `cd` shell builtin does a syscall stat on each level of
	the ancestor dir of a path. Without creating the mountpoint for any ancestor dir, `cd` would fail.

	Args:
		os_image_dir: the path of the OS image inside the umbrella local cache.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		mount_list: a list of mountpoints which already been inside the parrot mountlist file.
		path: a dir path.

	Returns:
		mount_str: a string including the mount items which are needed to added into the parrot mount file.
	"""
	mount_str = ''
	if not path: #if the path is NULL, directly return.
		return
	path_list = []
	tmp_path = path
	while tmp_path != '/':
		path_list.insert(0, tmp_path)
		tmp_path = remove_trailing_slashes(os.path.dirname(tmp_path))
	for item in path_list:
		logging.debug("Judge whether the following mountpoint exists: %s", item)
		fake_mount_path = '%s/fake_mount%s' % (sandbox_dir, item)
		#if item is under localdir, do nothing.
		if item in remove_trailing_slashes(os.path.dirname(sandbox_dir)):
			break
		if not os.path.exists(os_image_dir + item) and item not in mount_list and not os.path.exists(fake_mount_path):
			logging.debug("The mountpoint (%s) does not exist, create a fake mountpoint (%s) for it!", item, fake_mount_path)
			os.makedirs(fake_mount_path)
			mount_str += '%s %s\n' % (item, fake_mount_path)
		else:
			logging.debug("The mountpoint (%s) already exists, do nothing!", item)
	return mount_str

def remove_trailing_slashes(path):
	"""Remove the trailing slashes of a string

	Args:
		path: a path, which can be any string.

	Returns:
		path: the new path without any trailing slashes.
	"""
	while len(path) > 1 and path.endswith('/'):
		path = path[:-1]
	return path

def construct_mountfile_full(sandbox_dir, os_image_dir, mount_dict, input_dict, output_f_dict, output_d_dict, cvmfs_cms_siteconf_mountpoint):
	"""Create the mountfile if parrot is used to create a sandbox for the application and a separate rootfs is needed.
	The trick here is the adding sequence does matter. The latter-added items will be checked first during the execution.

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		os_image_dir: the path of the OS image inside the umbrella local cache.
		mount_dict: all the mount items extracted from the specification file and possible implicit dependencies like cctools.
		input_dict: the setting of input files specified by the --inputs option
		cvmfs_cms_siteconf_mountpoint: a string in the format of '/cvmfs/cms.cern.ch/SITECONF/local <SITEINFO dir in the umbrella local cache>/local'

	Returns:
		the path of the mountfile.
	"""
	mount_list = []
	mountfile_path = sandbox_dir + "/.__mountlist"
	with open(mountfile_path, "wb") as mountfile:
		new_root = mount_dict["/"]
		mountfile.write("/ " + new_root + "\n")
		mount_list.append('/')
		del mount_dict["/"]
		mountfile.write(new_root + " " + new_root + "\n")	#this one is needed to avoid recuisive path resolution.
		mount_list.append(new_root)

		mountfile.write("%s %s\n" % (os.path.dirname(sandbox_dir), os.path.dirname(sandbox_dir)))
		mount_list.append(os.path.dirname(sandbox_dir))

		logging.debug("Adding items from mount_dict into %s", mountfile_path)
		for key in mount_dict:
			#os.path.dirname('/a/b/') is '/a/b'. Therefore, before and after calling dirname, use remove_trailing_slashes to remove the trailing slashes.
			key = remove_trailing_slashes(key)
			mount_str = create_fake_mount(os_image_dir, sandbox_dir, mount_list, remove_trailing_slashes(os.path.dirname(key)))
			if mount_str:
				logging.debug("Adding fake mount items (%s) into %s", mount_str, mountfile_path)
				mountfile.write(mount_str)
			mount_list.append(key)
			mount_list.append(mount_dict[key])
			mountfile.write(key + " " + mount_dict[key] + "\n")
			mountfile.write(mount_dict[key] + " " + mount_dict[key] + "\n")

		for key in output_f_dict:
			mountfile.write(key + " " + output_f_dict[key] + "\n")

		for key in output_d_dict:
			mountfile.write(key + " " + output_d_dict[key] + "\n")

		#common-mountlist includes all the common mountpoint (/proc, /dev, /sys, /mnt, /disc, /selinux)
		if os.path.exists(os_image_dir + "/common-mountlist") and os.path.isfile(os_image_dir + "/common-mountlist"):
			logging.debug("Adding items from %s/common-mountlist into %s", os_image_dir, mountfile_path)
			with open(os_image_dir + "/common-mountlist", "rb") as f:
				for line in f:
					tmplist = line.split(' ')
					item = remove_trailing_slashes(tmplist[0])
					mount_str = create_fake_mount(os_image_dir, sandbox_dir, mount_list, remove_trailing_slashes(os.path.dirname(item)))
					if mount_str:
						logging.debug("Adding fake mount items (%s) into %s", mount_str, mountfile_path)
						mountfile.write(mount_str)
					mount_list.append(tmplist[0])
					mountfile.write(line)

		logging.debug("Add sandbox_dir(%s) into %s", sandbox_dir, mountfile_path)
		mountfile.write(sandbox_dir + ' ' + sandbox_dir + '\n')
		mount_list.append(sandbox_dir)

		logging.debug("Add /etc/hosts and /etc/resolv.conf into %s", mountfile_path)
		mount_str = create_fake_mount(os_image_dir, sandbox_dir, mount_list, '/etc')
		if mount_str:
			logging.debug("Adding fake mount items (%s) into %s", mount_str, mountfile_path)
			mountfile.write(mount_str)
		mountfile.write('/etc/hosts /etc/hosts\n')
		mount_list.append('/etc/hosts')
		mountfile.write('/etc/resolv.conf /etc/resolv.conf\n')
		mount_list.append('/etc/resolv.conf')

		#nd workstation uses NSCD (Name Service Cache Daemon) to deal with passwd, group, hosts services. Here first check whether the current uid and gid is in the /etc/passwd and /etc/group, if yes, use them. Otherwise, construct separate passwd and group files.
		#If the current user name and group can not be found in /etc/passwd and /etc/group, a fake passwd and group file will be constructed under sandbox_dir.
		existed_user = in_local_passwd()
		if existed_user == 'yes':
			logging.debug("Add /etc/passwd into %s", mountfile_path)
			mountfile.write('/etc/passwd /etc/passwd\n')
		else:
			logging.debug("Construct a fake passwd file: .passwd, add .passwd into %s", mountfile_path)
			with open('.passwd', 'w+') as f:
				f.write('%s:x:%d:%d:unknown:%s:%s\n' % (getpass.getuser(), os.getuid(), os.getgid(), sandbox_dir + '/' + getpass.getuser(), os.environ['SHELL']))
			mountfile.write('/etc/passwd %s/.passwd\n' % (sandbox_dir))

			logging.debug("Construct a fake acl file: .__acl, add .__acl into %s", mountfile_path)
			with open('.__acl', 'w+') as acl_file:
				acl_file.write('%s rwlax\n' % getpass.getuser())

		mount_list.append('/etc/passwd')
		#getpass.getuser() returns the login name of the user
		#os.makedirs(getpass.getuser()) #it is not really necessary to create this dir.

		existed_group = in_local_group()
		if existed_group == 'yes':
			logging.debug("Add /etc/group into %s", mountfile_path)
			mountfile.write('/etc/group /etc/group\n')
		else:
			logging.debug("Construct a fake group file: .group, add .group into %s", mountfile_path)
			with open('.group', 'w+') as f:
				f.write('%s:x:%d:%d\n' % (grp.getgrgid(os.getgid())[0], os.getgid(), os.getuid()))
			mountfile.write('/etc/group %s/.group\n' % (sandbox_dir))
		mount_list.append('/etc/group')

		#add /var/run/nscd/socket into mountlist
		mount_str = create_fake_mount(os_image_dir, sandbox_dir, mount_list, '/var/run/nscd')
		if mount_str:
			logging.debug("Adding fake mount items (%s) into %s", mount_str, mountfile_path)
			mountfile.write(mount_str)
		mountfile.write('/var/run/nscd/socket ENOENT\n')
		mount_list.append('/var/run/nscd/socket')

		if os.path.exists(os_image_dir + "/special_files") and os.path.isfile(os_image_dir + "/special_files"):
			logging.debug("Add %s/special_files into %s", os_image_dir, mountfile_path)
			with open(os_image_dir + "/special_files", "rb") as f:
				for line in f:
					tmplist = line.split(' ')
					item = remove_trailing_slashes(tmplist[0])
					mount_str = create_fake_mount(os_image_dir, sandbox_dir, mount_list, remove_trailing_slashes(os.path.dirname(item)))
					if mount_str:
						logging.debug("Adding fake mount items (%s) into %s", mount_str, mountfile_path)
						mountfile.write(mount_str)
					mount_list.append(tmplist[0])
					mountfile.write(line)

		#add the input_dict into mountflie
		logging.debug("Add items from input_dict into %s", mountfile_path)
		for key in input_dict:
			key = remove_trailing_slashes(key)
			mount_str = create_fake_mount(os_image_dir, sandbox_dir, mount_list, remove_trailing_slashes(os.path.dirname(key)))
			if mount_str:
				logging.debug("Adding fake mount items (%s) into %s", mount_str, mountfile_path)
				mountfile.write(mount_str)
			mountfile.write(key + " " + input_dict[key] + "\n")
			mount_list.append(key)

		if cvmfs_cms_siteconf_mountpoint == '':
			logging.debug('cvmfs_cms_siteconf_mountpoint is null')
		else:
			mountfile.write('/cvmfs /cvmfs\n')
			mountfile.write(cvmfs_cms_siteconf_mountpoint + '\n')
			logging.debug('cvmfs_cms_siteconf_mountpoint is not null: %s', cvmfs_cms_siteconf_mountpoint)
	return mountfile_path

def construct_mountfile_cvmfs_cms_siteconf(sandbox_dir, cvmfs_cms_siteconf_mountpoint):
	""" Create the mountfile if chroot and docker is used to execute a CMS application and the host machine does not have cvmfs installed.

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		cvmfs_cms_siteconf_mountpoint: a string in the format of '/cvmfs/cms.cern.ch/SITECONF/local <SITEINFO dir in the umbrella local cache>/local'

	Returns:
		the path of the mountfile.
	"""
	mountfile_path = sandbox_dir + "/.__mountlist"
	with open(mountfile_path, "wb") as f:
		f.write(cvmfs_cms_siteconf_mountpoint + '\n')
		logging.debug('cvmfs_cms_siteconf_mountpoint is not null: %s', cvmfs_cms_siteconf_mountpoint)

	return mountfile_path

def construct_mountfile_easy(sandbox_dir, input_dict, output_f_dict, output_d_dict, mount_dict, cvmfs_cms_siteconf_mountpoint):
	""" Create the mountfile if parrot is used to create a sandbox for the application and a separate rootfs is not needed.
	The trick here is the adding sequence does matter. The latter-added items will be checked first during the execution.

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		mount_dict: all the mount items extracted from the specification file and possible implicit dependencies like cctools.
		input_dict: the setting of input files specified by the --inputs option
		cvmfs_cms_siteconf_mountpoint: a string in the format of '/cvmfs/cms.cern.ch/SITECONF/local <SITEINFO dir in the umbrella local cache>/local'

	Returns:
		the path of the mountfile.
	"""
	mountfile_path = sandbox_dir + "/.__mountlist"
	with open(mountfile_path, "wb") as f:
		for key in input_dict:
			f.write(key + " " + input_dict[key] + "\n")

		for key in output_f_dict:
			f.write(key + " " + output_f_dict[key] + "\n")

		for key in output_d_dict:
			f.write(key + " " + output_d_dict[key] + "\n")

		for key in mount_dict:
			f.write(key + " " + mount_dict[key] + "\n")
			f.write(mount_dict[key] + " " + mount_dict[key] + "\n")

		if cvmfs_cms_siteconf_mountpoint == '':
			logging.debug('cvmfs_cms_siteconf_mountpoint is null')
		else:
			f.write(cvmfs_cms_siteconf_mountpoint + '\n')
			logging.debug('cvmfs_cms_siteconf_mountpoint is not null: %s', cvmfs_cms_siteconf_mountpoint)

	return mountfile_path

def construct_env(sandbox_dir, os_image_dir):
	""" Read env_list inside an OS image and save all the environment variables into a dictionary.

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		os_image_dir: the path of the OS image inside the umbrella local cache.

	Returns:
		env_dict: a dictionary which includes all the environment variables from env_list
	"""
	if os.path.exists(os_image_dir + "/env_list") and os.path.isfile(os_image_dir + "/env_list"):
		with open(os_image_dir + "/env_list", "rb") as f:
			env_dict = {}
			for line in f:
				index = line.find("=")
				key = line[:index]
				value = line[(index+1):-1]
				env_dict[key] = value
			return env_dict
	return {}

def has_docker_image(hardware_platform, distro_name, distro_version, tag):
	"""Check whether the required docker image exists on the local machine or not.

	Args:
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		distro_name: the name of the required OS (e.g., redhat).
		distro_version: the version of the required OS (e.g., 6.5).
		tag: the tag of the expected docker image. tag is os_id

	Returns:
		If the required docker image exists on the local machine, returns 'yes'.
		Otherwise, returns 'no'.
	"""
	name = "%s-%s-%s" %(distro_name, distro_version, hardware_platform)
	cmd = "docker images %s | awk '{print $2}'" % (name)
	logging.debug("Start to run the command: %s", cmd)
	p = subprocess.Popen(cmd, stdout = subprocess.PIPE, shell = True)
	(stdout, stderr) = p.communicate()
	rc = p.returncode
	logging.debug("returncode: %d\nstdout: %s\nstderr: %s", rc, stdout, stderr)
	if rc != 0:
		subprocess_error(cmd, rc, stdout, stderr)
	#str = "\n%s\s+" % (name)
	if stdout.find(tag) == -1:
		return 'no'
	else:
		return 'yes'

def create_docker_image(sandbox_dir, hardware_platform, distro_name, distro_version, tag):
	"""Create a docker image based on the cached os image directory.

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		distro_name: the name of the required OS (e.g., redhat).
		distro_version: the version of the required OS (e.g., 6.5).
		tag: the tag of the expected docker image. tag is os_id

	Returns:
		If the docker image is imported from the tarball successfully, returns None.
		Otherwise, directly exit.
	"""
	name = "%s-%s-%s" %(distro_name, distro_version, hardware_platform)
	location = os.path.dirname(sandbox_dir) + '/cache/' + tag + '/' + name
	#docker container runs as root user, so use the owner option of tar command to set the owner of the docker image
	cmd = 'cd ' + location + '; tar --owner=root -c .|docker import - ' + name + ":" + tag + '; cd -'
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		subprocess_error(cmd, rc, stdout, stderr)

def construct_chroot_mount_dict(sandbox_dir, output_dir, input_dict, need_separate_rootfs, os_image_dir, mount_dict, host_cctools_path):
	"""Construct directory mount list and file mount list for chroot. chroot requires the target mountpoint must be created within the chroot jail.

	Args:
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		output_f_dict: the mappings of output files (key is the file path used by the application; value is the file path the user specifies.)
		output_d_dict: the mappings of output dirs (key is the dir path used by the application; value is the dir path the user specified.)
		input_dict: the setting of input files specified by the --inputs option.
		need_separate_rootfs: whether a separate rootfs is needed to execute the user's command.
		os_image_dir: the path of the OS image inside the umbrella local cache.
		mount_dict: a dict including each mounting item in the specification, whose key is the access path used by the user's task; whose value is the actual storage path.
		host_cctools_path: the path of cctools under the umbrella local cache.

	Returns:
		a tuple includes the directory mount list and the file mount list
	"""
	dir_dict = {}
	file_dict = {}
	logging.debug("need_separate_rootfs: %d", need_separate_rootfs)
	if need_separate_rootfs == 1:
		logging.debug("Add %s into dir_dict of chroot", os_image_dir + "/common-mountlist")
		with open(os_image_dir + "/common-mountlist") as f:
			for line in f:
				index = line.find(' ')
				item = line[:index]
				dir_dict[item] = item

		#special_files includes all the paths of the files which includes all the file paths of special types (block, character, socket, pipe)
		logging.debug("Add %s into dir_dict of chroot", os_image_dir + "/special_files")
		with open(os_image_dir + "/special_files") as f:
			for line in f:
				index = line.find(' ')
				item = line[:index]
				if os.path.exists(item):
					file_dict[item] = item

	if host_cctools_path:
		logging.debug("Add cctools binary (%s) into dir_dict of chroot", host_cctools_path)
		dir_dict[host_cctools_path] = host_cctools_path

	logging.debug("Add sandbox_dir and output_dir into dir_dict of chroot")
	dir_dict[sandbox_dir] = sandbox_dir
	dir_dict[output_dir] = output_dir

	logging.debug("Add items from mount_dict into dir_dict of chroot")
	for key in mount_dict:
		if key != '/':
			value = mount_dict[key]
			mode = os.lstat(value).st_mode
			if S_ISDIR(mode):
				dir_dict[value] = key
			else:
				file_dict[value] = key

	logging.debug("Add /etc/passwd /etc/group /etc/hosts /etc/resolv.conf into file_dict of chroot")
	file_dict['/etc/passwd'] = '/etc/passwd'
	file_dict['/etc/group'] = '/etc/group'
	file_dict['/etc/hosts'] = '/etc/hosts'
	file_dict['/etc/resolv.conf'] = '/etc/resolv.conf'

	logging.debug("Add input_dict into file_dict of chroot")
	for key in input_dict:
		value = input_dict[key]
		mode = os.lstat(value).st_mode
		if S_ISDIR(mode):
			dir_dict[value] = key
		else:
			file_dict[value] = key

	logging.debug("dir_dict:")
	logging.debug(dir_dict)
	logging.debug("file_dict:")
	logging.debug(file_dict)
	return (dir_dict, file_dict)

def chroot_mount_bind(dir_dict, file_dict, sandbox_dir, need_separate_rootfs, hardware_platform, distro_name, distro_version):
	"""Create each target mountpoint under the cached os image directory through `mount --bind`.

	Args:
		dir_dict: a dict including all the directory mountpoints needed to be created inside the OS image.
		file_dict: a dict including all the file mountpoints needed to be created inside the OS image.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		need_separate_rootfs: whether a separate rootfs is needed to execute the user's command.
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		distro_name: the name of the required OS (e.g., redhat).
		distro_version: the version of the required OS (e.g., 6.5).

	Returns:
		If no error happens, returns None.
		Otherwise, directly exit.
	"""
	logging.debug("Use mount --bind to redirect mountpoints")
	if need_separate_rootfs == 1:
		os_image_name = "%s-%s-%s" %(distro_name, distro_version, hardware_platform)
		os_image_path = os.path.dirname(sandbox_dir) + '/cache/' + os_image_name
	else:
		os_image_path = '/'
	#mount --bind -o ro hostdir sandboxdir
	for key in dir_dict:
		jaildir = '%s%s' % (os_image_path, dir_dict[key])
		hostdir = key
		#if jaildir and hostdir are the same, there is no necessary to do mount.
		if jaildir != hostdir:
			if not os.path.exists(jaildir):
				os.makedirs(jaildir)
			cmd = 'mount --bind -o ro %s %s' % (hostdir, jaildir)
			rc, stdout, stderr = func_call(cmd)
			if rc != 0:
				subprocess_error(cmd, rc, stdout, stderr)

	for key in file_dict:
		jailfile = '%s%s' % (os_image_path, file_dict[key])
		hostfile = key
		if jailfile != hostfile:
			if not os.path.exists(jailfile):
				d = os.path.dirname(jailfile)
				if not os.path.exists(d):
					os.makedirs(d)
				with open(jailfile, 'w+') as f:
					pass
			cmd = 'mount --bind -o ro %s %s' % (hostfile, jailfile)
			rc, stdout, stderr = func_call(cmd)
			if rc != 0:
				subprocess_error(cmd, rc, stdout, stderr)

def chroot_post_process(dir_dict, file_dict, sandbox_dir, need_separate_rootfs, hardware_platform, distro_name, distro_version):
	"""Remove all the created target mountpoints within the cached os image directory.
	It is not necessary to change the mode of the output dir, because only the root user can use the chroot method.

	Args:
		dir_dict: a dict including all the directory mountpoints needed to be created inside the OS image.
		file_dict: a dict including all the file mountpoints needed to be created inside the OS image.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		need_separate_rootfs: whether a separate rootfs is needed to execute the user's command.
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		distro_name: the name of the required OS (e.g., redhat).
		distro_version: the version of the required OS (e.g., 6.5).

	Returns:
		If no error happens, returns None.
		Otherwise, directly exit.
	"""
	logging.debug("post process of chroot")
	if need_separate_rootfs == 1:
		os_image_name = "%s-%s-%s" %(distro_name, distro_version, hardware_platform)
		os_image_path = os.path.dirname(sandbox_dir) + '/cache/' + os_image_name
	else:
		os_image_path = '/'

	#file_dict must be processed ahead of dir_dict, because we can not umount a directory if there is another mountpoints created for files under it.
	for key in file_dict:
		jailfile = '%s%s' % (os_image_path, file_dict[key])
		hostfile = key
		if jailfile != hostfile:
			if os.path.exists(jailfile):
				cmd = 'umount -f %s' % (jailfile)
				rc, stdout, stderr = func_call(cmd)
				if rc != 0:
					subprocess_error(cmd, rc, stdout, stderr)

	for key in dir_dict:
		jaildir = '%s%s' % (os_image_path, dir_dict[key])
		hostdir = key
		if jaildir != hostdir:
			if os.path.exists(jaildir):
				cmd = 'umount -f %s' % (jaildir)
				rc, stdout, stderr = func_call(cmd)
				if rc != 0:
					subprocess_error(cmd, rc, stdout, stderr)

			#remove all the empty ancestor directory
				parent_dir = jaildir
				mode = os.lstat(parent_dir).st_mode
				if S_ISDIR(mode):
					while len(os.listdir(parent_dir)) == 0:
						os.rmdir(parent_dir)
						parent_dir = os.path.dirname(parent_dir)

def workflow_repeat(cwd_setting, sandbox_dir, sandbox_mode, output_f_dict, output_d_dict, input_dict, env_para_dict, user_cmd, hardware_platform, host_linux_distro, distro_name, distro_version, need_separate_rootfs, os_image_dir, os_image_id, host_cctools_path, cvmfs_cms_siteconf_mountpoint, mount_dict, sw_mount_dict, meta_json, new_os_image_dir):
	"""Run user's task with the help of the sandbox techniques, which currently inculde chroot, parrot, docker.

	Args:
		cwd_setting: the current working directory for the execution of the user's command.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		sandbox_mode: the execution engine.
		output_f_dict: the mappings of output files (key is the file path used by the application; value is the file path the user specifies.)
		output_d_dict: the mappings of output dirs (key is the dir path used by the application; value is the dir path the user specified.)
		input_dict: the setting of input files specified by the --inputs option.
		env_para_dict: the environment variables which need to be set for the execution of the user's command.
		user_cmd: the user's command.
		hardware_platform: the architecture of the required hardware platform (e.g., x86_64).
		distro_name: the name of the required OS (e.g., redhat).
		distro_version: the version of the required OS (e.g., 6.5).
		need_separate_rootfs: whether a separate rootfs is needed to execute the user's command.
		os_image_dir: the path of the OS image inside the umbrella local cache.
		os_image_id: the id of the OS image.
		host_cctools_path: the path of cctools under the umbrella local cache.
		cvmfs_cms_siteconf_mountpoint: a string in the format of '/cvmfs/cms.cern.ch/SITECONF/local <SITEINFO dir in the umbrella local cache>/local'
		mount_dict: a dict including each mounting item in the specification, whose key is the access path used by the user's task; whose value is the actual storage path.
		sw_mount_dict: a dict only including all the software mounting items.
		meta_json: the json object including all the metadata of dependencies.
		new_os_image_dir: the path of the newly created OS image with all the packages installed by package manager.

	Returns:
		If no error happens, returns None.
		Otherwise, directly exit.
	"""
	#sandbox_dir will be the home directory of the sandbox
	print 'Executing the application ....'
	if not os.path.exists(sandbox_dir):
		os.makedirs(sandbox_dir)
	logging.debug("chdir(%s)", sandbox_dir)
	os.chdir(sandbox_dir) #here, we indeed want to chdir to sandbox_dir, not cwd_setting, to do preparation work like create mountlist file for Parrot.
	#at this point, all the software should be under the cache dir, all the mountpoint of the software should be in mount_dict
	print "Execution engine: %s" % sandbox_mode
	logging.debug("execution engine: %s", sandbox_mode)
	logging.debug("need_separate_rootfs: %d", need_separate_rootfs)
	if sandbox_mode == "chroot":
		if need_separate_rootfs == 1:
			env_dict = construct_env(sandbox_dir, os_image_dir)
		else:
			env_dict = os.environ

		if cvmfs_cms_siteconf_mountpoint:
			item = cvmfs_cms_siteconf_mountpoint.split(' ')[1]
			logging.debug("Adding the siteconf meta (%s) into mount_dict", item)
			mount_dict[item] = item
			logging.debug("Create a parrot mountfile for the siteconf meta (%s)", item)
			env_dict['PARROT_MOUNT_FILE'] = construct_mountfile_cvmfs_cms_siteconf(sandbox_dir, cvmfs_cms_siteconf_mountpoint)

		#traverse the common-mountlist file to create the mountpoint and mount --bind -o ro
		#traverse the special file to create the mountpoint and mount --bind
		#remount /etc/passwd /etc/group /etc/hosts /etc/resolv.conf
		(dir_dict, file_dict) = construct_chroot_mount_dict(sandbox_dir, output_dir, input_dict, need_separate_rootfs, os_image_dir, mount_dict, host_cctools_path)
		chroot_mount_bind(dir_dict, file_dict, sandbox_dir, need_separate_rootfs, hardware_platform, distro_name, distro_version)
		logging.debug("Construct environment variables ....")
		logging.debug("Add software binary into PATH")
		extra_path = collect_software_bin(host_cctools_path, sw_mount_dict)
		env_dict['PATH'] = '%s:%s' % (env_dict['PATH'], extra_path[:-1])
		logging.debug("Add env_para_dict into environment variables")
		for key in env_para_dict:
			env_dict[key] = env_para_dict[key]
		print "Start executing the user's task: %s" % user_cmd[0]
		rc, stdout, stderr = func_call_withenv(user_cmd[0], env_dict)
		chroot_post_process(dir_dict, file_dict, sandbox_dir, 0, hardware_platform, distro_name, distro_version)
		#rename the sandbox_dir to output_dir
		logging.debug("Rename sandbox_dir (%s) to output_dir (%s)", sandbox_dir, output_dir)
		os.rename(sandbox_dir, output_dir)
		print "The output has been put into the output dir: %s" % output_dir
	elif sandbox_mode == "docker":
		if need_separate_rootfs == 1:
			if has_docker_image(hardware_platform, distro_name, distro_version, os_image_id) == 'no':
				logging.debug("Start to construct a docker image from the os image")
				create_docker_image(sandbox_dir, hardware_platform, distro_name, distro_version, os_image_id)
				logging.debug("Finish constructing a docker image from the os image")

			if cvmfs_cms_siteconf_mountpoint:
				item = cvmfs_cms_siteconf_mountpoint.split(' ')[1]
				logging.debug("Adding the siteconf meta (%s) into mount_dict", item)
				mount_dict[item] = item
				logging.debug("Create a parrot mountfile for the siteconf meta (%s)", item)
				env_para_dict['PARROT_MOUNT_FILE'] = construct_mountfile_cvmfs_cms_siteconf(sandbox_dir, cvmfs_cms_siteconf_mountpoint)

			logging.debug("Add a volume item (%s:%s) for the sandbox_dir", sandbox_dir, sandbox_dir)
			#-v /home/hmeng/umbrella_test/output:/home/hmeng/umbrella_test/output
			volume_output = " -v %s:%s " % (sandbox_dir, sandbox_dir)
			#-v /home/hmeng/umbrella_test/cache/git-x86_64-redhat5:/software/git-x86_64-redhat5/
			logging.debug("Start to construct other volumes from input_dict")
			volume_parameters = construct_docker_volume(input_dict, mount_dict, output_f_dict, output_d_dict)

			#-e "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/software/git-x86_64-redhat5/bin"
			logging.debug("Set the environment variables ....")
			path_env = obtain_path(os_image_dir, sw_mount_dict)
			other_envs = transfer_env_para_docker(env_para_dict)

			docker_image_name = "%s-%s-%s" %(distro_name, distro_version, hardware_platform)

			#by default, docker executes user_cmd as the root user, `chown` is used to change the owner of the output dir to be the user who calls `umbrella`
			chown_cmd = 'chown -R %d:%d %s %s %s' % (os.getuid(), os.getgid(), sandbox_dir, ' '.join(output_f_dict), ' '.join(output_d_dict))

			#to count the post processing time, this cmd is split into two commands
			container_name = "umbrella_%s_%s_%s" % (docker_image_name, os_image_id, os.path.basename(sandbox_dir))

			#do not enable `-i` and `-t` option of Docker, it will fail when condor execution engine is chosen.
			#to allow the exit code of user_cmd to be transferred back, seperate the user_cmd and the chown command.
			cmd = 'docker run --name %s %s %s -e "PATH=%s" %s %s:%s /bin/sh -c "cd %s; %s"' % (container_name, volume_output, volume_parameters, path_env, other_envs, docker_image_name, os_image_id, cwd_setting, user_cmd[0])
			print "Start executing the user's task: %s" % cmd
			rc, stdout, stderr = func_call(cmd)

			#docker export container_name > tarball
			if len(new_os_image_dir) > 0:
				if not os.path.exists(new_os_image_dir):
					os.makedirs(new_os_image_dir)
				os_tar = new_os_image_dir + ".tar"

				cmd = "docker export %s > %s" % (container_name, os_tar)
				rc, stdout, stderr = func_call(cmd)
				if rc != 0:
					subprocess_error(cmd, rc, stdout, stderr)

				#uncompress the tarball
				cmd = "tar xf %s -C %s" % (os_tar, new_os_image_dir)
				extract_tar(os_tar, new_os_image_dir, "tar")

			#docker rm container_name
			cmd = "docker rm %s" % (container_name)
			rc, stdout, stderr = func_call(cmd)
			if rc != 0:
				subprocess_error(cmd, rc, stdout, stderr)

			cmd1 = 'docker run --rm %s %s -e "PATH=%s" %s %s:%s %s' % (volume_output, volume_parameters, path_env, other_envs, docker_image_name, os_image_id, chown_cmd)
			rc, stdout, stderr = func_call(cmd1)
			if rc != 0:
				subprocess_error(cmd, rc, stdout, stderr)

		else:
			#if a separate rootfs is not needed to execute the user's cmd, should forcely use other execution engine to run the user cmd.
			cleanup(tempfile_list, tempdir_list)
			logging.debug("Docker execution engine can only be used when a separate rootfs is needed.")
			sys.exit("Docker execution engine can only be used when a separate rootfs is needed.\n")
	elif sandbox_mode == "parrot":
		if need_separate_rootfs == 1:
			logging.debug("Construct environment variables ....")
			env_dict = construct_env(sandbox_dir, os_image_dir)
			env_dict['PWD'] = cwd_setting
			logging.debug("Construct mounfile ....")
			env_dict['PARROT_MOUNT_FILE'] = construct_mountfile_full(sandbox_dir, os_image_dir, mount_dict, input_dict, output_f_dict, output_d_dict, cvmfs_cms_siteconf_mountpoint)
			for key in env_para_dict:
				env_dict[key] = env_para_dict[key]

			#here, setting the linker will cause strange errors.
			logging.debug("Construct dynamic linker path ....")
			result = get_linker_path(hardware_platform, os_image_dir)
			if not result:
				cleanup(tempfile_list, tempdir_list)
				logging.critical("Can not find the dynamic linker inside the os image (%s)!", os_image_dir)
				sys.exit("Can not find the dynamic linker inside the os image (%s)!\n" % os_image_dir)

			env_dict['PARROT_LDSO_PATH'] = result
			env_dict['USER'] = getpass.getuser()
			#env_dict['HOME'] = sandbox_dir + '/' + getpass.getuser()

			logging.debug("Add software binary into PATH")
			extra_path = collect_software_bin(host_cctools_path, sw_mount_dict)
			if "PATH" not in env_dict:
				env_dict['PATH'] = '.:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'
			env_dict['PATH'] = '%s%s' % (extra_path, env_dict['PATH'])

			print "Start executing the user's task: %s" % user_cmd[0]
			rc, stdout, stderr = func_call_withenv(user_cmd[0], env_dict)
		else:
			env_dict = os.environ
			env_dict['PARROT_MOUNT_FILE'] = construct_mountfile_easy(sandbox_dir, input_dict, output_f_dict, output_d_dict, mount_dict, cvmfs_cms_siteconf_mountpoint)
			for key in env_para_dict:
				env_dict[key] = env_para_dict[key]

			if 'PATH' not in env_dict: #if we run umbrella on Condor, Condor will not set PATH by default.
				env_dict['PATH'] = '.:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'
				logging.debug("PATH is empty, forcely set it to be %s", env_dict['PATH'])
			else:
				env_dict['PATH'] = '.' + env_dict['PATH']
				logging.debug("Forcely add '.' into PATH")

			logging.debug("Add software binary into PATH")
			extra_path = collect_software_bin(host_cctools_path, sw_mount_dict)
			env_dict['PATH'] = '%s%s' % (extra_path, env_dict['PATH'])

			print "Start executing the user's task: %s" % user_cmd[0]
			rc, stdout, stderr = func_call_withenv(user_cmd[0], env_dict)

#		logging.debug("Removing the parrot mountlist file and the parrot submit file from the sandbox")
#		if os.path.exists(env_dict['PARROT_MOUNT_FILE']):
#			os.remove(env_dict['PARROT_MOUNT_FILE'])

	else:
		pass

def condor_process(spec_path, spec_json, spec_path_basename, meta_path, sandbox_dir, output_dir, input_list_origin, user_cmd, cwd_setting, condorlog_path, cvmfs_http_proxy):
	"""Process the specification when condor execution engine is chosen

	Args:
		spec_path: the absolute path of the specification.
		spec_json: the json object including the specification.
		spec_path_basename: the file name of the specification.
		meta_path: the path of the json file including all the metadata information.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		output_dir: the output directory.
		input_list_origin: the list of input file paths.
		user_cmd: the user's command.
		cwd_setting: the current working directory for the execution of the user's command.
		condorlog_path: the path of the umbrella log executed on the remote condor execution node.
		cvmfs_http_proxy: HTTP_PROXY environmetn variable used to access CVMFS by Parrot

	Returns:
		If no errors happen, return None;
		Otherwise, directly exit.
	"""
	if not os.path.exists(sandbox_dir):
		os.makedirs(sandbox_dir)

	print "Checking the validity of the umbrella specification ..."
	if spec_json.has_key("hardware") and spec_json["hardware"] and spec_json.has_key("kernel") and spec_json["kernel"] and spec_json.has_key("os") and spec_json["os"]:
		logging.debug("Setting the environment parameters (hardware, kernel and os) according to the specification file ....")
		(hardware_platform, cpu_cores, memory_size, disk_size, kernel_name, kernel_version, linux_distro, distro_name, distro_version, os_id) = env_parameter_init(spec_json["hardware"], spec_json["kernel"], spec_json["os"])
	else:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("this specification is not complete! You must have a hardware section, a kernel section and a os section!")
		sys.exit("this spec has no hardware section\n")

	condor_submit_path = sandbox_dir + "/condor_task.submit"

	print "Constructing Condor submission file according to the umbrella specification ..."
	transfer_inputs = ''
	new_input_options = ''
	logging.debug("Transform input_list_origin into condor attributes ....")
	for item in input_list_origin:
		index_equal = item.find('=')
		access_path = item[:index_equal]
		actual_path = item[(index_equal+1):]
		transfer_inputs += ',%s' % (actual_path)
		new_input_options += '%s=%s,' % (access_path, os.path.basename(actual_path))
	if new_input_options[-1] == ',':
		new_input_options = new_input_options[:-1]
	logging.debug("transfer_input_files: %s, %s", spec_path, transfer_inputs)
	logging.debug("The new_input_options of Umbrella: %s", new_input_options)

	condor_output_dir = tempfile.mkdtemp(dir=".")
	condor_output_dir = os.path.abspath(condor_output_dir)
	condor_log_path = sandbox_dir + '/condor_task.log'

	umbrella_fullpath = which_exec("umbrella")
	if umbrella_fullpath == None:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("Failed to find the executable umbrella. Please modify your $PATH.")
		sys.exit("Failed to find the executable umbrella. Please modify your $PATH.\n")

	logging.debug("The full path of umbrella is: %s" % umbrella_fullpath)

	#find cctools_python
	cmd = 'which cctools_python'
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		subprocess_error(cmd, rc, stdout, stderr)
	cctools_python_path = stdout[:-1]

	condor_submit_file = open(condor_submit_path, "w+")
	condor_submit_file.write('universe = vanilla\n')
	condor_submit_file.write('executable = %s\n' % cctools_python_path)
	if cvmfs_http_proxy:
		condor_submit_file.write('arguments = "./umbrella -s local --spec %s --cvmfs_http_proxy %s --meta %s -l condor_umbrella -i \'%s\' -o %s --log condor_umbrella.log run \'%s\'"\n' % (spec_path_basename, cvmfs_http_proxy, os.path.basename(meta_path), new_input_options, os.path.basename(condor_output_dir), user_cmd[0]))
	else:
		condor_submit_file.write('arguments = "./umbrella -s local --spec %s --meta %s -l condor_umbrella -i \'%s\' -o %s --log condor_umbrella.log run \'%s\'"\n' % (spec_path_basename, os.path.basename(meta_path), new_input_options, os.path.basename(condor_output_dir), user_cmd[0]))
#	condor_submit_file.write('PostCmd = "echo"\n')
#	condor_submit_file.write('PostArguments = "$?>%s/condor_rc"\n' % os.path.basename(condor_output_dir))
	condor_submit_file.write('transfer_input_files = %s, %s, %s, %s%s\n' % (cctools_python_path, umbrella_fullpath, meta_path, spec_path, transfer_inputs))
	condor_submit_file.write('transfer_output_files = %s, condor_umbrella.log\n' % os.path.basename(condor_output_dir))
	condor_submit_file.write('transfer_output_remaps = "condor_umbrella.log=%s"\n' % condorlog_path)

	#the python on the redhat5 machines in the ND condor pool is 2.4. However, umbrella requires python 2.6.* or 2.7*.
	if linux_distro == "redhat5":
		condor_submit_file.write('requirements = TARGET.Arch == "%s" && TARGET.OpSys == "%s" && TARGET.OpSysAndVer == "redhat6"\n' % (hardware_platform, kernel_name))
	else:
		#condor_submit_file.write('requirements = TARGET.Arch == "%s" && TARGET.OpSys == "%s" && TARGET.OpSysAndVer == "%s" && TARGET.has_docker == true\n' % (hardware_platform, kernel_name, linux_distro))
		condor_submit_file.write('requirements = TARGET.Arch == "%s" && TARGET.OpSys == "%s" && TARGET.OpSysAndVer == "%s"\n' % (hardware_platform, kernel_name, linux_distro))
	condor_submit_file.write('environment = PATH=.:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin\n')

	condor_submit_file.write('output = %s/condor_stdout\n' % sandbox_dir)
	condor_submit_file.write('error = %s/condor_stderr\n' % sandbox_dir)
	condor_submit_file.write('log = %s\n' % condor_log_path)

	condor_submit_file.write('should_transfer_files = yes\n')
	condor_submit_file.write('when_to_transfer_output = on_exit\n')
	condor_submit_file.write('queue\n')
	condor_submit_file.close()

	#submit condor job
	print "Submitting the Condor job ..."
	cmd = 'condor_submit ' + condor_submit_path
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		subprocess_error(cmd, rc, stdout, stderr)
	#keep tracking whether condor job is done
	print "Waiting for the job is done ..."
	logging.debug("Waiting for the job is done ...")
	cmd = 'condor_wait %s' % condor_log_path
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		subprocess_error(cmd, rc, stdout, stderr)

	#check the content of condor log file to figure out the exit code of the remote executed umbrella
	remote_rc = 500
	with open(condor_log_path, 'rb') as f:
		content = f.read()
		str = "Normal termination (return value "
		index1 = content.rfind(str)
		index2 = content.find(')', index1)
		remote_rc = int(content[(index1 + len(str)):index2])

	print "The exit code of the remote executed umbrella found in the condor log file (%s) is %d!" % (condor_log_path, remote_rc)
	logging.debug("The exit code of the remote executed umbrella found in the condor log file (%s) is %d!", condor_log_path, remote_rc)

	if remote_rc == 500:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("Can not find the exit code of the remote executed umbrella inside the condor log file (%s)!", condor_log_path)
		sys.exit("Can not find the exit code of the remote executed umbrella inside the condor log file (%s)!" % condor_log_path)
	elif remote_rc != 0:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("The remote umbrella fails and the exit code is %d.", remote_rc)
		sys.exit("The remote umbrella fails and the exit code is %d." % remote_rc)

	logging.debug("the condor jos is done, put the output back into the output directory!")
	print "the condor jobs is done, put the output back into the output directory!"
	#check until the condor job is done, post-processing (put the output back into the output directory)
	#the semantics of condor_output_files only supports transferring a dir from the execution node back to the current working dir (here it is condor_output_dir).
	os.rename(condor_output_dir, output_dir)

	print "move condor_stdout, condor_stderr and condor_task.log from sandbox_dir to output_dir."
	logging.debug("move condor_stdout, condor_stderr and condor_task.log from sandbox_dir to output_dir.")
	os.rename(sandbox_dir + '/condor_stdout', output_dir + '/condor_stdout')
	os.rename(sandbox_dir + '/condor_stderr', output_dir + '/condor_stderr')
	os.rename(sandbox_dir + '/condor_task.log', output_dir + '/condor_task.log')

	print "Remove the sandbox dir"
	logging.debug("Remove the sandbox_dir.")
	shutil.rmtree(sandbox_dir)

	print "The output has been put into the output dir: %s" % output_dir

def decide_instance_type(cpu_cores, memory_size, disk_size, instances):
	""" Compare the required hardware configurations with each instance type, and return the first matched instance type, return 'no' if no matched instance type exist.
	We can rank each instance type in the future, so that in the case of multiple matches exit, the closest matched instance type is returned.

	Args:
		cpu_cores: the number of required  cpus (e.g., 1).
		memory_size: the memory size requirement (e.g., 2GB). Not case sensitive.
		disk_size: the disk size requirement (e.g., 2GB). Not case sensitive.
		instances: the instances section of the ec2 json file.

	Returns:
		If there is no matched instance type, return 'no'.
		Otherwise, returns the first matched instance type.
	"""
	cpu_cores = int(cpu_cores)
	memory_size = int(memory_size[:-2])
	disk_size = int(disk_size[:-2])
	for item in instances:
		j = instances[item]
		inst_mem = int(j["memory"][:-2])
		inst_disk = int(j["disk"][:-2])
		if cpu_cores <= int(j["cores"]) and memory_size <= inst_mem and disk_size <= inst_disk:
			return item
	return 'no'

def ec2_process(spec_path, spec_json, meta_path, meta_json, ec2_path, ec2_json, ssh_key, ec2_key_pair, ec2_security_group, sandbox_dir, output_dir, sandbox_mode, input_list, input_list_origin, env_options, user_cmd, cwd_setting, ec2log_path, cvmfs_http_proxy):
	"""
	Args:
		spec_path: the path of the specification.
		spec_json: the json object including the specification.
		meta_path: the path of the json file including all the metadata information.
		meta_json: the json object including all the metadata of dependencies.
		ec2_path: the path of the json file including all infomration about the ec2 AMIs and instance types.
		ec2_json: the json object corresponding to ec2_path.
		ssh_key: the name the private key file to use when connecting to an instance.
		ec2_key_pair: the path of the key-pair to use when launching an instance.
		ec2_security_group: the security group within which the EC2 instance should be run.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		output_dir: the output directory.
		sandbox_mode: the execution engine.
		input_list: a list including all the absolute path of the input files on the local machine.
		input_list_origin: the list of input file paths.
		env_options: the original `--env` option.
		user_cmd: the user's command.
		cwd_setting: the current working directory for the execution of the user's command.
		ec2log_path: the path of the umbrella log executed on the remote EC2 execution node.
		cvmfs_http_proxy: HTTP_PROXY environmetn variable used to access CVMFS by Parrot

	Returns:
		If no errors happen, return None;
		Otherwise, directly exit.
	"""
	print "Checking the validity of the umbrella specification ..."
	if spec_json.has_key("hardware") and spec_json["hardware"] and spec_json.has_key("kernel") and spec_json["kernel"] and spec_json.has_key("os") and spec_json["os"]:
		(hardware_platform, cpu_cores, memory_size, disk_size, kernel_name, kernel_version, linux_distro, distro_name, distro_version, os_id) = env_parameter_init(spec_json["hardware"], spec_json["kernel"], spec_json["os"])
	else:
		cleanup(tempfile_list, tempdir_list)
		sys.exit("this spec has no hardware section!\n")

	#According to the given specification file, the AMI and the instance type can be identified. os and arch can be used to decide the AMI; cores, memory and disk can be used to decide the instance type.
	#decide the AMI according to (distro_name, distro_version, hardware_platform)
	print "Deciding the AMI according to the umbrella specification ..."
	name = '%s-%s-%s' % (distro_name, distro_version, hardware_platform)
	if ec2_json.has_key(name):
		if os_id == '':
			for item in ec2_json[name]:
				logging.debug("The AMI information is: ")
				logging.debug(ec2_json[name][item])
				ami = ec2_json[name][item]['ami']
				user_name = ec2_json[name][item]['user']
				break
		else:
			if ec2_json[name].has_key(os_id):
				logging.debug("The AMI information is: ")
				logging.debug(ec2_json[name][os_id])
				ami = ec2_json[name][os_id]['ami']
				user_name = ec2_json[name][os_id]['user']
			else:
				cleanup(tempfile_list, tempdir_list)
				logging.critical("%s with the id <%s> is not in the ec2 json file (%s).", name, os_id, ec2_path)
				sys.exit("%s with the id <%s> is not in the ec2 json file (%s)." % (name, os_id, ec2_path))
	else:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("%s is not in the ec2 json file (%s).", name, ec2_path)
		sys.exit("%s is not in the ec2 json file (%s).\n" % (name, ec2_path))

	#decide the instance type according to (cores, memory, disk)
	print "Deciding the instance type according to the umbrella specification ..."
	instances = ec2_json["instances"]
	r = decide_instance_type(cpu_cores, memory_size, disk_size, instances)
	if r == 'no':
		cleanup(tempfile_list, tempdir_list)
		logging.critical("No matched instance type.")
		sys.exit("No matched instance type.\n")
	else:
		logging.debug("the matched instance type exists: %s", r)

	#start the instance and obtain the instance id
	print "Starting an Amazon EC2 instance ..."
	instance_id = get_instance_id(ami, r, ec2_key_pair, ec2_security_group)
	logging.debug("Start the instance and obtain the instance id: %s", instance_id)

	#get the public DNS of the instance_id
	print "Obtaining the public DNS of the Amazon EC2 instance ..."
	public_dns = get_public_dns(instance_id)
	logging.debug("Get the public DNS of the instance_id: %s", public_dns)

	#public_dns = "ec2-52-11-98-91.us-west-2.compute.amazonaws.com"
	#install wget on the instance
	print "Installing wget on the EC2 instance ..."
	logging.debug("Install wget on the instance")
	#here we should judge the os type, yum is used by Fedora, CentOS, and REHL.
	#without `-t` option of ssh, if the username is not root, `ssh + sudo` will get the following error: sudo: sorry, you must have a tty to run sudo.

	#ssh exit code 255: the remote node is down or unavailable
	rc = 300
	while rc != 0:
		if distro_name in ["fedora", "centos", "redhat"]:
			cmd = 'ssh -t -o ConnectionAttempts=5 -o StrictHostKeyChecking=no -o ConnectTimeout=60 -i %s %s@%s \'sudo yum -y install wget\'' % (ssh_key, user_name, public_dns)
		else:
			cleanup(tempfile_list, tempdir_list)
			sys.exit("Currently the supported Linux distributions are redhat, centos and fedora.\n")
		rc, stdout, stderr = func_call(cmd)
		if rc != 0:
			logging.debug("`%s` fails with the return code of %d, \nstdout: %s, \nstderr: %s" % (cmd, rc, stdout, stderr))
			time.sleep(5)

	#python, the python is needed to execute umbrella itself
	print "Installing python 2.6.9 on the instance ..."
	logging.debug("Install python 2.6.9 on the instance.")
	python_name = 'python-2.6.9-%s-%s' % (linux_distro, hardware_platform)
	python_item = meta_search(meta_json, python_name)
	python_url = python_item["source"][0] #get the url of python
	cmd = 'ssh -o ConnectionAttempts=5 -o StrictHostKeyChecking=no -o ConnectTimeout=60 -i %s %s@%s \'wget %s\'' % (ssh_key, user_name, public_dns, python_url)
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		terminate_instance(instance_id)
		subprocess_error(cmd, rc, stdout, stderr)

	scheme, netloc, path, query, fragment = urlparse.urlsplit(python_url)
	python_url_filename = os.path.basename(path)
	cmd = 'ssh -o ConnectionAttempts=5 -o StrictHostKeyChecking=no -o ConnectTimeout=60 -i %s %s@%s \'tar zxvf %s\'' % (ssh_key, user_name, public_dns, python_url_filename)
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		terminate_instance(instance_id)
		subprocess_error(cmd, rc, stdout, stderr)

	#scp umbrella, meta.json and input files to the instance
	print "Sending the umbrella task to the EC2 instance ..."
	logging.debug("scp relevant files into the HOME dir of the instance.")
	input_file_string = ''
	for input_file in input_list:
		input_file_string += input_file + ' '

	umbrella_fullpath = which_exec("umbrella")
	cmd = 'scp -i %s %s %s %s %s %s@%s:' % (ssh_key, umbrella_fullpath, spec_path, meta_path, input_file_string, user_name, public_dns)
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		terminate_instance(instance_id)
		subprocess_error(cmd, rc, stdout, stderr)

	#change the --inputs option to put all the inputs directory in the home dir of the instance
	logging.debug("change the --inputs option to put all the inputs directory in the home dir of the instance")
	new_input_options = ''
	logging.debug("Transform input_list_origin ....")
	for item in input_list_origin:
		index_equal = item.find('=')
		access_path = item[:index_equal]
		actual_path = item[(index_equal+1):]
		new_input_options += '%s=%s,' % (access_path, os.path.basename(actual_path))
	if new_input_options[-1] == ',':
		new_input_options = new_input_options[:-1]
	logging.debug("The new_input_options of Umbrella: %s", new_input_options) #--inputs option

	#find cctools_python
	cmd = 'which cctools_python'
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		subprocess_error(cmd, rc, stdout, stderr)
	cctools_python_path = stdout[:-1]

	#cvmfs_http_proxy
	cvmfs_http_proxy_option = ''
	if cvmfs_http_proxy:
		cvmfs_http_proxy_option = '--cvmfs_http_proxy %s' % cvmfs_http_proxy

	#execute the command on the instance
	print "Executing the user's task on the EC2 instance ..."
	logging.debug("Execute the command on the instance ...")
	if env_options == '':
		cmd = 'ssh -o ConnectionAttempts=5 -o StrictHostKeyChecking=no -o ConnectTimeout=60 -i %s %s@%s "%s/bin/python umbrella %s -s local --spec %s --log ec2_umbrella.log --meta %s -l ec2_umbrella -o output -i \'%s\' run \'%s\'"' % (ssh_key, user_name, public_dns, python_name, cvmfs_http_proxy_option, os.path.basename(spec_path), os.path.basename(meta_path), new_input_options, user_cmd[0])
	else:
		cmd = 'ssh -o ConnectionAttempts=5 -o StrictHostKeyChecking=no -o ConnectTimeout=60 -i %s %s@%s "%s/bin/python umbrella -s local --spec %s --meta %s -l ec2_umbrella -o output -i \'%s\' -e %s run \'%s\'"' % (ssh_key, user_name, public_dns, python_name, cvmfs_http_proxy_option, os.path.basename(spec_path), os.path.basename(meta_path), new_input_options, env_options, user_cmd[0])
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		terminate_instance(instance_id)
		subprocess_error(cmd, rc, stdout, stderr)

	#postprocessing
	print "Transferring the output of the user's task from the EC2 instance back to the local machine ..."
	logging.debug("Create a tarball for the output dir on the instance.")
	cmd = 'ssh -o ConnectionAttempts=5 -o StrictHostKeyChecking=no -o ConnectTimeout=60 -i %s %s@%s \'tar cvzf output.tar.gz output\'' % (ssh_key, user_name, public_dns)
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		terminate_instance(instance_id)
		subprocess_error(cmd, rc, stdout, stderr)

	logging.debug("The instance returns the output.tar.gz to the local machine.")
	cmd = 'scp -i %s %s@%s:output.tar.gz %s/' % (ssh_key, user_name, public_dns, sandbox_dir)
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		terminate_instance(instance_id)
		subprocess_error(cmd, rc, stdout, stderr)

	logging.debug("The instance returns the remote umbrella log file to the local machine.")
	cmd = 'scp -i %s %s@%s:ec2_umbrella.log %s' % (ssh_key, user_name, public_dns, ec2log_path)
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		terminate_instance(instance_id)
		subprocess_error(cmd, rc, stdout, stderr)

	logging.debug("Uncompress output.tar.gz to the output_dir (%s).", output_dir)
	cmd = 'tar zxvf %s/output.tar.gz -C %s' % (sandbox_dir, sandbox_dir)
	rc, stdout, stderr = func_call(cmd)
	if rc != 0:
		terminate_instance(instance_id)
		subprocess_error(cmd, rc, stdout, stderr)

	logging.debug("Rename %s/output to the output_dir (%s).", sandbox_dir, output_dir)
	os.rename('%s/output' % sandbox_dir, output_dir)
	shutil.rmtree(sandbox_dir)

	print "Terminating the EC2 instance ..."
	terminate_instance(instance_id)

	print "The output has been put into the output dir: %s" % output_dir

def obtain_yum_package(spec_json):
	"""Check whether this spec includes a package_manager section, which in turn includes a list attr.

	Args:
		spec_json: the json object including the specification.

	Returns:
		if a package list is specified in the spec_json, return a list of the required package name.
		Otherwise, return None
	"""
	if spec_json.has_key("package_manager") and spec_json["package_manager"] and spec_json["package_manager"].has_key("list"):
		pac_str = spec_json["package_manager"]["list"]
		pac_list = pac_str.split()
		pac_list.sort()
		if len(pac_list) > 0:
			return pac_list
	return None

def cal_new_os_id(sec, old_os_id, pac_list):
	"""Calculate the id of the new OS based on the old_os_id and the package_manager section

	Args:
		sec: the json object including the package_manager section.
		old_os_id: the id of the original os image without any info about package manager.
		pac_list: a list of the required package name.

	Returns:
		md5_value: the md5 value of the string constructed from binding old_os_id and information from the package_manager section.
		install_cmd: the package install cmd, such as: yum -y install python
	"""
	pm_name = attr_check(sec, "name")
	cmd = pm_name + " -y install " + ' '.join(pac_list)
	install_cmd = []
	install_cmd.append(cmd)
	pac_str = ''.join(pac_list)

	config_str = ''
	if sec.has_key("config") and sec["config"]:
		l = []
		for item in sec["config"]:
			id_attr = sec["config"][item]["id"]
			l.append(id_attr)
		l.sort()
		config_str = ''.join(l)

	data = old_os_id + pm_name + pac_str + config_str
	md5 = hashlib.md5()
	md5.update(data)
	md5_value = md5.hexdigest()
	return (md5_value, install_cmd)

def specification_process(spec_json, sandbox_dir, behavior, meta_json, sandbox_mode, output_f_dict, output_d_dict, input_dict, env_para_dict, user_cmd, cwd_setting, cvmfs_http_proxy):
	""" Create the execution environment specified in the specification file and run the task on it.

	Args:
		spec_json: the json object including the specification.
		sandbox_dir: the sandbox dir for temporary files like Parrot mountlist file.
		behavior: the umbrella behavior, such as `run`.
		meta_json: the json object including all the metadata of dependencies.
		sandbox_mode: the execution engine.
		output_f_dict: the mappings of output files (key is the file path used by the application; value is the file path the user specifies.)
		output_d_dict: the mappings of output dirs (key is the dir path used by the application; value is the dir path the user specified.)
		input_dict: the setting of input files specified by the --inputs option.
		env_para_dict: the environment variables which need to be set for the execution of the user's command.
		user_cmd: the user's command.
		cwd_setting: the current working directory for the execution of the user's command.
		cvmfs_http_proxy: HTTP_PROXY environmetn variable used to access CVMFS by Parrot

	Returns:
		None.
	"""
	print "Checking the validity of the umbrella specification ..."
	if spec_json.has_key("hardware") and spec_json["hardware"] and spec_json.has_key("kernel") and spec_json["kernel"] and spec_json.has_key("os") and spec_json["os"]:
		logging.debug("Setting the environment parameters (hardware, kernel and os) according to the specification file ....")
		(hardware_platform, cpu_cores, memory_size, disk_size, kernel_name, kernel_version, linux_distro, distro_name, distro_version, os_id) = env_parameter_init(spec_json["hardware"], spec_json["kernel"], spec_json["os"])
	else:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("this specification is not complete! You must have a hardware section, a kernel section and a os section!")
		sys.exit("this specification is not complete! You must have a hardware section, a kernel section and a os section!\n")

	host_linux_distro =  env_check(sandbox_dir, sandbox_mode, hardware_platform, cpu_cores, memory_size, disk_size, kernel_name, kernel_version)

	#check os
	need_separate_rootfs = 0
	os_image_dir = ''
	if os_id == "":
		if sandbox_mode in ["docker"]:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("the specification does not provide a concrete OS image, but docker execution engine needs a specific OS image!")
			sys.exit("the specification does not provide a concrete OS image, but docker execution engine needs a specific OS image!\n")

		if linux_distro != host_linux_distro:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("the specification does not provide a concrete OS image, and the OS image of the local machine does not matching the requirement!")
			sys.exit("the specification does not provide a concrete OS image, and the OS image of the local machine does not matching the requirement!\n")
		else:
			logging.debug("the specification does not provide a concrete OS image, but the OS image of the local machine matches the requirement!")
			print "the specification does not provide a concrete OS image, but the OS image of the local machine matches the requirement!\n"
	else:
		need_separate_rootfs = 1
		if sandbox_mode in ["destructive"]:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("A separate os image is specified, which does not support the %s sandbox mode!", sandbox_mode)
			sys.exit("A separate os image is specified, which does not support the %s sandbox mode!" % sandbox_mode)

	#check for dependencies which need to be installed by package managers
	pac_list = obtain_yum_package(spec_json)
	if pac_list:
		logging.debug("The spec needs to install yum packages, therefore a sperate root filesystem is needed.")
		print "The spec needs to install yum packages, therefore a sperate root filesystem is needed."
		if sandbox_mode in ["parrot"]:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("Installing packages through package managers requires the root authority! Please choose a different sandbox mode (docker or destructive)!")
			sys.exit("Installing packages through package managers requires the root authority! Please choose a different sandbox mode(docker or destructive)!")

	mount_dict = {}
	cvmfs_cms_siteconf_mountpoint = ''
	host_cctools_path = '' #the path of the cctools binary which is compatible with the host machine under the umbrella cache

	if sandbox_mode in ["parrot"]:
		logging.debug("To use parrot sandbox mode, cctools binary is needed")
		host_cctools_path = cctools_download(sandbox_dir, meta_json, hardware_platform, host_linux_distro, 'unpack')
		logging.debug("Add mountpoint (%s:%s) into mount_dict", host_cctools_path, host_cctools_path)
		mount_dict[host_cctools_path] = host_cctools_path
		parrotize_user_cmd(user_cmd, sandbox_dir, cwd_setting, host_linux_distro, hardware_platform, meta_json, cvmfs_http_proxy)

	if need_separate_rootfs:
		#download the os dependency into the local
		item = '%s-%s-%s' % (distro_name, distro_version, hardware_platform) #example of item here: redhat-6.5-x86_64
		os_image_dir = "%s/cache/%s/%s" % (os.path.dirname(sandbox_dir), os_id, item)
		logging.debug("A separate OS (%s) is needed!", os_image_dir)
		mountpoint = '/'
		action = 'unpack'
		r3 = dependency_process(item, os_id, action, meta_json, sandbox_dir)
		logging.debug("Add mountpoint (%s:%s) into mount_dict for /.", mountpoint, r3)
		mount_dict[mountpoint] = r3

	#check for cvmfs dependency
	is_cms_cvmfs_app = 0

	cvmfs_path = ""
	cvmfs_mountpoint = ""
	result = needCVMFS(spec_json, meta_json)
	if result:
		(cvmfs_path, cvmfs_mountpoint) = result

	if cvmfs_path:
		logging.debug("cvmfs is needed! (%s)", cvmfs_path)
		print "cvmfs is needed! (%s)" % cvmfs_path

		cvmfs_ready = False
		if need_separate_rootfs:
			os_cvmfs_path = "%s%s" % (os_image_dir, cvmfs_mountpoint)
			if os.path.exists(os_cvmfs_path) and os.path.isdir(os_cvmfs_path):
				cvmfs_ready = True
				logging.debug("The os image has /cvmfs/cms.cern.ch!")
				print "The os image has /cvmfs/cms.cern.ch!"

		if not cvmfs_ready:
			local_cvmfs = ""
			local_cvmfs = check_cvmfs_repo(cvmfs_path[7:])
			if len(local_cvmfs) > 0:
				mount_dict[cvmfs_mountpoint] = local_cvmfs
				logging.debug("The cvmfs is installed on the local host, and its mountpoint is: %s", local_cvmfs)
				print "The cvmfs is installed on the local host, and its mountpoint is: %s" % local_cvmfs

			else:
				logging.debug("The cvmfs is not installed on the local host.")
				print "The cvmfs is not installed on the local host."

				if cvmfs_path.find("cms.cern.ch") != -1:
					is_cms_cvmfs_app = 1 #cvmfs is needed to deliver cms.cern.ch repo, and the local host has no cvmfs installed.

					if sandbox_mode in ["destructive"]:
						logging.critical("the % sandbox mode can not deliver cvmfs!", sandbox_mode)
						print "the % sandbox mode can not deliver cvmfs!" % sandbox_mode

					if not cvmfs_http_proxy or len(cvmfs_http_proxy) == 0:
						cleanup(tempfile_list, tempdir_list)
						logging.debug("Access CVMFS through Parrot requires the --cvmfs_http_proxy of umbrella to be set.")
						sys.exit("Access CVMFS through Parrot requires the --cvmfs_http_proxy of umbrella to be set.")

					#currently, if the logic reaches here, only parrot execution engine is allowed.
					cvmfs_cms_siteconf_mountpoint = set_cvmfs_cms_siteconf(sandbox_dir)
					#add cvmfs SITEINFO into mount_dict
					list1 = cvmfs_cms_siteconf_mountpoint.split(' ')
					logging.debug("Add mountpoint (%s:%s) into mount_dict for cvmfs SITEINFO", list1[0], list1[1])
					mount_dict[list1[0]] = list1[1]

					if sandbox_mode != "parrot":
						logging.debug("To use parrot to access cvmfs, cctools binary is needed")
						host_cctools_path = cctools_download(sandbox_dir, meta_json, hardware_platform, linux_distro, 'unpack')
						logging.debug("Add mountpoint (%s:%s) into mount_dict", host_cctools_path, host_cctools_path)
						mount_dict[host_cctools_path] = host_cctools_path
						parrotize_user_cmd(user_cmd, sandbox_dir, cwd_setting, linux_distro, hardware_platform, meta_json, cvmfs_http_proxy)

	if need_separate_rootfs:
		new_os_image_dir = ""
		#if some packages from package managers are needed, ceate a intermediate os image with all the packages ready.
		if pac_list:
			new_sw_sec = spec_json["package_manager"]["config"]
			(new_os_id, pm_cmd) = cal_new_os_id(spec_json["package_manager"], os_id, pac_list)
			new_os_image_dir = "%s/cache/%s/%s" % (os.path.dirname(sandbox_dir), new_os_id, item)

			logging.debug("Installing the package into the image (%s), and create a new image: %s ...", os_image_dir, new_os_image_dir)
			if os.path.exists(new_os_image_dir) and os.path.isdir(new_os_image_dir):
				logging.debug("the new os image already exists!")
				pass
			else:
				logging.debug("the new os image does not exist!")
				new_env_para_dict = {}

				#install dependency specified in the spec_json["package_manager"]["config"] section
				logging.debug('Install dependency specified in the spec_json["package_manager"]["config"] section.')
				software_install(mount_dict, new_env_para_dict, new_sw_sec, meta_json, sandbox_dir)

				#install dependencies through package managers
				logging.debug("Create an intermediate OS image with all the dependencies from package managers ready!")
				workflow_repeat(cwd_setting, sandbox_dir, sandbox_mode, output_f_dict, output_d_dict, input_dict, env_para_dict, pm_cmd, hardware_platform, host_linux_distro, distro_name, distro_version, need_separate_rootfs, os_image_dir, os_id, host_cctools_path, cvmfs_cms_siteconf_mountpoint, mount_dict, mount_dict, meta_json, new_os_image_dir)
				logging.debug("Finishing creating the intermediate OS image!")

			#use the intermidate os image which has all the dependencies from package manager ready as the os image
			os_image_dir = new_os_image_dir
			os_id = new_os_id

	if spec_json.has_key("software") and spec_json["software"]:
		software_install(mount_dict, env_para_dict, spec_json["software"], meta_json, sandbox_dir)
	else:
		logging.debug("this spec does not have software section!")
		software_install(mount_dict, env_para_dict, "", meta_json, sandbox_dir)

	sw_mount_dict = dict(mount_dict) #sw_mount_dict will be used later to config the $PATH
	if spec_json.has_key("data") and spec_json["data"]:
		data_install(spec_json["data"], meta_json, sandbox_dir, mount_dict, env_para_dict)
	else:
		logging.debug("this spec does not have data section!")

	workflow_repeat(cwd_setting, sandbox_dir, sandbox_mode, output_f_dict, output_d_dict, input_dict, env_para_dict, user_cmd, hardware_platform, host_linux_distro, distro_name, distro_version, need_separate_rootfs, os_image_dir, os_id, host_cctools_path, cvmfs_cms_siteconf_mountpoint, mount_dict, sw_mount_dict, meta_json, "")

def dependency_check(item):
	"""Check whether an executable exists or not.

	Args:
		item: the name of the executable to be found.

	Returns:
		If the executable can be found through $PATH, return 0;
		Otherwise, return -1.
	"""
	print "dependency check -- ", item, " "
	result = which_exec(item)
	if result == None:
		logging.debug("Failed to find the executable `%s` through $PATH.", item)
		print "Failed to find the executable `%s` through $PATH." % item
		return -1
	else:
		logging.debug("Find the executable `%s` through $PATH.", item)
		print "Find the executable `%s` through $PATH." % item
		return 0

def get_instance_id(image_id, instance_type, ec2_key_pair, ec2_security_group):
	""" Start one VM instance through Amazon EC2 command line interface and return the instance id.

	Args:
		image_id: the Amazon Image Identifier.
		instance_type: the Amazon EC2 instance type used for the task.
		ec2_key_pair: the path of the key-pair to use when launching an instance.
		ec2_security_group: the security group within which the EC2 instance should be run.

	Returns:
		If no error happens, returns the id of the started instance.
		Otherwise, directly exit.
	"""
	cmd = 'ec2-run-instances %s -t %s -k %s -g %s --associate-public-ip-address true' % (image_id, instance_type, ec2_key_pair, ec2_security_group)
	logging.debug("Starting an instance: %s", cmd)
	p = subprocess.Popen(cmd, stdout = subprocess.PIPE, shell = True)
	(stdout, stderr) = p.communicate()
	rc = p.returncode
	logging.debug("returncode: %d\nstdout: %s\nstderr: %s", rc, stdout, stderr)
	if rc != 0:
		subprocess_error(cmd, rc, stdout, stderr)
	str = "\nINSTANCE"
	index = stdout.find(str)
	if index == -1:
		cleanup(tempfile_list, tempdir_list)
		sys.exit("Fail to get the instance id!")
	else:
		instance_id = stdout[(index+9):(index+20)]
		return instance_id

def terminate_instance(instance_id):
	"""Terminate an instance.

	Args:
		instance_id: the id of the VM instance.

	Returns:
		None.
	"""
	logging.debug("Terminate the ec2 instance: %s", instance_id)
	cmd = 'ec2-terminate-instances %s' % instance_id
	p = subprocess.Popen(cmd, stdout = subprocess.PIPE, shell = True)

def get_public_dns(instance_id):
	"""Get the public dns of one VM instance from Amazon EC2.
	`ec2-run-instances` can not directly return the public dns of the instance, so this function is needed to check the result of `ec2-describe-instances` to obtain the public dns of the instance.

	Args:
		instance_id: the id of the VM instance.

	Returns:
		If no error happens, returns the public dns of the instance.
		Otherwise, directly exit.
	"""
	public_dns = ''
	while public_dns == None or public_dns == '' or public_dns == 'l':
		cmd = 'ec2-describe-instances ' + instance_id
		p = subprocess.Popen(cmd, stdout = subprocess.PIPE, shell = True)
		(stdout, stderr) = p.communicate()
		rc = p.returncode
		logging.debug("returncode: %d\nstdout: %s\nstderr: %s", rc, stdout, stderr)
		if rc != 0:
			subprocess_error(cmd, rc, stdout, stderr)

		str = "\nPRIVATEIPADDRESS"
		index = stdout.find(str)
		if index >= 0:
			index1 = stdout.find("ec2", index + 1)
			if index1 == -1:
				time.sleep(5)
				continue
			public_dns = stdout[index1:-1]
			break
	return public_dns

def add2spec(item, source_dict, target_dict):
	"""Abstract the metadata information (source format checksum size) from source_dict (metadata database) and add these information into target_dict (umbrella spec).
	For any piece of metadata information, if it already exists in target_dict, do nothing; otherwise, add it into the umbrella spec.

	Args:
		item: the name of a dependency
		source_dict: fragment of an Umbrella metadata database
		target_dict: fragement of an Umbrella specficiation

	Returns:
		None
	"""
	#item must exist inside target_dict.
	ident = None
	if source_dict.has_key("checksum"):
		checksum = source_dict["checksum"]
		ident = checksum
		if not target_dict.has_key("id"):
			target_dict["id"] = ident
		if not target_dict.has_key(checksum):
			target_dict["checksum"] = source_dict["checksum"]

	if source_dict.has_key("source"):
		if len(source_dict["source"]) == 0:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("the source attribute of %s can not be empty!" % item)
			sys.exit("the source attribute of %s can not be empty!" % item)
		else:
			source = source_dict["source"][0]

		#if checksum is not provided in source_dict, the first url in the source section will be set to the ident.
		if not ident and not target_dict.has_key("id"):
			target_dict["id"] = source

		if not target_dict.has_key("source"):
			target_dict["source"] = list(source_dict["source"])

	else:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("%s does not have source attribute in the umbrella metadata database!", item)
		sys.exit("%s does not have source attribute in the umbrella metadata database!" % item)

	if source_dict.has_key("format") and not target_dict.has_key("format"):
		target_dict["format"] = source_dict["format"]

	if source_dict.has_key("size") and not target_dict.has_key("size"):
		target_dict["size"] = source_dict["size"]

	if source_dict.has_key("uncompressed_size") and not target_dict.has_key("uncompressed_size"):
		target_dict["uncompressed_size"] = source_dict["uncompressed_size"]

def add2db(item, source_dict, target_dict):
	"""Add the metadata information (source format checksum size) about item from source_dict (umbrella specification) to target_dict (metadata database).
	The item can be identified through two mechanisms: checksum attribute or one source location, which is used when checksum is not applicable for this item.
	If the item has been in the metadata database, do nothing; otherwise, add it, together with its metadata, into the metadata database.

	Args:
		item: the name of a dependency
		source_dict: fragment of an Umbrella specification
		target_dict: fragement of an Umbrella metadata database

	Returns:
		None
	"""
	if not item in target_dict:
		target_dict[item] = {}

	ident = None
	if source_dict.has_key("checksum"):
		checksum = source_dict["checksum"]
		if target_dict[item].has_key(checksum):
			logging.debug("%s has been inside the metadata database!", item)
			return
		ident = checksum
		target_dict[item][ident] = {}
		target_dict[item][ident]["checksum"] = source_dict["checksum"]

	if source_dict.has_key("source"):
		if len(source_dict["source"]) == 0:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("the source attribute of %s can not be empty!" % item)
			sys.exit("the source attribute of %s can not be empty!" % item)
		else:
			source = source_dict["source"][0]

		if target_dict[item].has_key(source):
			logging.debug("%s has been inside the metadata database!", item)
			return

		#if checksum is not provided in source_dict, the first url in the source section will be set to the ident.
		if not ident:
			ident = source
			target_dict[item][ident] = {}

		target_dict[item][ident]["source"] = list(source_dict["source"])
	else:
		cleanup(tempfile_list, tempdir_list)
		logging.critical("%s does not have source attribute in the umbrella specification!", item)
		sys.exit("%s does not have source attribute in the umbrella specification!" % item)

	if source_dict.has_key("format"):
		target_dict[item][ident]["format"] = source_dict["format"]

	if source_dict.has_key("size"):
		target_dict[item][ident]["size"] = source_dict["size"]

	if source_dict.has_key("uncompressed_size"):
		target_dict[item][ident]["uncompressed_size"] = source_dict["uncompressed_size"]

def prune_attr(dict_item, attr_list):
	"""Remove certain attributes from a dict.
	If a specific ttribute does not exist, pass.

	Args:
		dict_item: a dict
		attr_list: a list of attributes which will be removed from the dict.

	Returns:
		None
	"""
	for item in attr_list:
		if dict_item.has_key(item):
			del dict_item[item]

def prune_spec(json_object):
	"""Remove the metadata information from a json file (which represents an umbrella specification).
	Note: the original json file will not be changed by this function.

	Args:
		json_object: a json file representing an umbrella specification

	Returns:
		temp_json: a new json file without metadata information
	"""
	logging.debug("Remove the metadata information from %s.\n", json_object)
	temp_json = dict(json_object)

	attr_list = ["source", "checksum", "format", "size", "uncompressed_size"]
	if temp_json.has_key("os"):
		os_sec = temp_json["os"]
		if os_sec:
			prune_attr(os_sec, attr_list)

	if temp_json.has_key("package_manager") and temp_json["package_manager"] \
		and temp_json["package_manager"].has_key("config") and temp_json["package_manager"]["config"]:
		pm_config_sec = temp_json["package_manager"]["config"]
		if pm_config_sec:
			for item in pm_config_sec:
				prune_attr(pm_config_sec[item], attr_list)

	if temp_json.has_key("software"):
		software_sec = temp_json["software"]
		if software_sec:
			for item in software_sec:
				prune_attr(software_sec[item], attr_list)

	if temp_json.has_key("data"):
		data_sec = temp_json["data"]
		if data_sec:
			for item in data_sec:
				prune_attr(data_sec[item], attr_list)
	return temp_json

def abstract_metadata(spec_json, meta_path):
	"""Abstract metadata information from a self-contained umbrella spec into a metadata database.

	Args:
		spec_json: a dict including the contents from a json file
		meta_path: the path of the metadata database.

	Returns:
		If the umbrella spec is not complete, exit directly.
		Otherwise, return None.
	"""
	hardware_sec = attr_check(spec_json, "hardware")
	hardware_arch = attr_check(hardware_sec, "arch")

	metadata = {}
	os_sec = attr_check(spec_json, "os")
	os_name = attr_check(os_sec, "name")
	os_version = attr_check(os_sec, "version")
	os_item = "%s-%s-%s" % (os_name, os_version, hardware_arch)
	os_item = os_item.lower()
	add2db(os_item, os_sec, metadata)

	if spec_json.has_key("package_manager") and spec_json["package_manager"] \
		and spec_json["package_manager"].has_key("config") and spec_json["package_manager"]["config"]:
		pm_config_sec = spec_json["package_manager"]["config"]
		if pm_config_sec:
			for item in pm_config_sec:
				add2db(item, pm_config_sec[item], metadata)

	if spec_json.has_key("software"):
		software_sec = spec_json["software"]
		if software_sec:
			for item in software_sec:
				add2db(item, software_sec[item], metadata)

	if spec_json.has_key("data"):
		data_sec = spec_json["data"]
		if data_sec:
			for item in data_sec:
				add2db(item, data_sec[item], metadata)

	with open(meta_path, 'w') as f:
		json.dump(metadata, f, indent=4)
		logging.debug("dump the metadata information from the umbrella spec to %s" % meta_path)
		print "dump the metadata information from the umbrella spec to %s" % meta_path

def needCVMFS(spec_json, meta_json):
	"""For each dependency in the spec_json, check whether cvmfs is needed to deliver it.

	Args:
		spec_json: the json object including the specification.
		meta_json: the json object including all the metadata of dependencies.

	Returns:
		if cvmfs is needed, return the cvmfs url. Otherwise, return None
	"""
	for sec_name in ["software", "data", "package_manager"]:
		if spec_json.has_key(sec_name) and spec_json[sec_name]:
			sec = spec_json[sec_name]
			if sec_name == "package_manager" and sec.has_key("config") and sec["config"]:
				sec = sec["config"]

			for item in sec:
				item_id = ""
				if sec[item].has_key("id") and len(sec[item]["id"]) > 0:
					item_id = sec[item]["id"]

				mountpoint = sec[item]["mountpoint"]

				result = meta_search(meta_json, item, item_id)
				if result.has_key("source") and len(result["source"]) > 0:
					url = result["source"][0]
					if url[:5] == "cvmfs":
						return (url, mountpoint)
	return None

def cleanup(filelist, dirlist):
	"""Cleanup the temporary files and dirs created by umbrella

	Args:
		filelist: a list including file paths
		dirlist: a list including dir paths

	Returns:
		None
	"""
	#cleanup the temporary files
	for item in filelist:
		if os.path.exists(item):
			logging.debug("cleanup temporary file: %s", item)
			print "cleanup temporary file: ", item
			os.remove(item)

	#cleanup the temporary dirs
	for item in dirlist:
		if os.path.exists(item):
			logging.debug("cleanup temporary dir: %s", item)
			print "cleanup temporary dir: ", item
			shutil.rmtree(item)

def separatize_spec(spec_json, meta_json, target_type):
	"""Given an umbrella specification and an umbrella metadata database, generate a self-contained umbrella specification or a metadata database only including the informationnecessary for the umbrella spec.
	If the target_type is spec, then generate a self-contained umbrella specification.
	If the target_type is db, then generate a metadata database only including the information necessary for the umbrella spec.

	Args:
		spec_json: the json object including the specification.
		meta_json: the json object including all the metadata of dependencies.
		target_type: the type of the target json file, which can be an umbrella spec or an umbrella metadata db.

	Returns:
		metadata: a json object
	"""
	#pull the metadata information of the spec from the meatadata db to the spec
	if target_type == "spec":
		metadata = dict(spec_json)

	#pull the metadata information of the spec from the metadata db into a separate db
	if target_type == "meta":
		metadata = {}


	hardware_sec = attr_check(spec_json, "hardware")
	hardware_arch = attr_check(hardware_sec, "arch")

	os_sec = attr_check(spec_json, "os")
	os_name = attr_check(os_sec, "name")
	os_version = attr_check(os_sec, "version")
	os_item = "%s-%s-%s" % (os_name, os_version, hardware_arch)
	os_item = os_item.lower()
	ident = None
	if os_sec.has_key("id"):
		ident = os_sec["id"]
	source = meta_search(meta_json, os_item, ident)

	if target_type == "spec":
		add2spec(os_item, source, metadata["os"])
	if target_type == "meta":
		add2db(os_item, source, metadata)

	if spec_json.has_key("package_manager") and spec_json["package_manager"] \
		and spec_json["package_manager"].has_key("config") and spec_json["package_manager"]["config"]:
		pm_config_sec = spec_json["package_manager"]["config"]
		if pm_config_sec:
			for item in pm_config_sec:
				ident = None
				if pm_config_sec[item].has_key("id"):
					ident = pm_config_sec[item]["id"]
				source = meta_search(meta_json, item, ident)
				if target_type == "spec":
					add2spec(os_item, source, metadata["package_manager"]["config"][item])
				if target_type == "meta":
					add2db(item, source, metadata)

	if spec_json.has_key("software"):
		software_sec = spec_json["software"]
		if software_sec:
			for item in software_sec:
				ident = None
				if software_sec[item].has_key("id"):
					ident = software_sec[item]["id"]
				source = meta_search(meta_json, item, ident)
				if target_type == "spec":
					add2spec(os_item, source, metadata["software"][item])
				if target_type == "meta":
					add2db(item, source, metadata)

	if spec_json.has_key("data"):
		data_sec = spec_json["data"]
		if data_sec:
			for item in data_sec:
				ident = None
				if data_sec[item].has_key("id"):
					ident = data_sec[item]["id"]
				source = meta_search(meta_json, item, ident)
				if target_type == "spec":
					add2spec(os_item, source, metadata["data"][item])
				if target_type == "meta":
					add2db(item, source, metadata)

	return metadata

def json2file(filepath, json_item):
	"""Write a json object into a file

	Args:
		filepath: a file path
		json_item: a dict representing a json object

	Returns:
		None
	"""
	with open(filepath, 'w') as f:
		json.dump(json_item, f, indent=4)
		logging.debug("dump a json object from the umbrella spec to %s" % filepath)
		print "dump a json object from the umbrella spec to %s" % filepath

def dir_create(filepath):
	"""Check the validity and existence of a file path. Create the directory for it if necessary. If the file already exists, exit directly.

	Args:
		filepath: a file path

	Returns:
		Exit directly if any error happens.
		Otherwise, returns None.
	"""
	logging.debug("Checking file path: %s", filepath)
	if os.path.exists(filepath):
		cleanup(tempfile_list, tempdir_list)
		logging.debug("The file (%s) already exists, please specify a new path!", filepath)
		sys.exit("The file (%s) already exists, please specify a new path!" % filepath)
	dirpath = os.path.dirname(filepath)

	if not os.path.exists(dirpath):
		os.makedirs(dirpath)
	else:
		if not os.path.isdir(dirpath):
			cleanup(tempfile_list, tempdir_list)
			logging.debug("The basename of the file (%s) is not a directory!\n", dirpath)
			sys.exit("The basename of the file (%s) is not a directory!\n" % dirpath)

def validate_meta(meta_json):
	"""Validate a metadata db.
	The current standard for a valid metadata db is: for each item, the "source" attribute must exist and not be not empty.

	Args:
		meta_json: a dict object representing a metadata db.

	Returns:
		If error happens, return directly with the error info.
		Otherwise, None.
	"""
	logging.debug("Starting validating the metadata db ....\n")
	print "Starting validating the metadata db ...."

	for name in meta_json:
		for ident in meta_json[name]:
			logging.debug("check for %s with the id of %s ...", name, ident)
			print "check for %s with the id of %s ..." % (name, ident)
			attr_check(meta_json[name][ident], "source", 1)

	logging.debug("Finish validating the metadata db ....\n")
	print "Finish validating the metadata db successfully!"

def validate_spec(spec_json, meta_json = None):
	"""Validate a spec_json.

	Args:
		spec_json: a dict object representing a specification.
		meta_json: a dict object representing a metadata db.

	Returns:
		If error happens, return directly with the error info.
		Otherwise, None.
	"""
	logging.debug("Starting validating the spec file ....\n")
	print "Starting validating the spec file ...."

	#validate the following three sections: hardware, kernel and os.
	env_parameter_init(spec_json["hardware"], spec_json["kernel"], spec_json["os"])

	for sec_name in ["software", "data", "package_manager"]:
		if spec_json.has_key(sec_name) and spec_json[sec_name]:
			sec = spec_json[sec_name]
			if sec_name == "package_manager" and sec.has_key("config") and sec["config"]:
				sec = sec["config"]
			for item in sec:
				if (sec[item].has_key("mountpoint") and sec[item]["mountpoint"]) \
						or (sec[item].has_key("mount_env") and sec[item]["mount_env"]):
					pass
				else:
					cleanup(tempfile_list, tempdir_list)
					logging.critical("%s in the %s section should have either <mountpoint> or <mount_env>!\n", item, sec_name)
					sys.exit("%s in the %s section should have either <mountpoint> or <mount_env>!\n" % (item, sec_name))

				if sec[item].has_key("source") and len(sec[item]["source"]) > 0:
					pass
				else:
					if meta_json:
						ident = None
						if sec[item].has_key("id"):
							ident = sec[item]["id"]
						result = meta_search(meta_json, item, ident)
						if result.has_key("source") and len(result["source"]) > 0:
							pass
						else:
							cleanup(tempfile_list, tempdir_list)
							logging.critical("%s in the metadata db should have <source> attr!\n", item)
							sys.exit("%s in the metadata db should have <source> attr!\n", item)
					else:
						cleanup(tempfile_list, tempdir_list)
						logging.critical("%s in the %s section should have <source> attr!\n", item, sec_name)
						sys.exit("%s in the %s section should have <source> attr!\n" % (item, sec_name))
	logging.debug("Finish validating the spec file ....\n")
	print "Finish validating the spec file successfully!"


def main():
	parser = OptionParser(usage="usage: %prog [options] run \"command\"",
						version="%prog CCTOOLS_VERSION")
	parser.add_option("--spec",
					action="store",
					help="The specification json file.",)
	parser.add_option("--meta",
					action="store",
					help="The source of meta information, which can be a local file path (e.g., file:///tmp/meta.json) or url (e.g., http://...).\nIf this option is not provided, the specification will be treated a self-contained specification.",)
	parser.add_option("-l", "--localdir",
					action="store",
					default="./umbrella_test",
					help="The path of directory used for all the cached data and all the sandboxes, the directory can be an existing dir. (By default: ./umbrella_test)",)
	parser.add_option("-o", "--output",
					action="store",
					help="The mappings of outputs in the format of container_path=local_path (i.e., /container/f1=/tmp/output/f1). Multiple mappings should be separated by comma. container_path should be consistent with the semantics of the application, local path must be non-existing.",)
	parser.add_option("-s", "--sandbox_mode",
					action="store",
					default="parrot",
					choices=['parrot', 'destructive', 'docker', 'ec2',],
					help="sandbox mode, which can be parrot, destructive, docker, ec2.)",)
	parser.add_option("-i", "--inputs",
					action="store",
					default='',
					help="The path of input files in the format of access_path=actual_path. i.e, -i '/home/hmeng/file1=/tmp/file2'. access_path must be consistent with the semantics of the provided command, actual_path can be relative or absolute. (By default: '')",)
	parser.add_option("-e", "--env",
					action="store",
					default='',
					help="The environment variable. I.e., -e 'PWD=/tmp'. (By default: '')")
	parser.add_option("--log",
					action="store",
					default="./umbrella.log",
					help="The path of umbrella log file. (By default: ./umbrella.log)",)
	parser.add_option("--cvmfs_http_proxy",
					action="store",
					help="HTTP_PROXY to access cvmfs (Used by Parrot)",)
	parser.add_option("--ec2",
					action="store",
					default='./ec2.json',
					help="The source of ec2 information. (By default: ./ec2.json)",)
	parser.add_option("--condor_log",
					action="store",
					help="The path of the condor umbrella log file. Required for condor execution engines.",)
	parser.add_option("--ec2_log",
					action="store",
					help="The path of the ec2 umbrella log file. Required for ec2 execution engines.",)
	parser.add_option("-g", "--ec2_group",
					action="store",
					help="the security group within which an Amazon EC2 instance should be run. (only for ec2)",)
	parser.add_option("-k", "--ec2_key",
					action="store",
					help="the name of the key pair to use when launching an Amazon EC2 instance. (only for ec2)",)
	parser.add_option("--ec2_sshkey",
					action="store",
					help="the name of the private key file to use when connecting to an Amazon EC2 instance. (only for ec2)",)

	(options, args) = parser.parse_args()
	logfilename = options.log
	if os.path.exists(logfilename) and not os.path.isfile(logfilename):
		sys.exit("The --log option <%s> is not a file!" % logfilename)

	global tempfile_list
	global tempdir_list

	logging.basicConfig(filename=logfilename, level=logging.DEBUG,
        format='%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

	logging.debug("*******Welcome to Umbrella*******")
	logging.debug("Arguments: ")
	logging.debug(sys.argv)

	start = datetime.datetime.now()
	logging.debug("Start time: %s", start)

	logging.debug("Check the validity of the command ....")
	if not args:
		logging.critical("You must provide the behavior and the command!")
		print "You must provide the behavior and the command!\n"
		parser.print_help()
		sys.exit(1)

	user_cmd = []

	behavior = args[0]
	logging.debug("Check the validity of the behavior: %s", behavior)
	behavior_list = ["run", "expand", "filter", "split", "validate"]
	if behavior not in behavior_list:
		logging.critical("%s is not supported by umbrella!", behavior)
		print behavior + " is not supported by umbrella!\n"
		parser.print_help()
		sys.exit(1)

	if behavior in ["run"]:
		sandbox_mode = options.sandbox_mode
		logging.debug("Check the sandbox_mode option: %s", sandbox_mode)
		if sandbox_mode in ["destructive"]:
			if getpass.getuser() != 'root':
				logging.critical("You must be root to use the %s sandbox mode.", sandbox_mode)
				print 'You must be root to use the %s sandbox mode.\n' % (sandbox_mode)
				parser.print_help()
				sys.exit(1)

		#get the absolute path of the localdir directory, which will cache all the data, and store all the sandboxes.
		#to allow the reuse the local cache, the localdir can be a dir which already exists.
		localdir = options.localdir
		localdir = os.path.abspath(localdir)
		logging.debug("Check the localdir option: %s", localdir)

		if not os.path.exists(localdir):
			logging.debug("create the localdir: %s", localdir)
			os.makedirs(localdir)

		sandbox_dir = tempfile.mkdtemp(dir=localdir)
		logging.debug("Create the sandbox_dir: %s", sandbox_dir)

		#add sandbox_dir into tempdir_list
		tempdir_list.append(sandbox_dir)

		#transfer options.env into a dictionary, env_para_dict
		env_para = options.env
		env_para_dict = {}
		if env_para == '':
			logging.debug("The env option is null")
			env_para_list = ''
			env_para_dict = {}
		else:
			logging.debug("Process the env option: %s", env_para)
			env_para = re.sub('\s+', '', env_para).strip()
			env_para_list = env_para.split(',')
			for item in env_para_list:
				index = item.find('=')
				name = item[:index]
				value = item[(index+1):]
				env_para_dict[name] = value
			logging.debug("the dictionary format of the env options (env_para_dict):")
			logging.debug(env_para_dict)

		#get the cvmfs HTTP_PROXY
		cvmfs_http_proxy = options.cvmfs_http_proxy

	if behavior in ["run", "expand", "filter", "split", "validate"]:
		spec_path = options.spec
		if behavior == "validate" and spec_path == None:
			spec_json = None
		else:
			spec_path_basename = os.path.basename(spec_path)
			logging.debug("Start to read the specification file: %s", spec_path)
			if not os.path.isfile(spec_path):
				logging.critical("The specification json file (%s) does not exist! Please refer the -c option.", spec_path)
				print "The specification json file does not exist! Please refer the -c option.\n"
				parser.print_help()
				sys.exit(1)

			with open(spec_path) as f: #python 2.4 does not support this syntax: with open () as
				spec_json = json.load(f)
				if behavior in ["run"]:
					user_cmd = args[1:]
					if len(user_cmd) == 0:
						if spec_json.has_key("cmd") and len(spec_json["cmd"]) > 0:
							user_cmd.append(spec_json["cmd"])
						else:
							user_cmd.append("/bin/sh") #set the user_cmd to be default: /bin/sh

					logging.debug("The user's command is: %s", user_cmd)

					#if the spec file has environ seciton, merge the variables defined in it into env_para_dict
					if spec_json.has_key("environ") and spec_json["environ"]:
						logging.debug("The specification file has environ section, update env_para_dict ....")
						spec_env = spec_json["environ"]
						for key in spec_env:
							env_para_dict[key] = spec_env[key]
						logging.debug("env_para_dict:")
						logging.debug(env_para_dict)

	if behavior in ["run"]:
		if 'PWD' in env_para_dict:
			cwd_setting = env_para_dict['PWD']
			logging.debug("PWD environment variable is set explicitly: %s", cwd_setting)
		else:
			cwd_setting = sandbox_dir
			env_para_dict['PWD'] = cwd_setting
			logging.debug("PWD is not set explicitly, use sandbox_dir (%s) as PWD", cwd_setting)

		#get the absolute path of each input file
		input_files = options.inputs
		input_files = re.sub( '\s+', '', input_files).strip() #remove all the whitespaces within the inputs option

		input_list = []
		input_dict = {}

		if input_files == '':
			input_list_origin = ''
			input_list = []
			input_dict = {}
			logging.debug("the inputs options is null")
		else:
			logging.debug("The inputs option: %s", input_files)
			input_list_origin = input_files.split(',')
			for item in input_list_origin:
				index = item.find('=')
				access_path = item[:index]
				actual_path = item[(index+1):]
				if access_path[0] != '/':
					access_path = os.path.join(cwd_setting, access_path)
				actual_path = os.path.abspath(actual_path)
				input_dict[access_path] = actual_path
				input_list.append(actual_path) #get the absolute path of each input file and add it into input_list
			logging.debug("The list version of the inputs option: ")
			logging.debug(input_list)
			logging.debug("The dict version of the inputs option: ")
			logging.debug(input_dict)

		#get the absolute path of each output file
		output_dir = options.output
		output_dict = {}
		output_f_dict = {}
		output_d_dict = {}
		if output_dir and len(output_dir) > 0:
			output_dir = re.sub( '\s+', '', output_dir).strip() #remove all the whitespaces within the inputs option
			if output_dir == "":
				logging.debug("the output option is null!")
			else:
				logging.debug("the output option: %s", output_dir)
				outputs = output_dir.split(',')
				for item in outputs:
					index = item.find('=')
					access_path = item[:index]
					actual_path = item[(index+1):]
					if access_path[0] != '/':
						logging.critical("the path of an output should be absolute!")
						sys.exit("the path of an output should be absolute!")
					actual_path = os.path.abspath(actual_path)
					output_dict[access_path] = actual_path

		if len(output_dict) > 0:
			if spec_json.has_key("output"):
				files = []
				dirs = []
				if spec_json["output"].has_key("files"):
					files = spec_json["output"]["files"]

				if spec_json["output"].has_key("dirs"):
					dirs = spec_json["output"]["dirs"]

				for key in output_dict.keys():
					if key in files:
						output_f_dict[key] = output_dict[key]
					elif key in dirs:
						output_d_dict[key] = output_dict[key]
					else:
						logging.critical("the output file (%s) is not specified in the spec file!", key)
						sys.exit("the output file (%s) is not specified in the spec file!" % key)
			else:
				logging.critical("the specification does not have a output section!")
				sys.exit("the specification does not have a output section!")

		del output_dict

		for f in output_f_dict.values():
			if not os.path.exists(f):
				logging.debug("create the output file: %s", f)
				d = os.path.dirname(f)
				if not os.path.exists(d):
					os.makedirs(d)
				elif not os.path.isdir(d):
					logging.critical("the parent path of the output file (%s) is not a directory!", f)
					sys.exit("the parent path of the output file (%s) is not a directory!" % f)
				else:
					pass
				new_file = open(f, 'a')
				new_file.close()
			elif len(f) != 0:
				cleanup(tempfile_list, tempdir_list)
				logging.critical("the output file (%s) already exists!", f)
				sys.exit("the output file (%s) already exists!\n" % f)
			else:
				pass

		for d in output_d_dict.values():
			if not os.path.exists(d):
				logging.debug("create the output dir: %s", d)
				os.makedirs(d)
			elif len(d) != 0:
				cleanup(tempfile_list, tempdir_list)
				logging.critical("the output dir (%s) already exists!", d)
				sys.exit("the output dir(%s) already exists!" % d)
			else:
				pass

	meta_json = None
	if behavior in ["run", "expand", "filter", "validate"]:
		"""
		meta_path is optional. If set, it provides the metadata information for the dependencies.
		If not set, the umbrella specification is treated as a self-contained specification.
		meta_path can be in either file:///filepath format or a http/https url like http:/ccl.cse.nd.edu/...
		"""
		meta_path = options.meta
		if meta_path:
			if meta_path[:7] == "file://":
				meta_path = meta_path[7:]
				logging.debug("Check the metatdata database file: %s", meta_path)
				if not os.path.exists(meta_path):
					cleanup(tempfile_list, tempdir_list)
					logging.critical("the metatdata database file (%s) does not exist!", meta_path)
					sys.exit("the metatdata database file (%s) does not exist!" % meta_path)
			elif meta_path[:7] == "http://" or meta_path[:8] == "https://":
				url = meta_path
				if behavior in ["run"]:
					meta_path = '%s/meta.json' % (sandbox_dir)
				if behavior in ["expand", "filter", "validate"]:
					#create a tempfile under /tmp
					(fd, meta_path) = tempfile.mkstemp()
					tempfile_list.append(meta_path)
					os.close(fd)
					logging.debug("Creating a temporary file (%s) to hold the metadata file specified by the --meta options!", meta_path)
				logging.debug("Download metadata database from %s into %s", url, meta_path)
				print "Download metadata database from %s into %s" % (url, meta_path)
				url_download(url, meta_path)
			else:
				cleanup(tempfile_list, tempdir_list)
				logging.debug("The format of the --meta option should be file:///... or http://... or https://... \n")
				sys.exit("The format of the --meta option should be file:///... or http://... or https://... \n")
		else:
			if behavior in ["run"]:
				#the provided specification should be self-contained.
				# One solution is to change all the current implementation of Umbrella to check whether the metadata information is included in the specification.
				# Another solution is to extract all the metadata information into a separate metadata database file. (This solution is currently used).
				meta_path = '%s/meta.json' % (sandbox_dir)
				abstract_metadata(spec_json, meta_path)
			elif behavior in ["expand", "filter"]:
				cleanup(tempfile_list, tempdir_list)
				logging.critical("The --meta option should be provided for the umbrella %s behavior!\n", behavior)
				sys.exit("The --meta option should be provided for the umbrella %s behavior!\n" % behavior)

		if meta_path:
			with open(meta_path) as f: #python 2.4 does not support this syntax: with open () as
				meta_json = json.load(f)

	if behavior in ["run", "validate", "split", "filter", "expand"]:
		#for validate, if only --spec is provided, then check whether this spec is self-contained.
		#for validate, if only --meta is provided, then check whether each item in the metadata db is well archived (for now, well-archived means the source attr is not null).
		#for validate, if both --spec and --meta are provided, then check whether the dependencies of the spec file is well archived.
		if spec_json == None:
			if meta_json == None:
				pass
			else:
				validate_meta(meta_json)
		else:
			if meta_json == None:
				validate_spec(spec_json)
			else:
				validate_spec(spec_json, meta_json)

	if behavior in ["run"]:
		if sandbox_mode == "ec2":
			ec2log_path = options.ec2_log
			ec2log_path = os.path.abspath(ec2log_path)
			if os.path.exists(ec2log_path):
				cleanup(tempfile_list, tempdir_list)
				sys.exit("The ec2_log option <%s> already exists!" % ec2log_path)

		if sandbox_mode == "condor":
			condorlog_path = options.condor_log
			condorlog_path = os.path.abspath(condorlog_path)
			if os.path.exists(condorlog_path):
				cleanup(tempfile_list, tempdir_list)
				sys.exit("The condor_log option <%s> already exists!" % condorlog_path)

	if behavior in ["run"]:
#		user_name = 'root' #username who can access the VM instances from Amazon EC2
#		ssh_key = 'hmeng_key_1018.pem' #the pem key file used to access the VM instances from Amazon EC2
		if sandbox_mode == "ec2":
			#ec2_path always refers to the local path of the ec2 resource json file.
			ec2_path = options.ec2
			ec2_path = os.path.abspath(ec2_path)
			logging.debug("Check the ec2 information file: %s", ec2_path)

			if not os.path.exists(ec2_path):
				path1 = ec2_path
				url = 'http://ccl.cse.nd.edu/software/umbrella/database/ec2.json'
				ec2_path = '%s/ec2.json' % (sandbox_dir)
				logging.debug("The provided ec2 info (%s) does not exist, download from %s into %s", path1, url, ec2_path)
				url_download(url, ec2_path)

			with open(ec2_path) as f: #python 2.4 does not support this syntax: with open () as
				ec2_json = json.load(f)

			ssh_key = os.path.abspath(options.ec2_sshkey)
			if not os.path.exists(ssh_key):
				cleanup(tempfile_list, tempdir_list)
				logging.critical("The ssh key file (%s) does not exists!", ssh_key)
				sys.exit("The ssh key file (%s) does not exists!\n" % ssh_key)

			ec2_security_group = options.ec2_group
			ec2_key_pair = options.ec2_key

			ec2_process(spec_path, spec_json, meta_path, meta_json, ec2_path, ec2_json, ssh_key, ec2_key_pair, ec2_security_group, sandbox_dir, output_dir, sandbox_mode, input_list, input_list_origin, env_para, user_cmd, cwd_setting, ec2log_path, cvmfs_http_proxy)
		elif sandbox_mode == "condor":
			condor_process(spec_path, spec_json, spec_path_basename, meta_path, sandbox_dir, output_dir, input_list_origin, user_cmd, cwd_setting, condorlog_path, cvmfs_http_proxy)
		elif sandbox_mode == "local":
			#first check whether Docker exists, if yes, use docker execution engine; if not, use parrot execution engine.
			if dependency_check('docker') == 0:
				logging.debug('docker exists, use docker execution engine')
				specification_process(spec_json, sandbox_dir, behavior, meta_json, 'docker', output_f_dict, output_d_dict, input_dict, env_para_dict, user_cmd, cwd_setting, cvmfs_http_proxy)
			else:
				logging.debug('docker does not exist, use parrot execution engine')
				specification_process(spec_json, sandbox_dir, behavior, meta_json, 'parrot', output_f_dict, output_d_dict, input_dict, env_para_dict, user_cmd, cwd_setting, cvmfs_http_proxy)
		else:
			if sandbox_mode == 'docker' and dependency_check('docker') != 0:
				cleanup(tempfile_list, tempdir_list)
				logging.critical('Docker is not installed on the host machine, please try other execution engines!')
				sys.exit('Docker is not installed on the host machine, please try other execution engines!')
			specification_process(spec_json, sandbox_dir, behavior, meta_json, sandbox_mode, output_f_dict, output_d_dict, input_dict, env_para_dict, user_cmd, cwd_setting, cvmfs_http_proxy)

	if behavior in ["expand", "filter"]:
		if len(args) != 2:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("The syntax for umbrella %s is: umbrella ... %s <filepath>.\n", behavior, behavior)
			sys.exit("The syntax for umbrella %s is: umbrella ... %s <filepath>.\n" % (behavior, behavior))
		target_specpath = os.path.abspath(args[1])
		dir_create(target_specpath)
		if behavior == "expand":
			new_json = separatize_spec(spec_json, meta_json, "spec")
		else:
			new_json = separatize_spec(spec_json, meta_json, "meta")
		#write new_json into the file specified by the user.
		json2file(target_specpath, new_json)

	if behavior in ["split"]:
		if len(args) != 3:
			cleanup(tempfile_list, tempdir_list)
			logging.critical("The syntax for umbrella split is: umbrella ... split <spec_filepath> <meta_filepath>.\n")
			sys.exit("The syntax for umbrella split is: umbrella ... split <spec_filepath> <meata_filepath>.\n")

		new_spec_path = os.path.abspath(args[1])
		db_path = os.path.abspath(args[2])
		dir_create(new_spec_path)
		dir_create(db_path)
		abstract_metadata(spec_json, db_path)
		new_json = prune_spec(spec_json)
		json2file(new_spec_path, new_json)

	cleanup(tempfile_list, tempdir_list)
	end = datetime.datetime.now()
	diff = end - start
	logging.debug("End time: %s", end)
	logging.debug("execution time: %d seconds", diff.seconds)

if __name__ == "__main__":
	main()

#set sts=4 sw=4 ts=4 expandtab ft=python
