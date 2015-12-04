/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include "debug.h"

#ifdef CCTOOLS_OPSYS_LINUX

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>

#include "disk_alloc.h"
#include "stringtools.h"
#include "path.h"

int disk_alloc_create(char *loc, int64_t size) {

	if(size <= 0) {
		debug(D_NOTICE, "Mountpoint pathname argument nonexistant.\n");
		return 1;
	}

	//Check for trailing '/'
	path_remove_trailing_slashes(loc);
	int result;
	char *device_loc = NULL;
	char *dd_args = NULL;
	char *losetup_args = NULL;
	char *mk_args = NULL;
	char *mount_args = NULL;

	//Set Loopback Device Location
	device_loc = string_format("%s/alloc.img", loc);
	//Make Directory for Loop Device
	if(mkdir(loc, 0777) != 0) {
		debug(D_NOTICE, "Failed to make directory at requested mountpoint.\n");
		goto error;
	}

	//Create Image
	dd_args = string_format("dd if=/dev/zero of=%s bs=1024 count=%"PRId64"", device_loc, size);
	if(system(dd_args) != 0) {
		unlink(device_loc);
		rmdir(loc);
		debug(D_NOTICE, "Failed to allocate junk space for loop device image.\n");
		goto error;
	}

	//Attach Image to Loop Device
	int j, losetup_flag = 0;
	for(j = 0; ; j++) {

		if(j >= 256) {
			losetup_flag = 1;
			break;
		}

		losetup_args = string_format("losetup /dev/loop%d %s", j, device_loc);
		mk_args = string_format("mkfs /dev/loop%d", j);
		mount_args = string_format("/dev/loop%d", j);

		if(system(losetup_args) == 0) {

			break;
		}
	}

	if(losetup_flag == 1) {
		unlink(device_loc);
		rmdir(loc);
		debug(D_NOTICE, "Failed to attach image to loop device.\n");
		goto error;
	}

	//Create Filesystem
	if(system(mk_args) != 0) {
		char *rm_dir_args;
		rm_dir_args = string_format("losetup -d /dev/loop%d; rm -r %s", j, loc);
		system(rm_dir_args);
		free(rm_dir_args);
		debug(D_NOTICE, "Failed to initialize filesystem on loop device.\n");
		goto error;
	}

	//Mount Loop Device
	result = mount(mount_args, loc, "ext4", 0, "");
	if(result != 0) {
		char *rm_dir_args;
		rm_dir_args = string_format("losetup -d /dev/loop%d; rm -r %s", j, loc);
		system(rm_dir_args);
		free(rm_dir_args);
		debug(D_NOTICE, "Failed to mount loop device.\n");
		goto error;
	}

	free(device_loc);
	free(dd_args);
	free(losetup_args);
	free(mk_args);
	free(mount_args);
	return 0;

	error:
		if(device_loc) {
			free(device_loc);
		}
		if(dd_args) {
			free(dd_args);
		}
		if(losetup_args) {
			free(losetup_args);
		}
		if(mk_args) {
			free(mk_args);
		}
		if(mount_args) {
			free(mount_args);
		}

		return 1;
}

int disk_alloc_delete(char *loc) {

	int result;

	//Check for trailing '/'
	path_remove_trailing_slashes(loc);
	char *pwd = get_current_dir_name();
	char *losetup_args = NULL;
	char *rm_args = NULL;
	char *device_loc = NULL;
	char *losetup_del_args = NULL;

	//Find Used Device
	char *dev_num = "-1";
	device_loc = string_format("%s/%s/alloc.img", pwd, loc);
	free(pwd);

	//Loop Device Unmounted
	result = umount2(loc, MNT_FORCE);
	if(result != 0) {
		if(errno != ENOENT) {
			debug(D_NOTICE, "Failed to unmount loop device.\n");
			goto error;
		}
	}

	//Find pathname of mountpoint associated with loop devide
	char loop_dev[128], loop_info[128], loop_mount[128];
	FILE *loop_find;
	losetup_args = string_format("losetup -j %s", device_loc);
	loop_find = popen(losetup_args, "r");
	fscanf(loop_find, "%s %s %s", loop_dev, loop_info, loop_mount);
	pclose(loop_find);
	int loop_dev_path_length = strlen(loop_mount);
	loop_mount[loop_dev_path_length - 1] = '\0';
	loop_dev[strlen(loop_dev) - 1] = '\0';
	char loop_mountpoint_array[128];
	int k;
	int max_mount_path_length = 62;

	//Copy only pathname of the mountpoint without extraneous paretheses
	for(k = 1; k < loop_dev_path_length; k++) {
		loop_mountpoint_array[k-1] = loop_mount[k];
	}
	loop_mountpoint_array[k] = '\0';

	if(strncmp(loop_mountpoint_array, device_loc, max_mount_path_length) == 0) {

		dev_num = loop_dev;
	}

	//Device Not Found
	if(strcmp(dev_num, "-1") == 0) {
		debug(D_NOTICE, "Failed to locate loop device associated with given mountpoint.\n");
		goto error;
	}

	rm_args = string_format("%s/alloc.img", loc);
	losetup_del_args = string_format("losetup -d %s", dev_num);

	//Loop Device Deleted
	result = system(losetup_args);
	if(result != 0) {

		if(errno != ENOENT) {
			debug(D_NOTICE, "Failed to remove loop device associated with given mountpoint.\n");
			goto error;
		}
	}

	//Image Deleted
	result = unlink(rm_args);
	if(result != 0) {
		debug(D_NOTICE, "Failed to delete image file associated with given mountpoint.\n");
		goto error;
	}

	//Directory Deleted
	result = rmdir(loc);
	if(result != 0) {
		debug(D_NOTICE, "Failed to delete directory associated with given mountpoint.\n");
		goto error;
	}

	free(losetup_del_args);
	free(losetup_args);
	free(rm_args);
	free(device_loc);

	return 0;

	error:
		if(losetup_del_args) {
			free(losetup_del_args);
		}
		if(losetup_args) {
			free(losetup_args);
		}
		if(rm_args) {
			free(rm_args);
		}
		if(device_loc) {
			free(device_loc);
		}

		return 1;
}

#else
int disk_alloc_create(char *loc, int64_t size) {

	debug(D_NOTICE, "Platform not supported by this library.\n");
	return 1;
}

int disk_alloc_delete(char *loc) {

	debug(D_NOTICE, "Platform not supported by this library.\n");
	return 1;
}
#endif
