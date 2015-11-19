/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

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
		return -1;
	}

	//Check for trailing '/'
	path_remove_trailing_slashes(loc);

	int result;
	char *device_loc, *dd_args, *losetup_args, *mk_args, *mount_args;
	dd_args = string_format("junk");
	losetup_args = string_format("junk");
	mk_args = string_format("junk");
	mount_args = string_format("junk");

	//Set Loopback Device Location
	device_loc = string_format("%s/alloc.img", loc);

	//Make Directory for Loop Device
	if(mkdir(loc, 0777) != 0) {

		goto error;
	}

	//Create Image
	dd_args = string_format("dd if=/dev/zero of=%s bs=1024 count=%"PRId64"", device_loc, size);
	if(system(dd_args) != 0) {
		unlink(device_loc);
		rmdir(loc);
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
		goto error;
	}

	//Create Filesystem
	if(system(mk_args) != 0) {
		char *rm_dir_args;
		rm_dir_args = string_format("losetup -d /dev/loop%d; rm -r %s", j, loc);
		system(rm_dir_args);
		free(rm_dir_args);
		goto error;
	}

	//Mount Loop Device
	result = mount(mount_args, loc, "ext4", 0, "");
	if(result != 0) {
		char *rm_dir_args;
		rm_dir_args = string_format("losetup -d /dev/loop%d; rm -r %s", j, loc);
		system(rm_dir_args);
		free(rm_dir_args);
		goto error;
	}

	free(device_loc);
	free(dd_args);
	free(losetup_args);
	free(mk_args);
	free(mount_args);

	return 0;

	error:
		printf("%d\n", errno);
		free(device_loc);
		free(dd_args);
		free(losetup_args);
		free(mk_args);
		free(mount_args);
		return -1;
}

int disk_alloc_delete(char *loc) {

	int result;

	//Check for trailing '/'
	path_remove_trailing_slashes(loc);

	char *losetup_args, *rm_args, *device_loc;
	losetup_args = string_format("junk");
	rm_args = string_format("junk");
	device_loc = string_format("junk");

	//Find Used Device
	int i;
	char *dev_num = "-1";
	device_loc = string_format("%s/alloc.img", loc);
	
	//Loop Device Unmounted
	result = umount2(loc, MNT_FORCE);
	if(result != 0) {

		if(errno != ENOENT) {
			goto error;
		}
	}

	for(i = 0; i < 256; i++) {

		char loop_dev[128], loop_info[128], loop_mount[128];
		FILE *loop_find;

		losetup_args = string_format("losetup -j %s", device_loc);
		loop_find = popen(losetup_args, "r");
		fscanf(loop_find, "%s %s %s", loop_dev, loop_info, loop_mount);
		pclose(loop_find);
		loop_mount[0] = '\0';
		loop_mount[strlen(loop_mount) - 1] = '\0';
		loop_dev[strlen(loop_dev) - 1] = '\0';
		if(strncmp(loop_mount, device_loc, 62) + 47 == 0) {

			dev_num = loop_dev;
			break;
		}
	}

	//Device Not Found
	if(strcmp(dev_num, "-1") == 0) {
		goto error;
	}

	rm_args = string_format("%s/alloc.img", loc);
	losetup_args = string_format("losetup -d %s", dev_num);

	//Loop Device Deleted
	result = system(losetup_args);
	if(result != 0) {

		if(errno != ENOENT) {
			goto error;
		}
	}

	//Image Deleted
	result = unlink(rm_args);
	if(result != 0) {

		goto error;
	}

	//Directory Deleted
	result = rmdir(loc);
	if(result != 0) {

		goto error;
	}

	free(losetup_args);
	free(rm_args);
	free(device_loc);

	return 0;

	error:

		free(losetup_args);
		free(rm_args);
		free(device_loc);

		return -1;
}
