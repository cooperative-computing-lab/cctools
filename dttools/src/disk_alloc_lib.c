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

#include "disk_alloc_lib.h"

int disk_alloc_create(char *loc, int64_t size) {

	int len = strlen(loc);

	//Check for trailing '/'
	if(loc[strlen(loc) - 1] == 47) {
		char *pos = loc + len - 1;
		*pos = '\0';
	}

	int result;
	char *device_loc = (char *) malloc(sizeof(char) * 200);
	char *dd_args = (char *) malloc(sizeof(char) * 200);
	char *losetup_args = (char *) malloc(sizeof(char) * 200);
	char *mk_args = (char *) malloc(sizeof(char) * 200);
	char *mount_args = (char *) malloc(sizeof(char) * 200);

	//Set Loopback Device Location
	sprintf(device_loc, "%s/alloc.img", loc);

	//Make Directory for Loop Device
	if(mkdir(loc, 7777) != 0) {
		printf("%s.\n", strerror(errno));
		goto error;
	}

	//Create Image
	sprintf(dd_args, "dd if=/dev/zero of=%s bs=1024 count=%"PRId64"", device_loc, size);
	if(system(dd_args) != 0) {
		unlink(device_loc);
		rmdir(loc);
		goto error;
	}

	//Attach Image to Loop Device
	int j, losetup_flag;
	for(j = 0; ; j++) {

		if(j >= 256) {
			losetup_flag = 1;
			break;
		}

		sprintf(losetup_args, "losetup /dev/loop%d %s", j, device_loc);
		sprintf(mk_args, "mkfs /dev/loop%d", j);
		sprintf(mount_args, "/dev/loop%d", j);

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
		char *rm_dir_args = (char *) malloc(sizeof(char) * 400);
		sprintf(rm_dir_args, "losetup -d /dev/loop%d; rm -r %s", j, loc);
		system(rm_dir_args);
		free(rm_dir_args);
		goto error;
	}

	//Mount Loop Device
	result = mount(mount_args, loc, "ext2", 0, "");
	if(result != 0) {
		char *rm_dir_args = (char *) malloc(sizeof(char) * 400);
		sprintf(rm_dir_args, "losetup -d /dev/loop%d; rm -r %s", j, loc);
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
		free(device_loc);
		free(dd_args);
		free(losetup_args);
		free(mk_args);
		free(mount_args);
		return -1;
}

int disk_alloc_delete(char *loc) {

	int result;

	int len = strlen(loc);

	//Check for trailing '/'
	if(loc[strlen(loc) - 1] == 47) {
		char *pos = loc + len - 1;
		*pos = '\0';
	}

	char *losetup_args = (char *) malloc(sizeof(char) * 200);
	char *rm_args = (char *) malloc(sizeof(char) * 200);
	
	//Find Used Device
	int i;
	int dev_num = -1;
	char *device_loc = (char *) malloc(sizeof(char) * 200);
	sprintf(device_loc, "(%s/alloc.img)", loc);

	for(i = 0; i < 256; i++) {

		char loop_dev[128], loop_info[128], loop_mount[128];
		FILE *loop_find;
		sprintf(losetup_args, "losetup /dev/loop%d", i);
		loop_find = popen(losetup_args, "r");
		fscanf(loop_find, "%s %s %s", loop_dev, loop_info, loop_mount);
		pclose(loop_find);

		if(strstr(loop_mount, device_loc) != NULL) {

			dev_num = i;
			break;
		}
	}

	free(device_loc);

	//Device Not Found
	if(dev_num == -1) {
		goto error;
	}

	sprintf(rm_args, "%s/alloc.img", loc);
	sprintf(losetup_args, "losetup -d /dev/loop%d", dev_num);

	//Loop Device Unmounted
	result = umount2(loc, MNT_FORCE);
	if(result != 0) {

		if(errno != ENOENT) {
			goto error;
		}
	}

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

	return 0;

	error:
		free(losetup_args);
		free(rm_args);
		return -1;
}
