/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>

#include "disk_alloc.h"

int disk_alloc_create(char *loc, char *space) {

	long long size;
	char metric;
	int result;

	//Check for trailing '/'
	if(loc[strlen(loc) - 1] == 47) {

		loc[strlen(loc) - 1] = 0;
	}

	//Scale disk allocation to correct size
	if(sscanf(space, "%lli%c", &size, &metric) == 2) {

		switch (toupper((int) metric)) {

			case 'P':
				size = size << 10;
			case 'T':
				size = size << 10;
			case 'G':
				size = size << 10;
			case 'M':
				size = size << 10;
			case 'K':
				break;
		}

	}

	else {

		printf("\nInvalid device size parameters passed.\n");
		return -1;
	}

	char *device_loc = (char *) malloc(sizeof(char) * 200);
	char *dd_args = (char *) malloc(sizeof(char) * 200);
	char *losetup_args = (char *) malloc(sizeof(char) * 200);
	char *mk_args = (char *) malloc(sizeof(char) * 200);
	char *mount_args = (char *) malloc(sizeof(char) * 200);

	//Set Loopback Device Location
	sprintf(device_loc, "%s/cct_img.img", loc);

	printf("\nInitializing disk allocation. This will take a moment.\n");

	//Make Directory for Loop Device
	if(mkdir(loc, 7777) != 0) {
		printf("\nFailed to make directory: %s.\n", strerror(errno));
		free(device_loc);
		free(dd_args);
		free(losetup_args);
		free(mk_args);
		free(mount_args);
		return -1;
	}

	//Create Image
	sprintf(dd_args, "dd if=/dev/zero of=%s bs=1024 count=%lli", device_loc, size);
	if(system(dd_args) != 0) {
		printf("\nFailed to create image file: %s.\n", strerror(errno));
		unlink(device_loc);
		rmdir(loc);
		free(device_loc);
		free(dd_args);
		free(losetup_args);
		free(mk_args);
		free(mount_args);
		return -1;
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
		printf("\nFailed to bind image file to loop device: %s.\n", strerror(errno));
		unlink(device_loc);
		rmdir(loc);
		free(device_loc);
		free(dd_args);
		free(losetup_args);
		free(mk_args);
		free(mount_args);
		return -1;
	}

	//Create Filesystem
	if(system(mk_args) != 0) {
		printf("\nFailed to make filesystem: %s.\n", strerror(errno));
		char *rm_dir_args = (char *) malloc(sizeof(char) * 400);
		sprintf(rm_dir_args, "losetup -d /dev/loop%d; rm -r %s", j, loc);
		system(rm_dir_args);
		free(rm_dir_args);
		free(device_loc);
		free(dd_args);
		free(losetup_args);
		free(mk_args);
		free(mount_args);
		return -1;
	}

	//Mount Loop Device
	result = mount(mount_args, loc, "ext2", 0, "");
	if(result != 0) {
		printf("\nFailed to mount/dev/loop%d: %s.\n", j, strerror(errno));
		char *rm_dir_args = (char *) malloc(sizeof(char) * 400);
		sprintf(rm_dir_args, "losetup -d /dev/loop%d; rm -r %s", j, loc);
		system(rm_dir_args);
		free(rm_dir_args);
		free(device_loc);
		free(dd_args);
		free(losetup_args);
		free(mk_args);
		free(mount_args);
		return -1;
	}

	free(device_loc);
	free(dd_args);
	free(losetup_args);
	free(mk_args);
	free(mount_args);

	printf("\nDisk space %s allocated on device: /dev/loop%d.\n", space, j);

	return j;
}

int disk_alloc_delete(char *loc, int dev_num) {

	int result;

	//Check for trailing '/'
	if(loc[strlen(loc) - 1] == 47) {

		loc[strlen(loc) - 1] = 0;
	}

	char *losetup_args = (char *) malloc(sizeof(char) * 200);
	char *rm_args = (char *) malloc(sizeof(char) * 200);

	sprintf(rm_args, "%s/cct_img.img", loc);
	sprintf(losetup_args, "losetup -d /dev/loop%d", dev_num);

	printf("\nPreparing to clean disk allocation.\n");

	//Loop Device Unmounted
	result = umount2(loc, MNT_FORCE);
	if(result != 0) {

		printf("\nFailed to unmount /dev/loop%d: %s.\nStopping disk deallocation.\n", dev_num, strerror(errno));
		free(losetup_args);
		free(rm_args);
		return -1;
	}

	//Loop Device Deleted
	result = system(losetup_args);
	if(result != 0) {

		printf("\nFailed to detach /dev/loop%d: %s. Stopping disk deallocation.\n", dev_num, strerror(errno));
		free(losetup_args);
		free(rm_args);
		return -1;
	}

	//Image Deleted
	result = unlink(rm_args);

	if(result != 0) {

		printf("\nFailed to remove directory: %s. Stopping disk deallocation.\n", strerror(errno));
		free(losetup_args);
		free(rm_args);
		return -1;
	}

	//Directory Deleted
	result = rmdir(loc);

	if(result != 0) {

		printf("\nFailed to remove directory: %s. Stopping disk deallocation.\n", strerror(errno));
		free(losetup_args);
		free(rm_args);
		return -1;
	}

	free(losetup_args);
	free(rm_args);

	printf("Disk allocation cleaned and removed.\n");

	return 0;
}
