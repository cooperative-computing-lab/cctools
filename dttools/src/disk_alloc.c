/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include "debug.h"

#ifdef CCTOOLS_OPSYS_LINUX

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <unistd.h>

#include "disk_alloc.h"
#include "path.h"
#include "stringtools.h"

int disk_alloc_create(char *loc, char *fs, int64_t size)
{

	if (size <= 0) {
		debug(D_NOTICE, "Mountpoint pathname argument nonexistant.\n");
		return 1;
	}

	// Check for trailing '/'
	path_remove_trailing_slashes(loc);
	int result;
	char *device_loc = NULL;
	char *dd_args = NULL;
	char *losetup_args = NULL;
	char *mk_args = NULL;
	char *mount_args = NULL;

	// Set Loopback Device Location
	device_loc = string_format("%s/alloc.img", loc);
	// Make Directory for Loop Device
	if (mkdir(loc, 0777) != 0) {
		debug(D_NOTICE, "Failed to make directory at requested mountpoint: %s.\n", strerror(errno));
		goto error;
	}

	// Create Image
	dd_args = string_format("dd if=/dev/zero of=%s bs=1024 count=%" PRId64 " > /dev/null 2> /dev/null", device_loc, size);
	if (system(dd_args) != 0) {
		debug(D_NOTICE, "Failed to allocate junk space for loop device image: %s.\n", strerror(errno));
		if (unlink(device_loc) == -1) {
			debug(D_NOTICE, "Failed to unlink loop device image while attempting to clean up after failure: %s.\n", strerror(errno));
			goto error;
		}
		if (rmdir(loc) == -1) {
			debug(D_NOTICE, "Failed to remove directory of loop device image while attempting to clean up after failure: %s.\n", strerror(errno));
		}
		goto error;
	}

	// Attach Image to Loop Device
	int j, losetup_flag = 0;
	for (j = 0;; j++) {

		if (j >= 256) {
			losetup_flag = 1;
			break;
		}

		// Binds the first available loop device to the specified mount point from input
		losetup_args = string_format("losetup /dev/loop%d %s > /dev/null 2> /dev/null", j, device_loc);
		// Makes the specified filesystem from input at the first available loop device
		mk_args = string_format("mkfs /dev/loop%d -t %s > /dev/null 2> /dev/null", j, fs);
		// Mounts the first available loop device
		mount_args = string_format("/dev/loop%d", j);

		if (system(losetup_args) == 0) {

			break;
		}
	}

	if (losetup_flag == 1) {
		debug(D_NOTICE, "Failed to attach image to loop device: %s.\n", strerror(errno));
		if (unlink(device_loc) == -1) {
			debug(D_NOTICE, "Failed to unlink loop device image while attempting to clean up after failure: %s.\n", strerror(errno));
			goto error;
		}
		if (rmdir(loc) == -1) {
			debug(D_NOTICE, "Failed to remove directory of loop device image while attempting to clean up after failure: %s.\n", strerror(errno));
		}
		goto error;
	}

	// Create Filesystem
	if (system(mk_args) != 0) {
		char *rm_dir_args;
		debug(D_NOTICE, "Failed to initialize filesystem on loop device: %s.\n", strerror(errno));
		rm_dir_args = string_format("losetup -d /dev/loop%d; rm -r %s", j, loc);
		if (system(rm_dir_args) == -1) {
			debug(D_NOTICE, "Failed to detach loop device and remove its contents while attempting to clean up after failure: %s.\n", strerror(errno));
		}
		free(rm_dir_args);
		goto error;
	}

	// Mount Loop Device
	result = mount(mount_args, loc, fs, 0, "");
	if (result != 0) {
		char *rm_dir_args;
		debug(D_NOTICE, "Failed to mount loop device: %s.\n", strerror(errno));
		rm_dir_args = string_format("losetup -d /dev/loop%d; rm -r %s", j, loc);
		if (system(rm_dir_args) == -1) {
			debug(D_NOTICE, "Failed to detach loop device and remove its contents while attempting to clean up after failure: %s.\n", strerror(errno));
		}
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
	if (device_loc) {
		free(device_loc);
	}
	if (dd_args) {
		free(dd_args);
	}
	if (losetup_args) {
		free(losetup_args);
	}
	if (mk_args) {
		free(mk_args);
	}
	if (mount_args) {
		free(mount_args);
	}

	return 1;
}

int disk_alloc_delete(char *loc)
{

	int result;
	char *losetup_args = NULL;
	char *rm_args = NULL;
	char *device_loc = NULL;
	char *losetup_del_args = NULL;

	// Check for trailing '/'
	path_remove_trailing_slashes(loc);
	// Check if location is relative or absolute
	result = strncmp(loc, "/", 1);
	if (result != 0) {
		char *pwd = get_current_dir_name();
		path_remove_trailing_slashes(pwd);
		device_loc = string_format("%s/%s/alloc.img", pwd, loc);
		free(pwd);
	} else {
		device_loc = string_format("%s/alloc.img", loc);
	}

	// Find Used Device
	char *dev_num = "-1";

	// Loop Device Unmounted
	result = umount2(loc, MNT_FORCE);
	if (result != 0) {
		if (errno != ENOENT) {
			debug(D_NOTICE, "Failed to unmount loop device: %s.\n", strerror(errno));
			goto error;
		}
	}

	// Find pathname of mountpoint associated with loop device
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

	// Copy only pathname of the mountpoint without extraneous paretheses
	for (k = 1; k < loop_dev_path_length; k++) {
		loop_mountpoint_array[k - 1] = loop_mount[k];
	}
	loop_mountpoint_array[k] = '\0';

	if (strncmp(loop_mountpoint_array, device_loc, max_mount_path_length) == 0) {

		dev_num = loop_dev;
	}

	// Device Not Found
	if (strcmp(dev_num, "-1") == 0) {
		debug(D_NOTICE, "Failed to locate loop device associated with given mountpoint: %s.\n", strerror(errno));
		goto error;
	}

	rm_args = string_format("%s/alloc.img", loc);
	losetup_del_args = string_format("losetup -d %s", dev_num);

	// Loop Device Deleted
	result = system(losetup_del_args);
	if (result != 0) {

		if (errno != ENOENT) {
			debug(D_NOTICE, "Failed to remove loop device associated with given mountpoint: %s.\n", strerror(errno));
			goto error;
		}
	}

	// Image Deleted
	result = unlink(rm_args);
	if (result != 0) {
		debug(D_NOTICE, "Failed to delete image file associated with given mountpoint: %s.\n", strerror(errno));
		goto error;
	}

	// Directory Deleted
	result = rmdir(loc);
	if (result != 0) {
		debug(D_NOTICE, "Failed to delete directory associated with given mountpoint: %s.\n", strerror(errno));
		goto error;
	}

	free(losetup_del_args);
	free(losetup_args);
	free(rm_args);
	free(device_loc);

	return 0;

error:
	if (losetup_del_args) {
		free(losetup_del_args);
	}
	if (losetup_args) {
		free(losetup_args);
	}
	if (rm_args) {
		free(rm_args);
	}
	if (device_loc) {
		free(device_loc);
	}

	return 1;
}

#else
int disk_alloc_create(char *loc, int64_t size)
{

	debug(D_NOTICE, "Platform not supported by this library.\n");
	return 1;
}

int disk_alloc_delete(char *loc)
{

	debug(D_NOTICE, "Platform not supported by this library.\n");
	return 1;
}
#endif
