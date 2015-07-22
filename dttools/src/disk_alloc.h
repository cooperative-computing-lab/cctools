/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DISK_ALLOC_H
#define DISK_ALLOC_H

/* The disk_alloc_create function creates
 * a file system generated at the user
 * defined location with a maximum capacity
 * given by the user defined size.
 * @param loc: A location for the file system.
 * @param size: The size (default in MB) to
 * 		set the file system's capacity.
 * @return: Return device number of successful
 * mount of loop device on success (i.e., return
 * 7 in the case of mounting on loop7), else -1. 
 */
int disk_alloc_create(char *loc, char *space);

/* The disk_alloc_delete functions deletes
 * a file system at the user defined location.
 * @param loc: Location of the file system to
 * be deleted.
 * @param dev_num: The device number which
 * corresponds to the mounted loop device
 * (i.e., loop7).
 * @return: Return 0 on success, else -1.
 */
int disk_alloc_delete(char *loc, int dev_num);

#endif
