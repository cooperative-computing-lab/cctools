/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdint.h>
#ifndef DISK_ALLOC_H
#define DISK_ALLOC_H

/* The disk_alloc_create function instantiates
 * a virtual device and creates a file system
 * generated at the user defined location with
 * a maximum capacity given by the user defined
 * size.
 * @param loc: Location for disk allocation.
 * @param size: The size to set the file
 * system's capacity (in KB).
 * @return: Return 0 on success, else -1.
 */
int disk_alloc_create(char *loc, char *fs, int64_t size);

/* The disk_alloc_delete functions deletes
 * a file system at the user defined location.
 * @param loc: Location of the disk allocation
 * to be deleted.
 * @return: Return 0 on success, else -1.
 */
int disk_alloc_delete(char *loc);
#endif
