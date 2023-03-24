/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MOUNT_H
#define VINE_MOUNT_H

/** @file vine_mount.h
A vine_mount describes the binding between a task and a file,
indicating where a file should be mounted in the name space,
and any special handling for that file.  Note that multiple
vine_tasks may mount the same vine_file differently, but the underlying
vine_file should not change.
*/

#include "vine_file.h"

struct vine_mount {
	struct vine_file *file;       // The file object to be mounted.
	char *remote_name;	      // Name of file as it appears to the task.
	vine_mount_flags_t flags;      // Special handling: VINE_CACHE for caching, VINE_WATCH for watching, etc.
	struct vine_file *substitute; // For transfer purposes, fetch from this substitute source instead.
};

struct vine_mount * vine_mount_create( struct vine_file *f, const char *remote_name, vine_mount_flags_t flags, struct vine_file *substitute );
struct vine_mount * vine_mount_clone( struct vine_mount *m );
void vine_mount_delete( struct vine_mount *m );

#endif


