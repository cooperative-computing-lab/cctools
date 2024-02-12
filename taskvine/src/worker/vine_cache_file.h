/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_CACHE_FILE_H
#define VINE_CACHE_FILE_H

#include "vine_cache.h"
#include "vine_task.h"
#include "vine_process.h"

#include <unistd.h>

#include "timestamp.h"

struct vine_cache_file {
	/* Static properties of cache file object. */
	vine_cache_type_t cache_type;
	char *source;
	struct vine_task *mini_task;

	/* Dynamic state tracking process to materialize the file. */
	struct vine_process *process;
	timestamp_t start_time;
	timestamp_t stop_time;
	pid_t pid;
	vine_cache_status_t status;

	/* Metadata info stored in disk in .meta file. */
	vine_file_type_t original_type; // original type of the object: file, url, temp, etc..
	vine_cache_level_t cache_level; // how long to cache the object.
	int mode;                       // unix mode bits of original object
	uint64_t size;                  // summed size of the file or dir tree in bytes
	time_t mtime;                   // source mtime of original object
	timestamp_t transfer_time;      // time to transfer (or create) the object
};

struct vine_cache_file *vine_cache_file_create( vine_cache_type_t type, const char *source, struct vine_task *mini_task);
void vine_cache_file_delete( struct vine_cache_file *f);

int vine_cache_file_load_metadata( struct vine_cache_file *f, const char *filename );
int vine_cache_file_save_metadata( struct vine_cache_file *f, const char *filename );

#endif
