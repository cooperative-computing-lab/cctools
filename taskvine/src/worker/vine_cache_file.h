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
	vine_cache_type_t type;
	timestamp_t start_time;
	timestamp_t stop_time;
	pid_t pid;
	char *source;
	int64_t actual_size;
	int mode;
	vine_cache_status_t status;
	struct vine_task *mini_task;
	struct vine_process *process;
};

struct vine_cache_file *vine_cache_file_create( vine_cache_type_t type, const char *source, int64_t actual_size, int mode, struct vine_task *mini_task);
void vine_cache_file_delete( struct vine_cache_file *f);

#endif
