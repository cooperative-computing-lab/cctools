/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#include "vine_cache_file.h"


struct vine_cache_file *vine_cache_file_create(
		vine_cache_type_t type, const char *source, int64_t actual_size, int mode, struct vine_task *mini_task)
{
	struct vine_cache_file *f = malloc(sizeof(*f));
	f->type = type;
	f->source = xxstrdup(source);
	f->actual_size = actual_size;
	f->mode = mode;
	f->pid = 0;
	f->status = VINE_CACHE_STATUS_NOT_PRESENT;
	f->mini_task = mini_task;
	f->process = 0;
	f->start_time = 0;
	f->stop_time = 0;
	return f;
}

void vine_cache_file_delete(struct vine_cache_file *f)
{
	if (f->mini_task) {
		vine_task_delete(f->mini_task);
	}
	if (f->process) {
		vine_process_delete(f->process);
	}
	if (f->meta) {
		vine_cache_meta_delete(f->meta);
	}
	free(f->source);
	free(f);
}
