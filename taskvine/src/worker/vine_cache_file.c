/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_cache_file.h"
#include "vine_protocol.h"

#include "debug.h"
#include "path_disk_size_info.h"
#include "xxmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

struct vine_cache_file *vine_cache_file_create(vine_cache_type_t cache_type, const char *source, struct vine_task *mini_task)
{
	struct vine_cache_file *f = malloc(sizeof(*f));
	memset(f, 0, sizeof(*f));

	f->cache_type = cache_type;
	f->source = xxstrdup(source);
	f->mini_task = mini_task;

	f->status = VINE_CACHE_STATUS_PENDING;

	/* Remaining items default to zero. */
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
	free(f->source);
	free(f);
}

int vine_cache_file_load_metadata(struct vine_cache_file *f, const char *filename)
{
	char line[VINE_LINE_MAX];
	char source[VINE_LINE_MAX];

	FILE *file = fopen(filename, "r");
	if (!file)
		return 0;

	/* Use of sscanf is simplified by matching %lld with long long */
	long long value;

	while (fgets(line, sizeof(line), file)) {
		if (sscanf(line, "type %lld", &value)) {
			f->original_type = value;
		} else if (sscanf(line, "cache_level %lld", &value)) {
			f->cache_level = value;
		} else if (sscanf(line, "mode %llo", &value)) {
			f->mode = value;
		} else if (sscanf(line, "size %lld", &value)) {
			f->size = value;
		} else if (sscanf(line, "mtime %lld", &value)) {
			f->mtime = value;
		} else if (sscanf(line, "transfer_time %lld", &value)) {
			f->transfer_time = value;
		} else if (sscanf(line, "transfer_start %lld", &value)) {
			f->start_time = value;
		} else if (sscanf(line, "source %[^\n]\n", source)) {
			if (f->source)
				free(f->source);
			f->source = strdup(source);
		} else {
			debug(D_VINE, "error in %s: %s\n", filename, line);
			fclose(file);
			return 0;
		}
	}

	fclose(file);
	return 1;
}

int vine_cache_file_save_metadata(struct vine_cache_file *f, const char *filename)
{
	FILE *file = fopen(filename, "w");
	if (!file)
		return 0;

	fprintf(file, "type %d\n", f->original_type);
	fprintf(file, "cache_level %d\n", f->cache_level);
	fprintf(file, "mode 0%o\n", f->mode);
	fprintf(file, "size %lld\n", (long long)f->size);
	fprintf(file, "mtime %lld\n", (long long)f->mtime);
	fprintf(file, "transfer_time %lld\n", (long long)f->transfer_time);
	fprintf(file, "transfer_start %lld\n", (long long)f->start_time);
	if (f->source) {
		fprintf(file, "source %s\n", f->source);
	}

	fclose(file);

	return 1;
}

/* Observe the mode, size, and mtime of a file or directory tree. */

int vine_cache_file_measure_metadata(const char *path, int *mode, int64_t *size, time_t *mtime)
{
	struct stat info;
	int64_t nfiles = 0;

	/* Get the basic metadata. */
	int result = stat(path, &info);
	if (result < 0)
		return 0;

	/* Measure the size of the item recursively, if a directory. */
	result = path_disk_size_info_get(path, size, &nfiles);
	if (result < 0)
		return 0;

	*mode = info.st_mode;
	*mtime = info.st_mtime;

	return 1;
}
