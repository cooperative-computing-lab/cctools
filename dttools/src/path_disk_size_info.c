/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "path_disk_size_info.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <sys/stat.h>

#include "debug.h"
#include "list.h"
#include "macros.h"
#include "stringtools.h"
#include "xxmalloc.h"

int path_disk_size_info_get(const char *path, int64_t *measured_size, int64_t *number_of_files) {
	struct path_disk_size_info *state = NULL;
	int result = path_disk_size_info_get_r(path, -1, &state);

	*measured_size   = state->last_byte_size_complete;
	*number_of_files = state->last_file_count_complete;

	path_disk_size_info_delete_state(state);

	return result;
}

int path_disk_size_info_get_r(const char *path, int64_t max_secs, struct path_disk_size_info **state) {
	int64_t start_time = time(0);
	int result = 0;

	if(!*state) {
		/* if state is null, there is no state, and path is the root of the measurement. */
		*state = calloc(1, sizeof(struct path_disk_size_info));
	}

	struct path_disk_size_info *s = *state;     /* shortcut for *state, so we do not need to type (*state)->... */

	/* if no current_dirs, we begin a new measurement. */
	if(!s->current_dirs) {
		s->complete_measurement = 0;

		DIR *here;
		if((here = opendir(path))) {
			s->current_dirs = list_create(0);
			s->size_so_far  = 0;
			s->count_so_far = 1;                     /* count the root directory */
			list_push_tail(s->current_dirs, here);
		} else {
			debug(D_DEBUG, "error reading disk usage on directory: %s.\n", path);
			s->size_so_far  = -1;
			s->count_so_far = -1;
			s->complete_measurement = 1;
			result       = -1;
			goto timeout;
		}
	}

	DIR *tail;
	while((tail = list_peek_tail(s->current_dirs))) {
		struct dirent *entry;
		struct stat   file_info;
		while((entry = readdir(tail))) {
			if( strcmp(".", entry->d_name) == 0 || strcmp("..", entry->d_name) == 0)
				continue;

			char *composed_path;
			if(entry->d_name[0] == '/') {
				composed_path = xxstrdup(entry->d_name);
			} else {
				composed_path = string_format("%s/%s", path, entry->d_name);
			}

			if(lstat(composed_path, &file_info) < 0) {
				if(errno == ENOENT) {
					/* our DIR structure is stale, and a file went away. We simply do nothing. */
				} else {
					debug(D_DEBUG, "error reading disk usage on '%s'.\n", path);
					result = -1;
				}
				continue;
			}

			s->count_so_far++;
			if(S_ISREG(file_info.st_mode)) {
				s->size_so_far += file_info.st_size;
			} else if(S_ISDIR(file_info.st_mode)) {
				DIR *branch;
				if((branch = opendir(composed_path))) {
					/* next while we read from the branch */
					list_push_tail(s->current_dirs, branch);
				} else {
					result = -1;
					continue;
				}
			} else if(S_ISLNK(file_info.st_mode)) {
				/* do nothing, avoiding infinite loops. */
			}

			if(max_secs > -1) {
				if( time(0) - start_time >= max_secs ) {
					goto timeout;
				}
			}
		}

		/* we are done reading a complete directory, and we go to the next in the queue */
		tail = list_pop_tail(s->current_dirs);
		closedir(tail);
	}

	list_delete(s->current_dirs);
	s->current_dirs = NULL;       /* signal that a new measurement is needed, if state structure is reused. */
	s->complete_measurement = 1;

timeout:
	if(s->complete_measurement) {
		/* if a complete measurement has been done, then update
		 * for the found value */
		s->last_byte_size_complete  = s->size_so_far;
		s->last_file_count_complete = s->count_so_far;

		debug(D_DEBUG, "completed measurement on '%s', %" PRId64 " files using %3.2lf MB\n",
				path, s->count_so_far, (double) s->size_so_far/(1024.0*1024));
	}
	else {
		/* else, we hit a timeout. measurement reported is conservative, from
		 * what we knew, and know so far. */

		s->last_byte_size_complete  = MAX(s->last_byte_size_complete, s->size_so_far);
		s->last_file_count_complete = MAX(s->last_file_count_complete, s->count_so_far);

		debug(D_DEBUG, "partial measurement on '%s', %" PRId64 " files using %3.3lf MB\n",
				path, s->count_so_far, (double) s->size_so_far/(1024.0*1024));
	}

	return result;
}

void path_disk_size_info_delete_state(struct path_disk_size_info *state) {
	if(!state)
		return;

	if(state->current_dirs) {
		DIR *dir;
		while((dir = list_pop_head(state->current_dirs))) {
			closedir(dir);
		}
		list_delete(state->current_dirs);
	}

	free(state);
}

/* vim: set noexpandtab tabstop=4: */
