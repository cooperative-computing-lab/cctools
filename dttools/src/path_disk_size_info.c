/*
Copyright (C) 2022 The University of Notre Dame
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

struct DIR_with_name {
	DIR *dir;
	char *name;
};


int path_disk_size_info_get(const char *path, int64_t *measured_size, int64_t *number_of_files) {

	struct stat info;
	int result = stat(path,&info);
	if(result==0) {
		if(S_ISDIR(info.st_mode)) {
			struct path_disk_size_info *state = NULL;
			result = path_disk_size_info_get_r(path, -1, &state);

			*measured_size   = state->last_byte_size_complete;
			*number_of_files = state->last_file_count_complete;

			path_disk_size_info_delete_state(state);
		} else {
			*measured_size   = info.st_size;
			*number_of_files = 1;
		}
	}
	
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

		struct DIR_with_name *here = calloc(1, sizeof(struct DIR_with_name));

		if((here->dir = opendir(path))) {
			here->name = xxstrdup(path);
			s->current_dirs = list_create();
			s->size_so_far  = 0;
			s->count_so_far = 1;                     /* count the root directory */
			list_push_tail(s->current_dirs, here);
		} else {
			debug(D_DEBUG, "error reading disk usage on directory: %s.\n", path);
			s->size_so_far  = -1;
			s->count_so_far = -1;
			s->complete_measurement = 1;
			result       = -1;

			free(here);
			goto timeout;
		}
	}

	struct DIR_with_name *tail;
	while((tail = list_peek_tail(s->current_dirs))) {
		struct dirent *entry;
		struct stat   file_info;
		
		if (!tail->dir) {	// only open dir when it's being processed
			tail->dir = opendir(tail->name);
			if (!tail->dir) {
				if (errno == ENOENT) {
					/* Do nothing as a directory might go away. */
					tail = list_pop_tail(s->current_dirs);
					free(tail->name);
					free(tail);
					continue;
				}
				else {
					debug(D_DEBUG, "error opening directory '%s', errno: %s.\n", tail->name, strerror(errno));
					result = -1;
					goto timeout;
				}
			}
		}
		/* Read out entries from the dir stream. */
		while((entry = readdir(tail->dir))) {
			if( strcmp(".", entry->d_name) == 0 || strcmp("..", entry->d_name) == 0)
				continue;

			char composed_path[PATH_MAX];
			if(entry->d_name[0] == '/') {
				strncpy(composed_path, entry->d_name, PATH_MAX);
			} else {
				snprintf(composed_path, PATH_MAX, "%s/%s", tail->name, entry->d_name);
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
				/* Only add name of directory, will only open it to read when it's its turn. */
				struct DIR_with_name *branch = calloc(1, sizeof(struct DIR_with_name));
				branch->name = xxstrdup(composed_path);
				list_push_head(s->current_dirs, branch);
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
		if (tail->dir) {
			closedir(tail->dir);
		}
		free(tail->name);
		free(tail);
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
	}
	else {
		/* else, we hit a timeout. measurement reported is conservative, from
		 * what we knew, and know so far. */

		s->last_byte_size_complete  = MAX(s->last_byte_size_complete, s->size_so_far);
		s->last_file_count_complete = MAX(s->last_file_count_complete, s->count_so_far);
	}

	return result;
}

void path_disk_size_info_delete_state(struct path_disk_size_info *state) {
	if(!state)
		return;

	if(state->current_dirs) {
		struct DIR_with_name *tail;
		while((tail = list_pop_tail(state->current_dirs))) {
			if(tail->dir)
				closedir(tail->dir);

			if(tail->name)
				free(tail->name);

			free(tail);
		}
		list_delete(state->current_dirs);
	}

	free(state);
}

/* vim: set noexpandtab tabstop=8: */
