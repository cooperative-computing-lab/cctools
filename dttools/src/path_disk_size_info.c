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

	if(!*state) {
		/* if state is null, there is no state, and path is the root of the measurement. */
		*state = calloc(1, sizeof(struct path_disk_size_info));
	}
	struct path_disk_size_info *s = *state;     /* shortcut for *state, so we do not need to type (*state)->... */

	int64_t start_time = time(0);
	int result = 0;
	
	/* if no current_dirs, we begin a new measurement. */
	if(!s->current_dirs) {
		s->complete_measurement = 0;
		s->current_dirs 		= list_create();
		s->size_so_far 			= 0;
		s->count_so_far 		= 0;
		list_push_tail(s->current_dirs, (void*) xxstrdup(path));
	}

	/* Start processing unseen directories.
	 * Each loop counts the current directory and all files immediately except 
	 * files that are directories. Directory entries are added to a list to be 
	 * counted later.
	 */
	char* tail_name;
	struct dirent *entry;
	struct stat file_info;
	DIR* tail_dir;
	int timeout = 0;
	while((tail_name = list_pop_tail(s->current_dirs))) {
		tail_dir = opendir(tail_name);
		if (!tail_dir) {
			if (errno == ENOENT) {
				/* Directory might have been removed so we ignore this. */
			}
			else {
				debug(D_DEBUG, "error opening directory '%s': (errno) %s\n", tail_name, strerror(errno));
				result = -1;
			}
			free(tail_name);
			continue;
		}

		/* Add current dir to stats */
		if(lstat(tail_name, &file_info) < 0) {
			if(errno == ENOENT) {
				/* Directory might have been removed so we ignore this. */
			} else {
				debug(D_DEBUG, "error reading disk usage on '%s'. errno: %s\n", path, strerror(errno));
				result = -1;
			}
			closedir(tail_dir);
			free(tail_name);
			continue;
		}
		s->size_so_far += file_info.st_size;
		s->count_so_far += 1;

		/* Read entries of opened directory */
		while((entry = readdir(tail_dir))) {
			if( strcmp(".", entry->d_name) == 0 || strcmp("..", entry->d_name) == 0)
				continue;

			char composed_path[PATH_MAX];
			if(entry->d_name[0] == '/') {	// absolute path
				strncpy(composed_path, entry->d_name, PATH_MAX);
			} else {						// relative path
				snprintf(composed_path, PATH_MAX, "%s/%s", tail_name, entry->d_name);
			}

			if(lstat(composed_path, &file_info) < 0) {
				if(errno == ENOENT) {
					/* our DIR structure is stale, and a file went away. We simply do nothing. */
				} else {
					debug(D_DEBUG, "error reading disk usage on '%s', errno %s.\n", path, strerror(errno));
					result = -1;
				}
				continue;
			}

			if(S_ISREG(file_info.st_mode)) {
				s->size_so_far += file_info.st_size;
				s->count_so_far++;
			} else if(S_ISDIR(file_info.st_mode)) {
				/* save to process later */
				list_push_head(s->current_dirs, xxstrdup(composed_path));
			} else if(S_ISLNK(file_info.st_mode)) {
				/* do nothing, avoiding infinite loops. */
			}

			if(max_secs > -1 && time(0) - start_time >= max_secs) {
				timeout = 1;
				break;
			}
		}
		
		/* we are done reading a complete directory, and we go to the next in the queue */
		closedir(tail_dir);
		free(tail_name);
		if (timeout) {
			break;
		}
	}

	list_delete(s->current_dirs);
	s->current_dirs = NULL;       /* signal that a new measurement is needed, if state structure is reused. */
	s->complete_measurement = 1;

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
		char* tail_name;
		while((tail_name = list_pop_tail(state->current_dirs))) {
			if(tail_name)
				free(tail_name);
		}
		list_delete(state->current_dirs);
	}

	free(state);
}

/* vim: set noexpandtab tabstop=4: */
