/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CWD_INFO_H
#define CWD_INFO_H

#include "int_sizes.h"
#include "list.h"

struct path_disk_size_info {
	int     complete_measurement;
	int64_t last_byte_size_complete;
	int64_t last_file_count_complete;

	int64_t size_so_far;
	int64_t count_so_far;

	struct list *current_dirs;
};

/** @file path_disk_size_info.h
Query disk space on the given directory.
*/

/** Get the total disk usage on path.
@param path Directory to be measured.
@param *measured_size A pointer to an integer that will be filled with the total space in bytes.
@param *number_of_files A pointer to an integer that will be filled with the total number of files, directories, and symbolic links.
@return zero on success, -1 if an error is encounterd (see errno).
*/
int path_disk_size_info_get(const char *path, int64_t *measured_size, int64_t *number_of_files);

/** Get a (perhaps partial) disk usage on path, but measure by max_secs at a time.
If *state is NULL, start a new measurement, otherwise continue from
the state recorded in state (see @ref path_disk_size_info).
When the function returns, if *state->complete_measurement is 1, then the measurement was completed before a timeout.
@param path Directory to be measured.
@param max_secs Maximum number of seconds to spend in the measurement.
@param *state State of the measurement.
@return zero on success, -1 if an error is encounterd (see errno).
*/
int path_disk_size_info_get_r(const char *path, int64_t max_secs, struct path_disk_size_info **state);

int path_disk_size_info_get_r_skip(const char *path, int64_t max_secs, struct path_disk_size_info **state, char **paths_to_skip, int num_skip);

void path_disk_size_info_delete_state(struct path_disk_size_info *state);

#endif
