/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_queue.h"
#include "batch_job.h"
#include "batch_file.h"

#include "sha1.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "path.h"
#include "hash_table.h"

#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <dirent.h>

struct hash_table *check_sums = NULL;
double total_checksum_time = 0.0;

/**
 * Create batch_file from outer_name and inner_name.
 * Outer/DAG name indicates the name that will be on the host/submission side.
 *  This is equivalent to the filename in Makeflow.
 * Inner/task name indicates the name that will be used for execution.
 *  IF no inner_name is given, or the specified batch_queue does not support
 *  remote renaming the outer_name will be used.
 **/
struct batch_file *batch_file_create(const char *outer_name, const char *inner_name)
{
	struct batch_file *f = calloc(1, sizeof(*f));
	f->outer_name = xxstrdup(outer_name);
	if (inner_name) {
		f->inner_name = xxstrdup(inner_name);
	} else {
		f->inner_name = xxstrdup(outer_name);
	}
	return f;
}

/**
 * Delete batch_file, including freeing outer_name and inner_name/
 **/
void batch_file_delete(struct batch_file *f)
{
	if (!f)
		return;

	free(f->outer_name);
	free(f->inner_name);

	free(f);
}

char *batch_file_to_string(struct batch_file *f)
{
	if (!strcmp(f->inner_name, f->outer_name)) {
		return strdup(f->outer_name);
	} else {
		return string_format("%s=%s", f->outer_name, f->inner_name);
	}
}

char *batch_file_list_to_string(struct list *file_list)
{
	struct batch_file *file;

	char *file_str = strdup("");
	char *separator = "";

	if (!file_list)
		return file_str;

	LIST_ITERATE(file_list, file)
	{
		/* Only add separator if past first item. */
		file_str = string_combine(file_str, separator);

		char *f = batch_file_to_string(file);
		file_str = string_combine(file_str, f);

		/* This could be set using batch_queue feature or option
		 * to allow for batch system specific separators. */
		separator = ",";

		free(f);
	}

	return file_str;
}

int batch_file_outer_compare(struct batch_file *file1, struct batch_file *file2)
{
	return strcmp(file1->outer_name, file2->outer_name);
}

/* Return the content based ID for a file.
 * generates the checksum of a file's contents if does not exist */
char *batch_file_generate_id(struct batch_file *f)
{
	if (check_sums == NULL) {
		check_sums = hash_table_create(0, 0);
	}
	char *check_sum_value = hash_table_lookup(check_sums, f->outer_name);
	if (check_sum_value == NULL) {
		unsigned char hash[SHA1_DIGEST_LENGTH];
		struct timeval start_time;
		struct timeval end_time;

		gettimeofday(&start_time, NULL);
		int success = sha1_file(f->outer_name, hash);
		gettimeofday(&end_time, NULL);
		double run_time = ((end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec)) / 1000000.0;
		total_checksum_time += run_time;
		debug(D_MAKEFLOW_HOOK, " The total checksum time is %lf", total_checksum_time);
		if (success == 0) {
			debug(D_MAKEFLOW, "Unable to checksum this file: %s", f->outer_name);
			return NULL;
		}
		f->hash = xxstrdup(sha1_string(hash));
		hash_table_insert(check_sums, f->outer_name, xxstrdup(sha1_string(hash)));
		debug(D_MAKEFLOW, "Checksum hash of %s is: %s", f->outer_name, f->hash);
		return xxstrdup(f->hash);
	}
	debug(D_MAKEFLOW, "Checksum already exists in hash table. Cached CHECKSUM hash of %s is: %s", f->outer_name, check_sum_value);
	return xxstrdup(check_sum_value);
}

/* Return the content based ID for a directory.
 * generates the checksum for the directories contents if does not exist
 * 		*NEED TO ACCOUNT FOR SYMLINKS LATER*  */
char *batch_file_generate_id_dir(char *file_name)
{
	if (check_sums == NULL) {
		check_sums = hash_table_create(0, 0);
	}
	char *check_sum_value = hash_table_lookup(check_sums, file_name);
	if (check_sum_value == NULL) {
		char *hash_sum = "";
		struct dirent **dp;
		int num;
		// Scans directory and sorts in reverse order
		num = scandir(file_name, &dp, NULL, alphasort);
		if (num < 0) {
			debug(D_MAKEFLOW, "Unable to scan %s", file_name);
			return NULL;
		} else {
			int i;
			for (i = num - 1; i >= 0; i--) {
				if (strcmp(dp[i]->d_name, ".") != 0 && strcmp(dp[i]->d_name, "..") != 0) {
					char *file_path = string_format("%s/%s", file_name, dp[i]->d_name);
					if (path_is_dir(file_path) == 1) {
						hash_sum = string_format("%s%s", hash_sum, batch_file_generate_id_dir(file_path));
					} else {
						unsigned char hash[SHA1_DIGEST_LENGTH];
						struct timeval start_time;
						struct timeval end_time;

						gettimeofday(&start_time, NULL);
						int success = sha1_file(file_path, hash);
						gettimeofday(&end_time, NULL);
						double run_time = ((end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec)) / 1000000.0;
						total_checksum_time += run_time;
						debug(D_MAKEFLOW_HOOK, " The total checksum time is %lf", total_checksum_time);
						if (success == 0) {
							debug(D_MAKEFLOW, "Unable to checksum this file: %s", file_path);
							free(file_path);
							free(dp[i]);
							continue;
						}
						hash_sum = string_format("%s%s:%s", hash_sum, file_name, sha1_string(hash));
					}
					free(file_path);
				}
				free(dp[i]);
			}
			free(dp);
			unsigned char hash[SHA1_DIGEST_LENGTH];
			sha1_buffer(hash_sum, strlen(hash_sum), hash);
			free(hash_sum);
			hash_table_insert(check_sums, file_name, xxstrdup(sha1_string(hash)));
			debug(D_MAKEFLOW, "Checksum hash of %s is: %s", file_name, sha1_string(hash));
			return xxstrdup(sha1_string(hash));
		}
	}
	debug(D_MAKEFLOW, "Checksum already exists in hash table. Cached CHECKSUM hash of %s is: %s", file_name, check_sum_value);
	return check_sum_value;
}
