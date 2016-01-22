/*
Copyright (C) 2013- The University of Notre Dame This software is
distributed under the GNU General Public License.  See the file
COPYING for details.
*/

#ifndef __RMSUMMARY_H
#define __RMSUMMARY_H

#include <stdio.h>
#include <stdlib.h>

#include "jx.h"
#include "int_sizes.h"

/* Environment variables names */
#define RESOURCES_CORES  "CORES"
#define RESOURCES_MEMORY "MEMORY"
#define RESOURCES_DISK   "DISK"
#define RESOURCES_GPUS   "GPUS"

// These fields are defined as signed integers, even though they
// will only contain positive numbers. This is to conversion to
// signed quantities when comparing to maximum limits.
struct rmsummary
{
	char    *category;
	char    *command;

	int64_t  start;
	int64_t  end;

	char    *exit_type;
	int64_t  signal;
	int64_t  exit_status;
	int64_t  last_error;

	int64_t  wall_time;
	int64_t  total_processes;
	int64_t  max_concurrent_processes;
	int64_t  cpu_time;
	int64_t  virtual_memory;
	int64_t  memory;                     /* a.k.a. resident memory */
	int64_t  swap_memory;

	int64_t  bytes_read;
	int64_t  bytes_written;

	int64_t  bytes_sent;
	int64_t  bytes_received;

	int64_t  total_files;
	int64_t  disk;

	int64_t  cores;
	int64_t  gpus;
	int64_t  task_id;

	struct rmsummary *limits_exceeded;

	/* these fields are not used when reading/printing summaries */
	int64_t  fs_nodes;

};

struct rmsummary_field
{
	char   *name;
	size_t  offset;
	int     type;
	union { uint64_t integer;
		double   real;
		char    *string;
	}       value;
};

void rmsummary_print(FILE *stream, struct rmsummary *s, struct jx *verbatim_fields);

int rmsummary_assign_int_field(struct rmsummary *s, const char *key, int64_t value);
int rmsummary_assign_char_field(struct rmsummary *s, const char *key, char *value);

/**  Reads a single summary file from filename **/
struct rmsummary *rmsummary_parse_file_single(const char *filename);

/**  Reads all summaries from filename **/
struct list *rmsummary_parse_file_multiple(const char *filename);

/**  Reads a single summary from stream. summaries are separated by '#' or '\n'. **/
struct rmsummary *rmsummary_parse_next(FILE *stream);

struct rmsummary *rmsummary_create(signed char default_value);
void rmsummary_delete(struct rmsummary *s);

void rmsummary_read_env_vars(struct rmsummary *s);

void rmsummary_merge_override(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_merge_max(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_merge_min(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_debug_report(const struct rmsummary *s);

#endif
