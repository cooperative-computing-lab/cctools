/*
Copyright (C) 2022 The University of Notre Dame This software is
distributed under the GNU General Public License.  See the file
COPYING for details.
*/

#ifndef __RMSUMMARY_H
#define __RMSUMMARY_H

#include <stdio.h>
#include <stdlib.h>

#include "jx.h"
#include "int_sizes.h"
#include "buffer.h"

/* Environment variables names */
#define RESOURCES_CORES     "CORES"
#define RESOURCES_MEMORY    "MEMORY"
#define RESOURCES_DISK      "DISK"
#define RESOURCES_WALL_TIME "WALL_TIME"
#define RESOURCES_GPUS      "GPUS"
#define RESOURCES_MPI_PROCESSES "MPI_PROCESSES"


struct rmsummary
{
	char    *category;
	char    *command;
	char    *taskid;
	
	double cores;
	double gpus;
	double memory;
	double disk;

	char    *exit_type;
	int64_t  signal;
	int64_t  exit_status;
	int64_t  last_error;

	double start;
	double end;

	double cores_avg;

	double wall_time;
	double cpu_time;

	double virtual_memory;
	double swap_memory;

	double bytes_read;
	double bytes_written;

	double bytes_received;
	double bytes_sent;
	double bandwidth;

	double machine_cpus;
	double machine_load;
	double context_switches;

	double max_concurrent_processes;
	double total_processes;

	double total_files;
	double fs_nodes;

    double workers;

	struct rmsummary *limits_exceeded;
	struct rmsummary *peak_times; /* from start, in usecs */

	char  *snapshot_name;         /* NULL for root summary, otherwise the label of the snapshot */
	size_t snapshots_count;       /* number of intermediate measurements, if any. */
	struct rmsummary **snapshots; /* snapshots_count sized array of snapshots. */
};

void rmsummary_print(FILE *stream, struct rmsummary *s, int pprint, struct jx *verbatim_fields);
void rmsummary_print_buffer(struct buffer *B, const struct rmsummary *s, int only_resources);
char *rmsummary_print_string(const struct rmsummary *s, int only_resources);

/* Returns 1 if resource set, 0 if resource does not exist. */
int rmsummary_set(struct rmsummary *s, const char *resource, double value);
double rmsummary_get(const struct rmsummary *s, const char *resource);

void rmsummary_set_by_offset(struct rmsummary *s, size_t offset, double value);
double rmsummary_get_by_offset(const struct rmsummary *s, size_t offset);

/**  Reads a single summary file from filename **/
struct rmsummary *rmsummary_parse_file_single(const char *filename);

/**  Reads a single summary file from string. **/
struct rmsummary *rmsummary_parse_string(const char *str);

/**  Reads all summaries from filename. **/
struct list *rmsummary_parse_file_multiple(const char *filename);

struct jx *rmsummary_to_json(const struct rmsummary *s, int only_resources);
struct rmsummary *json_to_rmsummary(struct jx *j);

/** Create a new rmsummary. Common default_value's: 0 (for accumulation) -1 (resource value is undefined) **/
struct rmsummary *rmsummary_create(double default_value);
void rmsummary_delete(struct rmsummary *s);

void rmsummary_read_env_vars(struct rmsummary *s);

void rmsummary_merge_max_w_time(struct rmsummary *dest, const struct rmsummary *src);

struct rmsummary *rmsummary_copy(const struct rmsummary *src, int deep_copy);
void rmsummary_merge_override(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_merge_override_basic(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_merge_default(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_merge_max(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_merge_min(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_add(struct rmsummary *dest, const struct rmsummary *src);

void rmsummary_debug_report(const struct rmsummary *s);

struct rmsummary *rmsummary_get_snapshot(const struct rmsummary *s, size_t i);
int rmsummary_check_limits(struct rmsummary *measured, struct rmsummary *limits);

size_t rmsummary_num_resources();
const char **rmsummary_list_resources();

const char *rmsummary_resource_units(const char *resource_name);
int rmsummary_resource_decimals(const char *resource_name);
size_t rmsummary_resource_offset(const char *resource_name);

const char *rmsummary_resource_to_str(const char *resource, double value, int include_units);

#endif
