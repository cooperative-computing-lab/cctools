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
#include "buffer.h"

/* Environment variables names */
#define RESOURCES_CORES     "CORES"
#define RESOURCES_MEMORY    "MEMORY"
#define RESOURCES_DISK      "DISK"
#define RESOURCES_WALL_TIME "WALL_TIME"
#define RESOURCES_GPUS      "GPUS"
#define RESOURCES_MPI_PROCESSES "MPI_PROCESSES"

// These fields are defined as signed integers, even though they
// will only contain positive numbers. This is to conversion to
// signed quantities when comparing to maximum limits.
struct rmsummary
{
	char    *category;
	char    *command;
	char    *taskid;

	int64_t  start;                          /* usecs */
	int64_t  end;                            /* usecs */

	char    *exit_type;
	int64_t  signal;
	int64_t  exit_status;
	int64_t  last_error;

	int64_t  wall_time;                      /* usecs */
	int64_t  total_processes;
	int64_t  max_concurrent_processes;
	int64_t  cpu_time;                       /* usecs */
	int64_t  virtual_memory;                 /* MB */
	int64_t  memory;                         /* MB. a.k.a. resident memory */
	int64_t  swap_memory;                    /* MB */

	int64_t  bytes_read;                     /* B */
	int64_t  bytes_written;                  /* B */

	int64_t  bytes_sent;                     /* B */
	int64_t  bytes_received;                 /* B */
	int64_t  bandwidth;                      /* bps */

	int64_t  total_files;
	int64_t  disk;                           /* MB */

	int64_t  cores;                          /* peak usage in a small time window */
	int64_t  cores_avg;
	int64_t  context_switches;
	int64_t  gpus;
    int64_t  machine_load;                   /* peak load of the host */
    int64_t  machine_cpus;                   /* number of cpus of the host */

	struct rmsummary *limits_exceeded;
	struct rmsummary *peak_times;           /* from start, in usecs */

	char  *snapshot_name;                   /* NULL for main summary, otherwise label of the snapshot. */
	int    snapshots_count;                 /* number of intermediate measurements, if any. */
	struct rmsummary **snapshots;           /* snapshots_count sized array of snapshots. */

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

void rmsummary_print(FILE *stream, struct rmsummary *s, int pprint, struct jx *verbatim_fields);
void rmsummary_print_buffer(struct buffer *B, const struct rmsummary *s, int only_resources);
char *rmsummary_print_string(const struct rmsummary *s, int only_resources);

const char *rmsummary_unit_of(const char *key);

int rmsummary_assign_int_field(struct rmsummary *s, const char *key, int64_t value);
int rmsummary_assign_char_field(struct rmsummary *s, const char *key, char *value);

int64_t rmsummary_get_int_field(const struct rmsummary *s, const char *key);
const char *rmsummary_get_char_field(const struct rmsummary *s, const char *key);

/**  Reads a single summary file from filename **/
struct rmsummary *rmsummary_parse_file_single(const char *filename);

/**  Reads a single summary file from string **/
struct rmsummary *rmsummary_parse_string(const char *str);

/**  Reads all summaries from filename **/
struct list *rmsummary_parse_file_multiple(const char *filename);

struct jx *rmsummary_to_json(const struct rmsummary *s, int only_resources);
struct rmsummary *json_to_rmsummary(struct jx *j);

struct rmsummary *rmsummary_create(signed char default_value);
void rmsummary_delete(struct rmsummary *s);

void rmsummary_read_env_vars(struct rmsummary *s);

void rmsummary_merge_max_w_time(struct rmsummary *dest, const struct rmsummary *src);

struct rmsummary *rmsummary_copy(const struct rmsummary *src);
void rmsummary_merge_override(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_merge_max(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_merge_min(struct rmsummary *dest, const struct rmsummary *src);
void rmsummary_add(struct rmsummary *dest, const struct rmsummary *src);

void rmsummary_debug_report(const struct rmsummary *s);

double rmsummary_to_external_unit(const char *field, int64_t n);
double rmsummary_to_base_unit(const char *field, int64_t n);
int rmsummary_to_internal_unit(const char *field, double input_number, int64_t *output_number, const char *unit);

size_t rmsummary_field_offset(const char *key);
int64_t rmsummary_get_int_field_by_offset(const struct rmsummary *s, size_t offset);

void rmsummary_add_conversion_field(const char *name, const char *internal, const char *external, const char *base, double exttoint, double inttobase, int float_flag);
int rmsummary_field_is_float(const char *key);

struct rmsummary *rmsummary_get_snapshot(const struct rmsummary *s, int i);

int rmsummary_check_limits(struct rmsummary *measured, struct rmsummary *limits);

#endif
