/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __RMON_TOOLS_H
#define __RMON_TOOLS_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <stddef.h>
#include <fcntl.h>
#include <limits.h>

#include <sys/types.h>
#include <sys/wait.h>

#include <sys/stat.h>
#include <fts.h>
#include <dirent.h>

#include "debug.h"
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "path.h"
#include "getopt_aux.h"
#include "int_sizes.h"

#include "rmsummary.h"

#define ALL_SUMMARIES_CATEGORY "(all)"
#define DEFAULT_CATEGORY "(without category)"

#define RULE_PREFIX "resource-rule-"
#define RULE_SUFFIX ".summary"

#define MAX_LINE 1024

enum fields      { TASK_ID = 0, NUM_TASKS, WALL_TIME, CPU_TIME, MAX_PROCESSES, TOTAL_PROCESSES, VIRTUAL, RESIDENT, SWAP, B_READ, B_WRITTEN, B_RX, B_TX, BANDWIDTH, FILES, DISK, CORES_PEAK, CORES_AVG, NUM_FIELDS};

struct rmDsummary
{
	char    *command;
	char    *category;
	char    *task_id;

	char    *file;


	double start;
	double end;
	double number_of_tasks;

	double  wall_time;
	double  total_processes;
	double  max_concurrent_processes;
	double  cpu_time;
	double  virtual_memory;
	double  memory;
	double  swap_memory;
	double  bytes_read;
	double  bytes_written;
	double  bytes_received;
	double  bytes_sent;
	double  bandwidth;
	double  total_files;
	double  disk;

	double  cores;
	double  cores_avg;
};

struct rmDsummary_set
{
	char  *category;
	struct list *summaries;

	//per resource, address by field
	struct itable *histograms;

	uint64_t overhead_min_waste_time_dependence;
	uint64_t overhead_min_waste_time_independence;
	uint64_t overhead_min_waste_brute_force;
	uint64_t overhead_max_throughput;
	uint64_t overhead_max_throughput_brute_force;
};

struct field {
	char  *abbrev;
	char  *name;
	char  *caption;
	char  *units;
	char  *format;
	int    cummulative;
	int    active;
	size_t offset;
};

extern struct field fields[];

#define value_of_field(s, f) (*((double *) ((char *) s + (f)->offset)))

#define assign_to_field(s, f, v)\
	*((double *) ((char *) s + (f)->offset)) = (double) v

char *sanitize_path_name(char *name);

char *get_rule_number(char *filename);

char *make_field_names_str(char *separator);

struct rmDsummary *rmsummary_to_rmDsummary(struct rmsummary *so);

struct rmDsummary *summary_bin_op(struct rmDsummary *s, struct rmDsummary *a, struct rmDsummary *b, double (*op)(double, double));
struct rmDsummary *summary_unit_op(struct rmDsummary *s, struct rmDsummary *a, double u, double (*op)(double, double));

double plus(double a, double b);
double minus(double a, double b);
double mult(double a, double b);
double minus_squared(double a, double b);
double divide(double a, double b);

void parse_fields_options(char *field_str);

char *parse_executable_name(char *command);

void parse_summary_from_filelist(struct rmDsummary_set *dest, char *filename, struct hash_table *categories);
void parse_summary_recursive(struct rmDsummary_set *dest, char *dirname, struct hash_table *categories);

struct rmDsummary_set *make_new_set(char *category);

void rmDsummary_print(FILE *output, struct rmDsummary *so);

char *field_str(struct field *f, double value);

#endif
