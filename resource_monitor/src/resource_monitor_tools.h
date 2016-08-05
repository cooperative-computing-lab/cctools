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

enum fields      { TASK_ID = 0, NUM_TASKS, WALL_TIME, CPU_TIME, MAX_PROCESSES, TOTAL_PROCESSES, VIRTUAL, RESIDENT, SWAP, B_READ, B_WRITTEN, B_RX, B_TX, BANDWIDTH, FILES, DISK, CORES, NUM_FIELDS};

struct rmsummary_set
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
	int    cummulative;
	int    active;
	size_t offset;
};

extern struct field fields[];

#define value_of_field(s, f) (*((int64_t *) ((char *) s + (f)->offset)))

#define assign_to_field(s, f, v)\
	*((double *) ((char *) s + (f)->offset)) = (double) v

char *sanitize_path_name(char *name);

char *get_rule_number(char *filename);

char *make_field_names_str(char *separator);



void parse_fields_options(char *field_str);
char *parse_executable_name(char *command);

void parse_summary_from_filelist(struct rmsummary_set *dest, char *filename, struct hash_table *categories);
void parse_summary_recursive(struct rmsummary_set *dest, char *dirname, struct hash_table *categories);

struct rmsummary_set *make_new_set(char *category);

char *field_str(struct field *f, double value);

#endif
