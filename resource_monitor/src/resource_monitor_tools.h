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

// Summaries in a set belong to the same category.
// The *stats are divided per resource.
struct rmsummary_set
{
	char  *category_name;
	struct list *summaries;

	uint64_t overhead_min_waste_time_dependence;
	uint64_t overhead_min_waste_time_independence;
	uint64_t overhead_min_waste_brute_force;
	uint64_t overhead_max_throughput;
	uint64_t overhead_max_throughput_brute_force;

	//per resource, address by field
	struct hash_table *stats;

};


char *sanitize_path_name(const char *name);

char *get_rule_number(const char *filename);

int field_is_active(const char *key);
int field_is_cumulative(const char *key);

void parse_fields_options(char *field_str);
char *parse_executable_name(char *command);

void parse_summary_from_filelist(struct rmsummary_set *dest, char *filename, struct hash_table *categories);
void parse_summary_recursive(struct rmsummary_set *dest, char *dirname, struct hash_table *categories);

struct rmsummary_set *make_new_set(char *category);

#endif
