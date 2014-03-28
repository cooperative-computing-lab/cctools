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

enum fields      { TASK_ID = 0, NUM_TASKS, WALL_TIME, CPU_TIME, MAX_PROCESSES, TOTAL_PROCESSES, VIRTUAL, RESIDENT, SWAP, B_READ, B_WRITTEN, FILES, FOOTPRINT, CORES, NUM_FIELDS};

struct rmDsummary
{
	char    *command;
	char    *category;

	char    *file;

	int64_t  task_id;

	double start;
	double end;
	double number_of_tasks;

	double  wall_time;
	double  total_processes;
	double  max_concurrent_processes;
	double  cpu_time;
	double  virtual_memory; 
	double  resident_memory; 
	double  swap_memory; 
	double  bytes_read;
	double  bytes_written;
	double  workdir_num_files;
	double  workdir_footprint;
	double  cores;

};

struct rmDsummary_set
{
	char  *category;
	struct list *summaries;

	//per resource, address by field
	struct itable *histograms;
};

struct field {
	char  *abbrev;
	char  *name;
	char  *units;
	int    active;
	size_t offset;
};

extern struct field fields[];

#define value_of_field(s, f) (*((double *) ((char *) s + (f)->offset)))

#define assign_to_field(s, f, v)\
	*((double *) ((char *) s + (f)->offset)) = (double) v

double usecs_to_secs(double usecs);
double secs_to_usecs(double secs);
double bytes_to_Mbytes(double bytes);
double Mbytes_to_bytes(double Mbytes);
double bytes_to_Gbytes(double bytes);
double Mbytes_to_Gbytes(double Mbytes);

char *sanitize_path_name(char *name);

int get_rule_number(char *filename);

char *make_field_names_str(char *separator);

struct rmDsummary *summary_bin_op(struct rmDsummary *s, struct rmDsummary *a, struct rmDsummary *b, double (*op)(double, double));
struct rmDsummary *summary_unit_op(struct rmDsummary *s, struct rmDsummary *a, double u, double (*op)(double, double));

double plus(double a, double b);
double minus(double a, double b);
double mult(double a, double b);
double minus_squared(double a, double b);
double divide(double a, double b);

void parse_fields_options(char *field_str);

struct rmDsummary *parse_summary(FILE *stream, char *filename);
struct rmDsummary *parse_summary_file(char *filename);
char *parse_executable_name(char *command);

void parse_summary_from_filelist(struct rmDsummary_set *dest, char *filename);
void parse_summary_recursive(struct rmDsummary_set *dest, char *dirname);

struct rmDsummary_set *make_new_set(char *category);

#endif
