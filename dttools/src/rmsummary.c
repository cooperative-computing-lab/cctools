/*
  Copyright (C) 2013- The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.
*/

/* In a summary file, all time fields are written as double, with
   units in seconds. Internally, however, time fields are kept as
   int64_t, with units in microseconds. Therefore we need to convert back
   and forth when reading/printing summaries.

   Similarly, workdir_footprint is reported as double, in megabytes,
   but it is kept internally as int64_t, in bytes.*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <math.h>

#include <ctype.h>

#include "buffer.h"
#include "debug.h"
#include "int_sizes.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"
#include "list.h"
#include "macros.h"
#include "rmsummary.h"
#include "stringtools.h"
#include "xxmalloc.h"

#define MAX_LINE 1024

int rmsummary_assign_char_field(struct rmsummary *s, const char *key, char *value) {
	if(strcmp(key, "category") == 0) {
		if(s->category)
			free(s->category);
		s->category = xxstrdup(value);
		return 1;
	}

	if(strcmp(key, "command") == 0) {
		if(s->command)
			free(s->command);
		s->command = xxstrdup(value);
		return 1;
	}

	if(strcmp(key, "exit_type") == 0) {
		if(s->exit_type)
			free(s->exit_type);
		s->exit_type = xxstrdup(value);
		return 1;
	}

	return 0;
}

int rmsummary_assign_int_field(struct rmsummary *s, const char *key, int64_t value) {
	if(strcmp(key, "task_id") == 0) {
		s->task_id = value;
		return 1;
	}

	if(strcmp(key, "start") == 0) {
		s->start = value;
		return 1;
	}

	if(strcmp(key, "end") == 0) {
		s->end = value;
		return 1;
	}

	if(strcmp(key, "wall_time") == 0) {
		s->wall_time = value;
		return 1;
	}

	if(strcmp(key, "cpu_time") == 0) {
		s->cpu_time = value;
		return 1;
	}

	if(strcmp(key, "signal") == 0) {
		s->signal = value;
		return 1;
	}

	if(strcmp(key, "exit_status") == 0) {
		s->exit_status = value;
		return 1;
	}

	if(strcmp(key, "last_error") == 0) {
		s->last_error = value;
		return 1;
	}

	if(strcmp(key, "max_concurrent_processes") == 0) {
		s->max_concurrent_processes = value;
		return 1;
	}

	if(strcmp(key, "total_processes") == 0) {
		s->total_processes = value;
		return 1;
	}

	if(strcmp(key, "virtual_memory") == 0) {
		s->virtual_memory = value;
		return 1;
	}

	if(strcmp(key, "memory") == 0) {
		s->memory = value;
		return 1;
	}

	if(strcmp(key, "swap_memory") == 0) {
		s->swap_memory = value;
		return 1;
	}

	if(strcmp(key, "bytes_read") == 0) {
		s->bytes_read = value;
		return 1;
	}

	if(strcmp(key, "bytes_written") == 0) {
		s->bytes_written = value;
		return 1;
	}

	if(strcmp(key, "total_files") == 0) {
		s->total_files = value;
		return 1;
	}

	if(strcmp(key, "disk") == 0) {
		s->disk = value;
		return 1;
	}

	if(strcmp(key, "cores") == 0) {
		s->cores = value;
		return 1;
	}

	if(strcmp(key, "gpus") == 0) {
		s->gpus = value;
		return 1;
	}

	return 0;
}


struct jx *rmsummary_to_json(struct rmsummary *s) {
	struct jx *output = jx_object(NULL);
	struct jx *array;

	if(s->disk > -1) {
		array = jx_arrayv(jx_integer(s->disk), jx_string("MB"), NULL);
		jx_insert(output, jx_string("disk"), array);
	}

	if(s->total_files > -1)
		jx_insert_integer(output, "total_files",   s->total_files);

	if(s->bytes_written > -1) {
		array = jx_arrayv(jx_integer(s->bytes_written), jx_string("MB"), NULL);
		jx_insert(output, jx_string("bytes_written"), array);
	}

	if(s->bytes_read > -1) {
		array = jx_arrayv(jx_integer(s->bytes_read), jx_string("MB"), NULL);
		jx_insert(output, jx_string("bytes_read"), array);
	}

	if(s->swap_memory > -1) {
		array = jx_arrayv(jx_integer(s->swap_memory), jx_string("MB"), NULL);
		jx_insert(output, jx_string("swap_memory"), array);
	}

	if(s->memory > -1) {
		array = jx_arrayv(jx_integer(s->memory), jx_string("MB"), NULL);
		jx_insert(output, jx_string("memory"), array);
	}

	if(s->virtual_memory > -1) {
		array = jx_arrayv(jx_integer(s->virtual_memory), jx_string("MB"), NULL);
		jx_insert(output, jx_string("virtual_memory"), array);
	}

	if(s->total_processes > -1)
		jx_insert_integer(output, "total_processes",   s->total_processes);

	if(s->max_concurrent_processes > -1)
		jx_insert_integer(output, "max_concurrent_processes",   s->max_concurrent_processes);

	if(s->cores > -1)
		jx_insert_integer(output, "cores",   s->cores);

	if(s->cpu_time > -1) {
		array = jx_arrayv(jx_integer(s->cpu_time), jx_string("us"), NULL);
		jx_insert(output, jx_string("cpu_time"), array);
	}

	if(s->wall_time > -1) {
		array = jx_arrayv(jx_integer(s->wall_time), jx_string("us"), NULL);
		jx_insert(output, jx_string("wall_time"), array);
	}

	if(s->end > -1) {
		array = jx_arrayv(jx_integer(s->end), jx_string("us"), NULL);
		jx_insert(output, jx_string("end"), array);
	}

	if(s->start > -1) {
		array = jx_arrayv(jx_integer(s->start), jx_string("us"), NULL);
		jx_insert(output, jx_string("start"), array);
	}

	if(s->exit_type)
	{
		if( strcmp(s->exit_type, "signal") == 0 )
			jx_insert_integer(output, "signal", s->signal);
		else if( strcmp(s->exit_type, "limits") == 0 )
			if(s->limits_exceeded) {
				struct jx *lim = rmsummary_to_json(s->limits_exceeded);
				jx_insert(output, jx_string("limits_exceeded"), lim);
			}
		jx_insert_string(output, "exit_type", "limits");
	}

	if(s->last_error)
		jx_insert_integer(output, "last_error", s->last_error);

	if(s->exit_status)
		jx_insert_integer(output, "exit_status", s->exit_status);

	if(s->command)
		jx_insert_string(output, "command",   s->command);

	if(s->category)
		jx_insert_string(output, "category",  s->category);


	return output;
}

static int json_number_of_array(struct jx *array, char *field, int64_t *number) {

	struct jx_item *first = array->u.items;

	if(!first)
		return 0;

	double result;

	if(jx_istype(first->value, JX_DOUBLE)) {
		result = first->value->u.double_value;
	} else if(jx_istype(first->value, JX_INTEGER)) {
		result = (double) first->value->u.integer_value;
	} else {
		return 0;
	}

	struct jx_item *second = first->next;

	if(!second)
		return 0;

	if(!jx_istype(second->value, JX_STRING))
		return 0;

	char *unit = second->value->u.string_value;

	/* all values in MB, or useconds. Incomplete list! */

	if(strcmp(unit, "us") == 0) {
		result *= 1;
	} else if(strcmp(unit, "s") == 0) {
		result *= USECOND;
	} else if(strcmp(unit, "MB") == 0) {
		result *= 1;
	} else if(strcmp(unit, "B") == 0) {
		result *= MEGABYTE;
	}

	/* round for worst case. */
	if(strcmp(field, "start") == 0) {
		*number = (int64_t) floor(result);
	} else {
		*number = (int64_t) ceil(result);
	}

	return 1;
}

struct rmsummary *json_to_rmsummary(struct jx *j) {
	if(!j || !jx_istype(j, JX_OBJECT))
		return NULL;

	struct rmsummary *s = rmsummary_create(-1);

	struct jx_pair *head = j->u.pairs;
	while(head) {
		if(!jx_istype(head->key, JX_STRING))
			continue;

		char *key = head->key->u.string_value;
		struct jx *value = head->value;

		if(jx_istype(value, JX_STRING)) {
			rmsummary_assign_char_field(s, key, value->u.string_value);
		} else if(jx_istype(value, JX_INTEGER)) {
			rmsummary_assign_int_field(s, key, value->u.integer_value);
		} else if(jx_istype(value, JX_ARRAY)) {
			int64_t number;
			int status = json_number_of_array(value, key, &number);
			if(status) {
				rmsummary_assign_int_field(s, key, number);
			}
		}

		head = head->next;
	}

	return s;
}

/* Parse the file, assuming there is a single summary in it. */
struct rmsummary *rmsummary_parse_file_single(const char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");

	if(!stream)
	{
		debug(D_NOTICE, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	struct rmsummary *s = rmsummary_parse_next(stream);
	fclose(stream);

	return s;
}


/* Parse the file assuming there are multiple summaries in it. Summary
   boundaries are lines starting with # */
struct list *rmsummary_parse_file_multiple(const char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");
	if(!stream)
	{
		debug(D_NOTICE, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	struct list      *lst = list_create(0);
	struct rmsummary *s;

	do
	{
		s = rmsummary_parse_next(stream);

		if(s)
			list_push_tail(lst, s);
	} while(s);

	fclose(stream);

	return lst;
}

/* Parse the stream for the next summary */
struct rmsummary *rmsummary_parse_next(FILE *stream)
{
	struct jx *j = jx_parse_stream(stream);

	if(!j)
		return NULL;

	struct rmsummary *s = json_to_rmsummary(j);

	jx_delete(j);
	return s;
}

void rmsummary_print(FILE *stream, struct rmsummary *s, struct jx *verbatim_fields)
{
	struct jx *jsum = rmsummary_to_json(s);

	if(verbatim_fields) {
		if(!jx_istype(verbatim_fields, JX_OBJECT)) {
			fatal("Vebatim fields are not a json object.");
		}
		struct jx_pair *head = verbatim_fields->u.pairs;

		while(head) {
			jx_insert(jsum, jx_copy(head->key), jx_copy(head->value));
			head = head->next;
		}
	}

	jx_pretty_print_stream(jsum, stream);
	jx_delete(jsum);
}

/* Create summary filling all numeric fields with default_value, and
all string fields with NULL. Usual values are 0, or -1. */
struct rmsummary *rmsummary_create(signed char default_value)
{
	struct rmsummary *s = malloc(sizeof(struct rmsummary));
	memset(s, default_value, sizeof(struct rmsummary));

	s->command   = NULL;
	s->category  = NULL;
	s->exit_type = NULL;
	s->limits_exceeded = NULL;

	s->last_error  = 0;
	s->exit_status = 0;

	return s;
}

void rmsummary_delete(struct rmsummary *s)
{
	if(!s)
		return;

	if(s->command)
		free(s->command);

	if(s->category)
		free(s->category);

	if(s->exit_type)
		free(s->exit_type);

	if(s->limits_exceeded)
		free(s->limits_exceeded);

	free(s);
}

void rmsummary_read_env_vars(struct rmsummary *s)
{
	char *value;
	if((value = getenv( RESOURCES_CORES  )))
		s->cores  = atoi(value);
	if((value = getenv( RESOURCES_MEMORY )))
		s->memory = atoi(value);
	if((value = getenv( RESOURCES_DISK )))
		s->disk   = atoi(value);
}

#define rmsummary_apply_op(dest, src, fn, field) (dest)->field = fn((dest)->field, (src)->field)

typedef int64_t (*rm_bin_op)(int64_t, int64_t);
void rmsummary_bin_op(struct rmsummary *dest, const struct rmsummary *src, rm_bin_op fn)
{
	rmsummary_apply_op(dest, src, fn, start);
	rmsummary_apply_op(dest, src, fn, end);
	rmsummary_apply_op(dest, src, fn, exit_status);
	rmsummary_apply_op(dest, src, fn, last_error);
	rmsummary_apply_op(dest, src, fn, wall_time);
	rmsummary_apply_op(dest, src, fn, max_concurrent_processes);
	rmsummary_apply_op(dest, src, fn, total_processes);
	rmsummary_apply_op(dest, src, fn, cpu_time);
	rmsummary_apply_op(dest, src, fn, virtual_memory);
	rmsummary_apply_op(dest, src, fn, memory);
	rmsummary_apply_op(dest, src, fn, swap_memory);
	rmsummary_apply_op(dest, src, fn, bytes_read);
	rmsummary_apply_op(dest, src, fn, bytes_written);
	rmsummary_apply_op(dest, src, fn, total_files);
	rmsummary_apply_op(dest, src, fn, disk);

	rmsummary_apply_op(dest, src, fn, cores);
	rmsummary_apply_op(dest, src, fn, fs_nodes);
}

/* Copy the value for all the fields in src > -1 to dest */
static int64_t override_field(int64_t d, int64_t s)
{
	return (s > -1) ? s : d;
}

void rmsummary_merge_override(struct rmsummary *dest, const struct rmsummary *src)
{
	rmsummary_bin_op(dest, src, override_field);
}


/* Select the max of the fields */
static int64_t max_field(int64_t d, int64_t s)
{
	return (d > s) ? d : s;
}

void rmsummary_merge_max(struct rmsummary *dest, const struct rmsummary *src)
{
	rmsummary_bin_op(dest, src, max_field);
}

/* Select the min of the fields, ignoring negative numbers */
static int64_t min_field(int64_t d, int64_t s)
{
	if(d < 0 || s < 0) {
		return MAX(-1, MAX(s, d)); /* return at least -1. treat -1 as undefined.*/
	} else {
		return MIN(s, d);
	}
}

void rmsummary_merge_min(struct rmsummary *dest, const struct rmsummary *src)
{
	rmsummary_bin_op(dest, src, min_field);
}

void rmsummary_debug_report(const struct rmsummary *s)
{
	if(s->cores != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "cores", s->cores);
	if(s->start != -1)
		debug(D_DEBUG, "max resource %-18s  s: %lf\n", "start", ((double) s->start / 1000000));
	if(s->end != -1)
		debug(D_DEBUG, "max resource %-18s  s: %lf\n", "end",   ((double) s->end   / 1000000));
	if(s->wall_time != -1)
		debug(D_DEBUG, "max resource %-18s  s: %lf\n", "wall_time", ((double) s->wall_time / 1000000));
	if(s->max_concurrent_processes != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "max_processes_processes", s->max_concurrent_processes);
	if(s->total_processes != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "total_processes", s->total_processes);
	if(s->cpu_time != -1)
		debug(D_DEBUG, "max resource %-18s  s: %lf\n", "cpu_time",  ((double) s->cpu_time  / 1000000));
	if(s->virtual_memory != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "virtual_memory", s->virtual_memory);
	if(s->memory != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "memory", s->memory);
	if(s->swap_memory != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "swap_memory", s->swap_memory);
	if(s->bytes_read != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "bytes_read", s->bytes_read);
	if(s->bytes_written != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "bytes_written", s->bytes_written);
	if(s->total_files != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "total_files", s->total_files);
	if(s->disk != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "disk", s->disk);
}

/* vim: set noexpandtab tabstop=4: */
