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

#include <ctype.h>

#include "buffer.h"
#include "debug.h"
#include "int_sizes.h"
#include "jx_print.h"
#include "list.h"
#include "macros.h"
#include "rmsummary.h"
#include "stringtools.h"
#include "xxmalloc.h"

#define MAX_LINE 1024

#define rmsummary_assign_as_int_field(s, key, value, field)	\
	if(!strcmp(#field, key)){				\
		s->field = strtoll(value, NULL, 10);		\
		return 1;}

//From seconds to microseconds
#define rmsummary_assign_as_time_field(s, key, value, field)		\
	if(!strcmp(#field, key)){					\
		s->field = (int64_t) 1000000*strtod(value, NULL);	\
		return 1;}

#define rmsummary_assign_as_string_field(s, key, value, field)	\
	if(!strcmp(#field, key)){				\
		if(s->field) free(s->field);        \
		s->field = xxstrdup(value);			\
		return 1;}

int rmsummary_assign_field(struct rmsummary *s, char *key, char *value)
{
	rmsummary_assign_as_string_field(s, key, value, category);
	rmsummary_assign_as_string_field(s, key, value, command);
	rmsummary_assign_as_int_field   (s, key, value, task_id);
	rmsummary_assign_as_time_field  (s, key, value, start);
	rmsummary_assign_as_time_field  (s, key, value, end);
	rmsummary_assign_as_string_field(s, key, value, exit_type);
	rmsummary_assign_as_int_field   (s, key, value, signal);
	rmsummary_assign_as_int_field   (s, key, value, exit_status);
	rmsummary_assign_as_int_field   (s, key, value, last_error);
	rmsummary_assign_as_time_field  (s, key, value, wall_time);
	rmsummary_assign_as_int_field   (s, key, value, max_concurrent_processes);
	rmsummary_assign_as_int_field   (s, key, value, total_processes);
	rmsummary_assign_as_time_field  (s, key, value, cpu_time);
	rmsummary_assign_as_int_field   (s, key, value, virtual_memory);
	rmsummary_assign_as_int_field   (s, key, value, memory);
	rmsummary_assign_as_int_field   (s, key, value, swap_memory);
	rmsummary_assign_as_int_field   (s, key, value, bytes_read);
	rmsummary_assign_as_int_field   (s, key, value, bytes_written);
	rmsummary_assign_as_int_field   (s, key, value, total_files);
	rmsummary_assign_as_int_field   (s, key, value, disk);
	rmsummary_assign_as_int_field   (s, key, value, cores);
	rmsummary_assign_as_int_field   (s, key, value, gpus);

	return 0;
}

/** Reads a single summary from stream. Summaries are not parsed, here
	we simply read between markers (---) **/
char *rmsummary_read_single_chunk(FILE *stream)
{
	struct buffer b;
	char   line[MAX_LINE];

	/* Skip comments, blank lines, and markers before the summary */
	char c;
	while( (c = getc(stream)) == '#' || isblank(c) || c == '-' )
	{
		/* Make sure we read complete comment lines */
		do {
			line[MAX_LINE - 1] = '\0';
			fgets(line, MAX_LINE, stream);
		} while(line[MAX_LINE - 1]);
	}

	if(feof(stream))
	{
		return NULL;
	}

	buffer_init(&b);

	ungetc(c, stream);
	while(fgets(line, MAX_LINE, stream))
	{
		if(string_prefix_is(line, "---")) {
			/* we got to the end of document */
			break;
		}

		buffer_printf(&b, "%s", line);
	}

	char *summ = xxstrdup(buffer_tostring(&b));

	buffer_free(&b);

	return summ;
}

/* Parse a string for summary fields. Separator is usually '\n' or ',' */
struct rmsummary *rmsummary_parse_from_str(const char *buffer, const char separator)
{
	char key[MAX_LINE], value[MAX_LINE];
	char *token, *saveptr;

	if(!buffer)
		return NULL;

	// strtok does not work with const char.
	char *buffer_copy = xxstrdup(buffer);

	const char delim[] = { separator, '\n', '\0'};

	struct rmsummary *s = rmsummary_create(-1);

	/* if source have no last_error, we do not want the -1 from above */
	s->last_error = 0;

	token = strtok_r(buffer_copy, delim, &saveptr);
	while(token)
	{
		if(sscanf(token, "%[^:]:%*[ \t]%[^\n]", key, value) >= 2)
		{
			char *key_trim   = string_trim_spaces(key);
			char *value_trim = string_trim_spaces(value);

			/* remove units if present */
			if(strcmp("limits_exceeded", key_trim) != 0)
			{
				char *space = strchr(value_trim, ' ');
				if(space)
					*space = '\0';
			}

			rmsummary_assign_field(s, key_trim, value_trim);
		}
		token = strtok_r(NULL, delim, &saveptr);
	}

	free(buffer_copy);

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

	char *buffer = rmsummary_read_single_chunk(stream);
	struct rmsummary *s = rmsummary_parse_from_str(buffer, '\n');

	free(buffer);
	fclose(stream);

	return s;
}

struct jx *rmsummary_to_json(struct rmsummary *s) {
	struct jx *output = jx_object(NULL);
	struct jx *array;

	if(s->disk > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->disk));
		jx_array_append(array, jx_string("MB"));
		jx_insert(output, jx_string("cpu_time"), array);
	}

	if(s->total_files > -1)
		jx_insert_integer(output, "total_files",   s->total_files);

	if(s->bytes_written > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->bytes_written));
		jx_array_append(array, jx_string("B"));
		jx_insert(output, jx_string("bytes_written"), array);
	}

	if(s->bytes_read > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->bytes_read));
		jx_array_append(array, jx_string("B"));
		jx_insert(output, jx_string("bytes_read"), array);
	}

	if(s->swap_memory > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->swap_memory));
		jx_array_append(array, jx_string("MB"));
		jx_insert(output, jx_string("swap_memory"), array);
	}

	if(s->memory > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->memory));
		jx_array_append(array, jx_string("MB"));
		jx_insert(output, jx_string("memory"), array);
	}

	if(s->virtual_memory > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->virtual_memory));
		jx_array_append(array, jx_string("MB"));
		jx_insert(output, jx_string("virtual_memory"), array);
	}

	if(s->total_processes > -1)
		jx_insert_integer(output, "total_processes",   s->total_processes);

	if(s->max_concurrent_processes > -1)
		jx_insert_integer(output, "max_concurrent_processes",   s->max_concurrent_processes);

	if(s->cores > -1)
		jx_insert_integer(output, "cores",   s->cores);

	if(s->cpu_time > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->cpu_time/1e6));
		jx_array_append(array, jx_string("s"));
		jx_insert(output, jx_string("cpu_time"), array);
	}

	if(s->wall_time > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->wall_time/1e6));
		jx_array_append(array, jx_string("s"));
		jx_insert(output, jx_string("wall_time"), array);
	}

	if(s->end > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->end/1e6));
		jx_array_append(array, jx_string("s"));
		jx_insert(output, jx_string("end"), array);
	}

	if(s->start > -1) {
		array = jx_array(NULL);
		jx_array_append(array, jx_double(s->start/1e6));
		jx_array_append(array, jx_string("s"));
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

	if(s->exit_type)
		jx_insert_string(output, "exit_type", s->exit_type);

	if(s->command)
		jx_insert_string(output, "command",   s->command);

	if(s->category)
		jx_insert_string(output, "category",  s->category);


	return output;
}

void rmsummary_print(FILE *stream, struct rmsummary *s, struct jx *verbatim_fields)
{
	struct jx *jsum = rmsummary_to_json(s);

	if(verbatim_fields) {
		if(!jx_istype(verbatim_fields, JX_OBJECT)) {
			fatal("Vebatim fields is not a json object.");
		}
		struct jx_pair *head = verbatim_fields->u.pairs;

		while(head) {
			jx_insert(jsum, head->key, head->value);
			head = head->next;
		}
	}

	jx_print_stream(jsum, stream);
	jx_delete(jsum);
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
	struct rmsummary *s;
	char             *buffer;

	buffer = rmsummary_read_single_chunk(stream);
	s      = rmsummary_parse_from_str(buffer, '\n');

	return s;
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
void rmsummary_bin_op(struct rmsummary *dest, struct rmsummary *src, rm_bin_op fn)
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

void rmsummary_merge_override(struct rmsummary *dest, struct rmsummary *src)
{
	rmsummary_bin_op(dest, src, override_field);
}


/* Select the max of the fields */
static int64_t max_field(int64_t d, int64_t s)
{
	return (d > s) ? d : s;
}

void rmsummary_merge_max(struct rmsummary *dest, struct rmsummary *src)
{
	rmsummary_bin_op(dest, src, max_field);
}

/* Select the min of the fields, ignoring negative numbers */
static int64_t min_field(int64_t d, int64_t s)
{
	if(d < 0 || s < 0) {
		return MAX(-1, MAX(s, d)); /* return at least -1. */
	} else {
		return MIN(s, d);
	}
}

void rmsummary_merge_min(struct rmsummary *dest, struct rmsummary *src)
{
	rmsummary_bin_op(dest, src, min_field);
}

void rmsummary_debug_report(struct rmsummary *s)
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
