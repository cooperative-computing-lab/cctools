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
#include "int_sizes.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "debug.h"
#include "list.h"
#include "rmsummary.h"

#define ONE_MEGABYTE 1048576 /* this many bytes */
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
	rmsummary_assign_as_string_field(s, key, value, limits_exceeded);
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

void rmsummary_print(FILE *stream, struct rmsummary *s, struct rmsummary *limits, char *preamble, char *epilogue)
{

	fprintf(stream, "---\n\n");
	if(preamble)
		fprintf(stream, "%s", preamble);

	if(s->command)
		fprintf(stream, "%s %s\n",  "command:", s->command);

	if(s->category)
		fprintf(stream, "%-15s%s\n",  "category:", s->category);

	if(s->exit_type)
		fprintf(stream, "%-20s%20s\n",  "exit_type:", s->exit_type);

	fprintf(stream, "%-20s%20" PRId64 "\n",  "exit_status:", s->exit_status);

	if(s->last_error)
		fprintf(stream, "%-20s%20" PRId64 " %s\n",  "last_error:", s->last_error, strerror(s->last_error));

	if(s->exit_type)
	{
		if( strcmp(s->exit_type, "signal") == 0 )
			fprintf(stream, "%-20s%20" PRId64 "\n",  "signal:", s->signal);
		else if( strcmp(s->exit_type, "limits") == 0 )
			fprintf(stream, "%-20s%s\n",  "limits_exceeded:", s->limits_exceeded);
	}

	rmsummary_print_only_resources(stream, s, "");

	if(limits) {
		rmsummary_print_only_resources(stream, limits, "limits_");
	}

	if(epilogue)
		fprintf(stream, "%s", epilogue);
}

void rmsummary_print_only_resources(FILE *stream, struct rmsummary *s, const char *prefix)
{
	if(!prefix){
		prefix = "";
	}

	if(s->start > -1)
		fprintf(stream, "%-20s%20lf s\n", "start:", s->start / 1000000e0);

	if(s->end > -1)
		fprintf(stream, "%-20s%20lf s\n", "end:",  s->end / 1000000e0);

	if(s->wall_time > -1)
		fprintf(stream, "%s%-20s%20lf s\n", prefix, "wall_time:", s->wall_time >= 0 ? s->wall_time / 1000000e0 : -1);

	if(s->cpu_time > -1)
		fprintf(stream, "%s%-20s%20lf s\n", prefix, "cpu_time:", s->cpu_time   >= 0 ? s->cpu_time  / 1000000e0 : -1);

	if(s->cores > -1)
		fprintf(stream, "%s%-20s%20" PRId64 "\n", prefix,  "cores:", s->cores);

	//Disable printing gpus for now, as we cannot measure them.
	//if(s->gpus > -1)
	//	fprintf(stream, "%s%-20s%20" PRId64 "\n",  prefix, "gpus:", s->gpus);

	if(s->max_concurrent_processes > -1)
		fprintf(stream, "%s%-20s%15" PRId64 " procs\n",  prefix, "max_concurrent_processes:", s->max_concurrent_processes);

	if(s->total_processes > -1)
		fprintf(stream, "%s%-20s%20" PRId64 " procs\n",  prefix, "total_processes:", s->total_processes);

	if(s->virtual_memory > -1)
		fprintf(stream, "%s%-20s%20" PRId64 " MB\n",  prefix, "virtual_memory:", s->virtual_memory);

	if(s->memory > -1)
		fprintf(stream, "%s%-20s%20" PRId64 " MB\n",  prefix, "memory:", s->memory);

	if(s->swap_memory > -1)
		fprintf(stream, "%s%-20s%20" PRId64 " MB\n",  prefix, "swap_memory:", s->swap_memory);

	if(s->bytes_read > -1)
		fprintf(stream, "%s%-20s%20" PRId64 " B\n",  prefix, "bytes_read:", s->bytes_read);

	if(s->bytes_written > -1)
		fprintf(stream, "%s%-20s%20" PRId64 " B\n",  prefix, "bytes_written:", s->bytes_written);

	if(s->total_files > -1)
		fprintf(stream, "%s%-20s%20" PRId64 " files+dirs\n",  prefix, "total_files:", s->total_files);

	if(s->disk > -1)
		fprintf(stream, "%s%-20s%20" PRId64 " MB\n", prefix, "disk:", s->disk);
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

struct rmsummary *rmsummary_parse_limits_exceeded(const char *limits_exceeded)
{
	struct rmsummary *limits = NULL;

	if(limits_exceeded)
		limits = rmsummary_parse_from_str(limits_exceeded, ',');

	return limits;
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
