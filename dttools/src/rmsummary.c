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

#include "int_sizes.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "debug.h"
#include "list.h"
#include "rmsummary.h"

#define ONE_MEGABYTE 1048576 /* this many bytes */

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
		s->field = xxstrdup(value);			\
		return 1;}

int rmsummary_assign_field(struct rmsummary *s, char *key, char *value)
{
	rmsummary_assign_as_string_field(s, key, value, command);
	rmsummary_assign_as_time_field  (s, key, value, start);
	rmsummary_assign_as_time_field  (s, key, value, end);
	rmsummary_assign_as_string_field(s, key, value, exit_type);
	rmsummary_assign_as_int_field   (s, key, value, signal);
	rmsummary_assign_as_string_field(s, key, value, limits_exceeded);
	rmsummary_assign_as_int_field   (s, key, value, exit_status);
	rmsummary_assign_as_time_field  (s, key, value, wall_time);
	rmsummary_assign_as_int_field   (s, key, value, max_concurrent_processes);
	rmsummary_assign_as_int_field   (s, key, value, total_processes);
	rmsummary_assign_as_time_field  (s, key, value, cpu_time);
	rmsummary_assign_as_int_field   (s, key, value, virtual_memory);
	rmsummary_assign_as_int_field   (s, key, value, resident_memory);
	rmsummary_assign_as_int_field   (s, key, value, swap_memory);
	rmsummary_assign_as_int_field   (s, key, value, bytes_read);
	rmsummary_assign_as_int_field   (s, key, value, bytes_written);
	rmsummary_assign_as_int_field   (s, key, value, workdir_num_files);
	rmsummary_assign_as_int_field   (s, key, value, workdir_footprint);
	rmsummary_assign_as_int_field   (s, key, value, cores);

	return 0;
}

/** Reads a single summary from stream. Summaries are not parsed, here
    we simply read between comments (#) and blank lines for the lines
    describing the summary.**/
char *rmsummary_read_single(FILE *stream)
{
	int nmax   = 1024;
	int ntotal = 0;
	int nline  = 1024;
	int n;

	char *line = malloc( nline * sizeof(char) );
	char *summ = malloc( nmax  * sizeof(char) );

	/* Skip comments and blank lines before the summary */
	char c;
	while( (c = getc(stream)) == '#' || isblank(c) ) 
	{
		ungetc(c, stream);
		/* Make sure we read complete comment lines */
		n = nline;
		while( n >= (nline - 1) )
		{
			fgets(line, nline, stream);
			n = strlen(line);
		}
	}

	if(feof(stream))
	{
		free(line);
		free(summ);
		return NULL;
	}

	ungetc(c, stream);
	while( (c = getc(stream)) != EOF )
	{
		ungetc(c, stream);
		if(c == '#')
			break;
		
		fgets(line, nline, stream);
		n = strlen(line);

		if( ntotal + n > nmax )
		{
			nmax = ntotal + n + nline;
			line = realloc(line, nmax * sizeof(char) );
			if(!line)
				fatal("Could not read summary file : %s.\n", strerror(errno)); 
		}
		memcpy((summ + ntotal), line, n); 
		ntotal += n;
	}

	free(line);
	return summ;
}

/* Parse a string for summary fields. Separator is usually '\n' or ',' */
#define MAX_LINE 1024
struct rmsummary *rmsummary_parse_single(char *buffer, char separator)
{
	char key[MAX_LINE], value[MAX_LINE];
	char *token, *saveptr;

	if(!buffer)
		return NULL;

	buffer = xxstrdup(buffer);

	const char delim[] = { separator, '\n', '\0'}; 

	struct rmsummary *s = make_rmsummary(-1);

	token = strtok_r(buffer, delim, &saveptr);
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

	free(buffer);

	return s;
}

/* Parse the file, assuming there is a single summary in it. */
struct rmsummary *rmsummary_parse_file_single(char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");

	if(!stream)
	{
		debug(D_NOTICE, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	char *buffer = rmsummary_read_single(stream);
	struct rmsummary *s = rmsummary_parse_single(buffer, '\n');

	free(buffer);
	fclose(stream);

	return s;
}

void rmsummary_print(FILE *stream, struct rmsummary *s)
{
	fprintf(stream, "%-20s%20s\n",  "command:", s->command);
	fprintf(stream, "%-20s%20lf s\n", "start:", s->start >= 0 ? s->start / 1000000e0 : -1);
	fprintf(stream, "%-20s%20lf s\n", "end:",   s->end   >= 0 ? s->end   / 1000000e0 : -1);
	fprintf(stream, "%-20s%20s\n",  "exit_type:", s->exit_type);
	fprintf(stream, "%-20s%20" PRId64 "\n",  "exit_status:", s->exit_status);

	if( strcmp(s->exit_type, "signal") == 0 )
		fprintf(stream, "%-20s%20" PRId64 "\n",  "signal:", s->signal);
	else if( strcmp(s->exit_type, "limits") == 0 )
		fprintf(stream, "%-20s%s\n",  "limits_exceeded:", s->limits_exceeded);

	fprintf(stream, "%-20s%20lf s\n", "wall_time:", s->wall_time >= 0 ? s->wall_time / 1000000e0 : -1);
	fprintf(stream, "%-20s%15" PRId64 " procs\n",  "max_concurrent_processes:", s->max_concurrent_processes);
	fprintf(stream, "%-20s%20" PRId64 " procs\n",  "total_processes:", s->total_processes);
	fprintf(stream, "%-20s%20lf s\n", "cpu_time:", s->cpu_time   >= 0 ? s->cpu_time  / 1000000e0 : -1);
	fprintf(stream, "%-20s%20" PRId64 " MB\n",  "virtual_memory:", s->virtual_memory);
	fprintf(stream, "%-20s%20" PRId64 " MB\n",  "resident_memory:", s->resident_memory);
	fprintf(stream, "%-20s%20" PRId64 " MB\n",  "swap_memory:", s->swap_memory);
	fprintf(stream, "%-20s%20" PRId64 " B\n",  "bytes_read:", s->bytes_read);
	fprintf(stream, "%-20s%20" PRId64 " B\n",  "bytes_written:", s->bytes_written);
	fprintf(stream, "%-20s%20" PRId64 " files+dirs\n",  "workdir_num_files:", s->workdir_num_files);
	fprintf(stream, "%-20s%20" PRId64 " MB\n",  "workdir_footprint:",
		s->workdir_footprint >= 0 ? s->workdir_footprint : -1);
}

/* Parse the file assuming there are multiple summaries in it. Summary
   boundaries are lines starting with # */
struct list *rmsummary_parse_file_multiple(char *filename)
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
	char             *buffer;

	do
	{
		buffer = rmsummary_read_single(stream);
		s      = rmsummary_parse_single(buffer, '\n');
		if(s)
			list_push_tail(lst, s);
	} while(s);

	fclose(stream);

	return lst;
}


/* Create summary filling all numeric fields with default_value, and
all string fields with NULL. Usual values are 0, or -1. */
struct rmsummary *make_rmsummary(signed char default_value)
{
	struct rmsummary *s = malloc(sizeof(struct rmsummary));
	memset(s, default_value, sizeof(struct rmsummary));

	s->command   = NULL;
	s->exit_type = NULL;
	s->limits_exceeded = NULL;

	return s;
}

void rmsummary_read_env_vars(struct rmsummary *s)
{
	char *value;
	if((value = getenv( RESOURCES_CORES  )))
		s->cores           = atoi(value);
	if((value = getenv( RESOURCES_MEMORY )))
		s->resident_memory = atoi(value);
	if((value = getenv( RESOURCES_DISK )))
		s->workdir_footprint = atoi(value);
}

#define rmsummary_apply_op(dest, src, fn, field) (dest)->field = fn((dest)->field, (src)->field)

typedef int64_t (*rm_bin_op)(int64_t, int64_t);
void rmsummary_bin_op(struct rmsummary *dest, struct rmsummary *src, rm_bin_op fn)
{
	rmsummary_apply_op(dest, src, fn, start);
	rmsummary_apply_op(dest, src, fn, end);
	rmsummary_apply_op(dest, src, fn, exit_status);
	rmsummary_apply_op(dest, src, fn, wall_time);
	rmsummary_apply_op(dest, src, fn, max_concurrent_processes);
	rmsummary_apply_op(dest, src, fn, total_processes);
	rmsummary_apply_op(dest, src, fn, cpu_time);
	rmsummary_apply_op(dest, src, fn, virtual_memory);
	rmsummary_apply_op(dest, src, fn, resident_memory);
	rmsummary_apply_op(dest, src, fn, swap_memory);
	rmsummary_apply_op(dest, src, fn, bytes_read);
	rmsummary_apply_op(dest, src, fn, bytes_written);
	rmsummary_apply_op(dest, src, fn, workdir_num_files);
	rmsummary_apply_op(dest, src, fn, workdir_footprint);

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
	if(s->resident_memory != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "resident_memory", s->resident_memory);
	if(s->swap_memory != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "swap_memory", s->swap_memory);
	if(s->bytes_read != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "bytes_read", s->bytes_read);
	if(s->bytes_written != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "bytes_written", s->bytes_written);
	if(s->workdir_num_files != -1)
		debug(D_DEBUG, "max resource %-18s   : %" PRId64 "\n", "workdir_num_files", s->workdir_num_files);
	if(s->workdir_footprint != -1)
		debug(D_DEBUG, "max resource %-18s MB: %" PRId64 "\n", "workdir_footprint", s->workdir_footprint);
}
