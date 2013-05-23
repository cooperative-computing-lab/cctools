/*
  Copyright (C) 2013- The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>

#include <ctype.h>

#include "int_sizes.h"
#include "xxmalloc.h"
#include "debug.h"
#include "list.h"
#include "rmsummary.h"

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
	rmsummary_assign_as_int_field   (s, key, value, max_processes);
	rmsummary_assign_as_int_field   (s, key, value, num_processes);
	rmsummary_assign_as_time_field  (s, key, value, cpu_time);
	rmsummary_assign_as_int_field   (s, key, value, virtual_memory);
	rmsummary_assign_as_int_field   (s, key, value, resident_memory);
	rmsummary_assign_as_int_field   (s, key, value, swap_memory);
	rmsummary_assign_as_int_field   (s, key, value, bytes_read);
	rmsummary_assign_as_int_field   (s, key, value, bytes_written);
	rmsummary_assign_as_int_field   (s, key, value, workdir_num_files);
	rmsummary_assign_as_int_field   (s, key, value, workdir_footprint);
	rmsummary_assign_as_int_field   (s, key, value, fs_nodes);

	return 0;
}

#define MAX_LINE 256
/**  Reads a single summary from stream **/
struct rmsummary *rmsummary_parse_single(FILE *stream)
{
	char line[MAX_LINE], key[MAX_LINE], value[MAX_LINE];

	char c;
	while( (c = getc(stream)) == '#' || isblank(c) ) 
	{
		ungetc(c, stream);
		fgets(line, MAX_LINE, stream);
	}

	if(feof(stream))
		return NULL;

	struct rmsummary *s = calloc(1, sizeof(struct rmsummary));
	ungetc(c, stream);
	while( (c = getc(stream)) != EOF )
	{
		ungetc(c, stream);
		if(c == '#')
			break;
		
		fgets(line, MAX_LINE, stream);

		if(sscanf(line, "%[^:]:%*[ \t]%[^\n]", key, value) < 2) 
			continue;

		rmsummary_assign_field(s, key, value);
	}

	return s;
}

struct rmsummary *rmsummary_parse_file_single(char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");

	if(!stream)
	{
		debug(D_NOTICE, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	struct rmsummary *s = rmsummary_parse_single(stream);
	fclose(stream);

	return s;
}

void rmsummary_print(FILE *stream, struct rmsummary *s)
{
	fprintf(stream, "%-30s%s\n",  "command:", s->command);
	fprintf(stream, "%-30s%lf\n", "start:", s->start / 1000000e0);
	fprintf(stream, "%-30s%lf\n", "end:",   s->end   / 1000000e0);
	fprintf(stream, "%-30s%s\n",  "exit_type:", s->exit_type);
	fprintf(stream, "%-30s%" PRId64 "\n",  "exit_status:", s->exit_status);

	if( strcmp(s->exit_type, "signal") == 0 )
		fprintf(stream, "%-30s%" PRId64 "\n",  "signal:", s->signal);
	else if( strcmp(s->exit_type, "limits") == 0 )
		fprintf(stream, "%-30s%s\n",  "limits_exceeded:", s->limits_exceeded);

	fprintf(stream, "%-30s%lf\n", "wall_time:", s->wall_time / 1000000e0);
	fprintf(stream, "%-30s%" PRId64 "\n",  "max_processes:", s->max_processes);
	fprintf(stream, "%-30s%" PRId64 "\n",  "num_processes:", s->num_processes);
	fprintf(stream, "%-30s%lf\n", "cpu_time:", s->cpu_time / 1000000e0);
	fprintf(stream, "%-30s%" PRId64 "\n",  "virtual_memory:", s->virtual_memory);
	fprintf(stream, "%-30s%" PRId64 "\n",  "resident_memory:", s->resident_memory);
	fprintf(stream, "%-30s%" PRId64 "\n",  "swap_memory:", s->swap_memory);
	fprintf(stream, "%-30s%" PRId64 "\n",  "bytes_read:", s->bytes_read);
	fprintf(stream, "%-30s%" PRId64 "\n",  "bytes_written:", s->bytes_written);
	fprintf(stream, "%-30s%" PRId64 "\n",  "workdir_num_files:", s->workdir_num_files);
	fprintf(stream, "%-30s%" PRId64 "\n",  "workdir_footprint:", s->workdir_footprint);
}

struct list *rmsummary_parse_file_multiple(char *filename)
{
	struct list      *lst = list_create(0);
	struct rmsummary *s;

	FILE *stream;
	stream = fopen(filename, "r");
	if(!stream)
	{
		debug(D_NOTICE, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	while( (s = rmsummary_parse_single(stream)) )
	{
		list_push_tail(lst, s);
	}

	fclose(stream);

	return lst;
}

