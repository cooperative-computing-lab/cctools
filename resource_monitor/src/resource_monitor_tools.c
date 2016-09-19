/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "resource_monitor_tools.h"
#include "macros.h"
#include "category.h"

#include "jx_parse.h"

struct hash_table *active_fields      = NULL;
struct hash_table *cummulative_fields = NULL;

void init_field_active() {
	active_fields = hash_table_create(0,0);

	hash_table_insert(active_fields, "cores",     active_fields);
	hash_table_insert(active_fields, "cores_avg", active_fields);
	hash_table_insert(active_fields, "disk",      active_fields);
	hash_table_insert(active_fields, "memory",    active_fields);
}

void init_field_cumulative() {

	cummulative_fields = hash_table_create(0,0);

	hash_table_insert(cummulative_fields, "wall_time", cummulative_fields);
	hash_table_insert(cummulative_fields, "cpu_time",  cummulative_fields);
}

int field_is_active(const char *key) {

	if(!active_fields) {
		init_field_active();
	}

	return !!(hash_table_lookup(active_fields, key));
}

int field_is_cumulative(const char *key) {
	if(!cummulative_fields) {
		init_field_cumulative();
	}

	return !!(hash_table_lookup(cummulative_fields, key));
}



char *sanitize_path_name(const char *name)
{
	char *new = xxstrdup(name);

	char *next = new;

	while( (next = strpbrk(next, " /.\n")) )
		*next = '_';

	return new;
}

char *get_rule_number(const char *filename)
{
	char  name[MAX_LINE];
	const char *base =  path_basename(filename);

	sscanf(base, RULE_PREFIX "%6c" RULE_SUFFIX, name);
	return xxstrdup(name);
}

void parse_fields_options(const char *field_str)
{
	if(!active_fields) {
		init_field_active();
	}
	hash_table_clear(active_fields);

	char *fields = xxstrdup(field_str);

	char *token = strtok(fields, ",");
	while(token) {
		if(strcmp(token, "wall_time") == 0) {
			hash_table_insert(active_fields, "wall_time", active_fields);
			debug(D_RMON, "adding field: wall time\n");
		} else if(strcmp(token, "max_concurrent_processes") == 0) {
			hash_table_insert(active_fields, "max_concurrent_processes", active_fields);
			debug(D_RMON, "adding field: concurrent processes\n");
		} else if(strcmp(token, "total_processes") == 0) {
			hash_table_insert(active_fields, "total_processes", active_fields);
			debug(D_RMON, "adding field: total processes\n");
		} else if(strcmp(token, "cpu_time") == 0) {
			hash_table_insert(active_fields, "cpu_time", active_fields);
			debug(D_RMON, "adding field: cpu time\n");
		} else if(strcmp(token, "virtual_memory") == 0) {
			hash_table_insert(active_fields, "virtual_memory", active_fields);
			debug(D_RMON, "adding field: virtual memory\n");
		} else if(strcmp(token, "memory") == 0) {
			hash_table_insert(active_fields, "memory", active_fields);
			debug(D_RMON, "adding field: resident memory\n");
		} else if(strcmp(token, "swap_memory") == 0) {
			hash_table_insert(active_fields, "swap_memory", active_fields);
			debug(D_RMON, "adding field: swap memory\n");
		} else if(strcmp(token, "bytes_read") == 0) {
			hash_table_insert(active_fields, "bytes_read", active_fields);
			debug(D_RMON, "adding field: bytes read\n");
		} else if(strcmp(token, "bytes_written") == 0) {
			hash_table_insert(active_fields, "bytes_written", active_fields);
			debug(D_RMON, "adding field: bytes written\n");
		} else if(strcmp(token, "bytes_received") == 0) {
			hash_table_insert(active_fields, "bytes_received", active_fields);
			debug(D_RMON, "adding field: bytes received\n");
		} else if(strcmp(token, "bytes_sent") == 0) {
			hash_table_insert(active_fields, "bytes_sent", active_fields);
			debug(D_RMON, "adding field: bytes sent\n");
		} else if(strcmp(token, "total_files") == 0) {
			hash_table_insert(active_fields, "total_files", active_fields);
			debug(D_RMON, "adding field: number of files\n");
		} else if(strcmp(token, "disk") == 0) {
			hash_table_insert(active_fields, "disk", active_fields);
			debug(D_RMON, "adding field: disk\n");
		} else if(strcmp(token, "cores") == 0) {
			hash_table_insert(active_fields, "cores", active_fields);
			debug(D_RMON, "adding field: cores\n");
		} else if(strcmp(token, "cores_avg") == 0) {
			hash_table_insert(active_fields, "cores_avg", active_fields);
			debug(D_RMON, "adding field: cores_avg\n");
		} else {
			fatal("'%s' is not a field option\n", token);
		}

		token = strtok(NULL, ",");
	}

	free(fields);
}

struct rmsummary *parse_summary(struct jx_parser *p, char *filename, struct hash_table *categories)
{
	static struct jx_parser *last_p = NULL;
	static int summ_id     = 1;

	if(last_p != p)
	{
		last_p  = p;
		summ_id = 1;
	}
	else
	{
		summ_id++;
	}

	struct jx *j = jx_parser_yield(p);

	if(!j)
		return NULL;

	struct rmsummary *so = json_to_rmsummary(j);
	jx_delete(j);

	if(!so)
		return NULL;

	if(!so->taskid) {
		so->taskid = get_rule_number(filename);
	}

	if(!so->category) {
		if(so->command) {
			so->category   = parse_executable_name(so->command);
		} else {
			so->category   = xxstrdup(DEFAULT_CATEGORY);
			so->command    = xxstrdup(DEFAULT_CATEGORY);
		}
	}

	struct category *c = category_lookup_or_create(categories, ALL_SUMMARIES_CATEGORY);
	category_accumulate_summary(c, so, NULL);

	return so;
}

void parse_summary_from_filelist(struct rmsummary_set *dest, char *filename, struct hash_table *categories)
{
	FILE *flist;

	if(strcmp(filename, "-") == 0)
	{
		flist = stdin;
	}
	else
	{
		flist = fopen(filename, "r");
		if(!flist)
			fatal("Cannot open resources summary list: %s : %s\n", filename, strerror(errno));
	}

	struct rmsummary *s;
	char   file_summ[MAX_LINE];
	while((fgets(file_summ, MAX_LINE, flist)))
	{
		FILE *stream;

		int n = strlen(file_summ);
		if(n < 1)
			continue;

		if(file_summ[n - 1] == '\n')
		{
			file_summ[n - 1] = '\0';
		}

		stream = fopen(file_summ, "r");
		if(!stream)
			fatal("Cannot open resources summary file: %s : %s\n", file_summ, strerror(errno));

		struct jx_parser *p = jx_parser_create(0);
		jx_parser_read_stream(p, stream);

		while((s = parse_summary(p, file_summ, categories)))
			list_push_tail(dest->summaries, s);

		jx_parser_delete(p);
		fclose(stream);
	}
}

char *parse_executable_name(char *command)
{
	command = string_trim_spaces(command);

	char *space = strchr(command, ' ');

	if(space)
		*space = '\0';

	char *executable = command;

	if(space)
		*space = ' ';

	return executable;
}

struct rmsummary_set *make_new_set(char *category)
{
	struct rmsummary_set *ss = malloc(sizeof(struct rmsummary_set));

	ss->category_name = category;
	ss->stats         = hash_table_create(0,0);
	ss->summaries     = list_create();

	return ss;
}

/* vim: set noexpandtab tabstop=4: */
