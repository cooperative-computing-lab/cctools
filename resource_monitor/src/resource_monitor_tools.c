/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "resource_monitor_tools.h"
#include "macros.h"
#include "category.h"

#include "jx_parse.h"

struct field fields[NUM_FIELDS + 1] = {
	[WALL_TIME] = {"t", "wall_time",      "wall time",       "s",        1, 1, offsetof(struct rmsummary, wall_time)},
	[CPU_TIME]  = {"c", "cpu_time",       "cpu time",        "s",        1, 1, offsetof(struct rmsummary, cpu_time)},
	[VIRTUAL  ] = {"v", "virtual_memory", "virtual memory",  "MB",       0, 1, offsetof(struct rmsummary, virtual_memory)},
	[RESIDENT ] = {"m", "memory",         "resident memory", "MB",       0, 1, offsetof(struct rmsummary, memory)},
	[SWAP     ] = {"s", "swap_memory",    "swap memory",     "MB",       0, 1, offsetof(struct rmsummary, swap_memory)},
	[B_READ   ] = {"r", "bytes_read",     "read bytes",      "MB",       0, 1, offsetof(struct rmsummary, bytes_read)},
	[B_WRITTEN] = {"w", "bytes_written",  "written bytes",   "MB",       0, 1, offsetof(struct rmsummary, bytes_written)},
	[B_RX   ]   = {"R", "bytes_received", "received bytes",  "MB",       0, 1, offsetof(struct rmsummary, bytes_received)},
	[B_TX]      = {"W", "bytes_sent",     "bytes_sent",      "MB",       0, 1, offsetof(struct rmsummary, bytes_sent)},
	[BANDWIDTH] = {"B", "bandwidth",      "bandwidth",       "Mbps",     0, 1, offsetof(struct rmsummary, bandwidth)},
	[FILES    ] = {"n", "total_files",    "num files",       "files",    0, 1, offsetof(struct rmsummary, total_files)},
	[DISK]      = {"z", "disk",           "disk",            "MB",       0, 1, offsetof(struct rmsummary, disk)},
	[CORES    ] = {"C", "cores",          "cores",           "cores",    0, 1, offsetof(struct rmsummary, cores)},
	[MAX_PROCESSES]   = {"p", "max_concurrent_processes", "max processes",   "procs", 0, 0, offsetof(struct rmsummary, max_concurrent_processes)},
	[TOTAL_PROCESSES] = {"P", "total_processes",          "total processes", "procs", 0, 0, offsetof(struct rmsummary, total_processes)},
	[NUM_FIELDS] = {NULL, NULL, NULL, NULL, 0, 0, 0}
};

char *sanitize_path_name(char *name)
{
	char *new = xxstrdup(name);

	char *next = new;

	while( (next = strpbrk(next, " /.\n")) )
		*next = '_';

	return new;
}

char *make_field_names_str(char *separator)
{
	char *str;
	struct buffer b;
	buffer_init(&b);

	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
		if(f->active)
			buffer_printf(&b, "%s: %s%s",  f->abbrev, f->name, separator);

	buffer_dup(&b, &str);
	buffer_free(&b);

	return str;
}

char *get_rule_number(char *filename)
{
	char  name[MAX_LINE];
	const char *base =  path_basename(filename);

	sscanf(base, RULE_PREFIX "%6c" RULE_SUFFIX, name);
	return xxstrdup(name);
}

void parse_fields_options(char *field_str)
{
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
		f->active = 0;

	char *c = field_str;
	while( *c != '\0' )
	{
		switch(*c)
		{
			case 't':
				fields[WALL_TIME].active = 1;
				debug(D_RMON, "adding field: wall time\n");
				break;
			case 'p':
				fields[MAX_PROCESSES].active = 1;
				debug(D_RMON, "adding field: concurrent processes\n");
				break;
			case 'P':
				fields[TOTAL_PROCESSES].active = 1;
				debug(D_RMON, "adding field: total processes\n");
				break;
			case 'c':
				fields[CPU_TIME].active = 1;
				debug(D_RMON, "adding field: cpu time\n");
				break;
			case 'v':
				fields[VIRTUAL].active = 1;
				debug(D_RMON, "adding field: virtual memory\n");
				break;
			case 'm':
				fields[RESIDENT].active = 1;
				debug(D_RMON, "adding field: resident memory\n");
				break;
			case 's':
				fields[SWAP].active = 1;
				debug(D_RMON, "adding field: swap memory\n");
				break;
			case 'r':
				fields[B_READ].active = 1;
				debug(D_RMON, "adding field: bytes read\n");
				break;
			case 'w':
				fields[B_WRITTEN].active = 1;
				debug(D_RMON, "adding field: bytes written\n");
				break;
			case 'R':
				fields[B_RX].active = 1;
				debug(D_RMON, "adding field: bytes received\n");
				break;
			case 'W':
				fields[B_TX].active = 1;
				debug(D_RMON, "adding field: bytes sent\n");
				break;
			case 'n':
				fields[FILES].active = 1;
				debug(D_RMON, "adding field: number of files\n");
				break;
			case 'z':
				fields[DISK].active = 1;
				debug(D_RMON, "adding field: footprint\n");
				break;
			case 'C':
				fields[CORES].active = 1;
				debug(D_RMON, "adding field: cores\n");
				break;
			default:
				fatal("'%c' is not a field option\n", *c);
				break;
		}
		c++;
	}
}

#define to_external(s, so, f) (s)->f = rmsummary_to_external_unit(#f, (so)->f)

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

	if(!so->task_id) {
		so->task_id = get_rule_number(filename);
	}

	if(!so->category) {
		if(so->command) {
			so->category   = parse_executable_name(so->command);
		} else {
			so->category   = xxstrdup(DEFAULT_CATEGORY);
			so->command    = xxstrdup(DEFAULT_CATEGORY);
		}
	}

	// if a value is negative, set it to zero
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(value_of_field(so, f) < 0)
		{
			assign_to_field(so, f, 0);
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


void parse_summary_recursive(struct rmsummary_set *dest, char *dirname, struct hash_table *categories)
{

	FTS *hierarchy;
	FTSENT *entry;
	char *argv[] = {dirname, NULL};

	hierarchy = fts_open(argv, FTS_PHYSICAL, NULL);

	if(!hierarchy)
		fatal("fts_open error: %s\n", strerror(errno));

	struct rmsummary *s;
	while( (entry = fts_read(hierarchy)) )
		if( S_ISREG(entry->fts_statp->st_mode) && strstr(entry->fts_name, RULE_SUFFIX) ) //bug: no links
		{
			FILE *stream;
			stream = fopen(entry->fts_accpath, "r");
			if(!stream)
				fatal("Cannot open resources summary file: %s : %s\n", entry->fts_accpath, strerror(errno));

			struct jx_parser *p = jx_parser_create(0);
			jx_parser_read_stream(p, stream);

			while((s = parse_summary(p, entry->fts_path, categories)))
				list_push_tail(dest->summaries, s);

			jx_parser_delete(p);
			fclose(stream);
		}

	fts_close(hierarchy);
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
	ss->stats         = itable_create(0);
	ss->summaries     = list_create();

	return ss;
}

/* vim: set noexpandtab tabstop=4: */
