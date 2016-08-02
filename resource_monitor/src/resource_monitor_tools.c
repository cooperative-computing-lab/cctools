/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "resource_monitor_tools.h"
#include "macros.h"
#include "category.h"

#include "jx_parse.h"

double usecs_to_secs(double usecs)
{
	return usecs/1000000;
}

double secs_to_usecs(double secs)
{
	return secs*1000000;
}

double Mbytes_to_bytes(double Mbytes)
{
	return Mbytes*1e6;
}

double bytes_to_Mbytes(double bytes)
{
	return bytes/1e6;
}

double bytes_to_Gbytes(double bytes)
{
	return bytes/1e9;
}

double Mbytes_to_Gbytes(double bytes)
{
	return bytes/1e3;
}

struct field fields[NUM_FIELDS + 1] = {
	[WALL_TIME]  = {"t", "wall_time",      "wall time",       "s",        PRId64, 1, 1, offsetof(struct rmsummary, wall_time)},
	[CPU_TIME]   = {"c", "cpu_time",       "cpu time",        "s",        PRId64, 1, 1, offsetof(struct rmsummary, cpu_time)},
	[VIRTUAL  ]  = {"v", "virtual_memory", "virtual memory",  "MB",       PRId64, 0, 1, offsetof(struct rmsummary, virtual_memory)},
	[RESIDENT ]  = {"m", "memory",         "resident memory", "MB",       PRId64, 0, 1, offsetof(struct rmsummary, memory)},
	[SWAP     ]  = {"s", "swap_memory",    "swap memory",     "MB",       PRId64, 0, 1, offsetof(struct rmsummary, swap_memory)},
	[B_READ   ]  = {"r", "bytes_read",     "read bytes",      "MB",       PRId64, 0, 1, offsetof(struct rmsummary, bytes_read)},
	[B_WRITTEN]  = {"w", "bytes_written",  "written bytes",   "MB",       PRId64, 0, 1, offsetof(struct rmsummary, bytes_written)},
	[B_RX   ]    = {"R", "bytes_received", "received bytes",  "MB",       PRId64, 0, 1, offsetof(struct rmsummary, bytes_received)},
	[B_TX]       = {"W", "bytes_sent",     "bytes_sent",      "MB",       PRId64, 0, 1, offsetof(struct rmsummary, bytes_sent)},
	[BANDWIDTH]  = {"B", "bandwidth",      "bandwidth",       "Mbps",     PRId64, 0, 1, offsetof(struct rmsummary, bandwidth)},
	[FILES    ]  = {"f", "total_files",    "num files",       "files",    PRId64, 0, 1, offsetof(struct rmsummary, total_files)},
	[DISK]       = {"z", "disk",           "disk",            "MB",       PRId64, 0, 1, offsetof(struct rmsummary, disk)},
	[CORES_AVG]  = {"C", "cores_avg",      "cores avg",       "cores",    ".2f",    0, 1, offsetof(struct rmsummary, cores_avg)},
	[CORES_PEAK] = {"P", "cores",          "cores peak",      "cores",    PRId64,    0, 1, offsetof(struct rmsummary, cores)},
	[MAX_PROCESSES]   = {"N", "max_concurrent_processes", "max processes",   "procs", PRId64, 0, 0, offsetof(struct rmsummary, max_concurrent_processes)},
	[TOTAL_PROCESSES] = {"n", "total_processes",          "total processes", "procs", PRId64, 0, 0, offsetof(struct rmsummary, total_processes)},
	[NUM_FIELDS] = {NULL, NULL, NULL, NULL, NULL, 0, 0, 0}
};

char *sanitize_path_name(char *name)
{
	char *new = xxstrdup(name);

	char *next = new;

	while( (next = strpbrk(next, " /.\n")) )
		*next = '_';

	return new;
}

struct rmDsummary *summary_bin_op(struct rmDsummary *s, struct rmDsummary *a, struct rmDsummary *b, double (*op)(double, double))
{
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(f->active)
		{
			assign_to_field(s, f, op(value_of_field(a, f), value_of_field(b, f)));
		}
	}

	return s;
}

struct rmDsummary *summary_unit_op(struct rmDsummary *s, struct rmDsummary *a, double u, double (*op)(double, double))
{
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(f->active)
		{
			assign_to_field(s, f, op(value_of_field(a, f), u));
		}
	}

	return s;
}

double plus(double a, double b)
{
	return a + b;
}

double minus(double a, double b)
{
	return a - b;
}

double mult(double a, double b)
{
	return a * b;
}

double minus_squared(double a, double b)
{
	return pow(a - b, 2);
}

double divide(double a, double b)
{
	return a/b;
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
			case 'N':
				fields[MAX_PROCESSES].active = 1;
				debug(D_RMON, "adding field: concurrent processes\n");
				break;
			case 'n':
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
			case 'f':
				fields[FILES].active = 1;
				debug(D_RMON, "adding field: number of files\n");
				break;
			case 'z':
				fields[DISK].active = 1;
				debug(D_RMON, "adding field: footprint\n");
				break;
			case 'P':
				fields[CORES_PEAK].active = 1;
				debug(D_RMON, "adding field: cores peak\n");
				break;
			case 'C':
				fields[CORES_AVG].active = 1;
				debug(D_RMON, "adding field: cores avg\n");
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

	if(categories) {
		if(so->category) {
			c = category_lookup_or_create(categories, so->category);
			category_accumulate_summary(c, so, NULL);
		}
	}

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

	ss->category  = category;

	ss->histograms = itable_create(0);

	ss->summaries = list_create();

	return ss;
}

char *field_str(struct field *f, double value) {
	char control_str[128];
	snprintf(control_str, sizeof(control_str) - 1, "%%%s", f->format);

	if(strcmp(f->format, PRId64) != 0) {
		return string_format(control_str, value);
	} else {
		return string_format(control_str, (int64_t) value);
	}
}


/* vim: set noexpandtab tabstop=4: */
