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
	[WALL_TIME]  = {"t", "wall_time",      "wall time",       "s",        PRId64, 1, 1, offsetof(struct rmDsummary, wall_time)},
	[CPU_TIME]   = {"c", "cpu_time",       "cpu time",        "s",        PRId64, 1, 1, offsetof(struct rmDsummary, cpu_time)},
	[VIRTUAL  ]  = {"v", "virtual_memory", "virtual memory",  "MB",       PRId64, 0, 1, offsetof(struct rmDsummary, virtual_memory)},
	[RESIDENT ]  = {"m", "memory",         "resident memory", "MB",       PRId64, 0, 1, offsetof(struct rmDsummary, memory)},
	[SWAP     ]  = {"s", "swap_memory",    "swap memory",     "MB",       PRId64, 0, 1, offsetof(struct rmDsummary, swap_memory)},
	[B_READ   ]  = {"r", "bytes_read",     "read bytes",      "MB",       PRId64, 0, 1, offsetof(struct rmDsummary, bytes_read)},
	[B_WRITTEN]  = {"w", "bytes_written",  "written bytes",   "MB",       PRId64, 0, 1, offsetof(struct rmDsummary, bytes_written)},
	[B_RX   ]    = {"R", "bytes_received", "received bytes",  "MB",       PRId64, 0, 1, offsetof(struct rmDsummary, bytes_received)},
	[B_TX]       = {"W", "bytes_sent",     "bytes_sent",      "MB",       PRId64, 0, 1, offsetof(struct rmDsummary, bytes_sent)},
	[BANDWIDTH]  = {"B", "bandwidth",      "bandwidth",       "Mbps",     PRId64, 0, 1, offsetof(struct rmDsummary, bandwidth)},
	[FILES    ]  = {"f", "total_files",    "num files",       "files",    PRId64, 0, 1, offsetof(struct rmDsummary, total_files)},
	[DISK]       = {"z", "disk",           "disk",            "MB",       PRId64, 0, 1, offsetof(struct rmDsummary, disk)},
	[CORES_AVG]  = {"C", "cores_avg",      "cores avg",       "cores",    ".2f",    0, 1, offsetof(struct rmDsummary, cores_avg)},
	[CORES_PEAK] = {"P", "cores",          "cores peak",      "cores",    PRId64,    0, 1, offsetof(struct rmDsummary, cores)},
	[MAX_PROCESSES]   = {"N", "max_concurrent_processes", "max processes",   "procs", PRId64, 0, 0, offsetof(struct rmDsummary, max_concurrent_processes)},
	[TOTAL_PROCESSES] = {"n", "total_processes",          "total processes", "procs", PRId64, 0, 0, offsetof(struct rmDsummary, total_processes)},
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

struct rmDsummary *rmsummary_to_rmDsummary(struct rmsummary *so) {

	struct rmDsummary *s = malloc(sizeof(struct rmDsummary));
	bzero(s, sizeof(*s));

	if(so->command) {
		s->command    = xxstrdup(so->command);
	}

	if(so->category)
	{
		s->category   = xxstrdup(so->category);
	}
	else if(so->command)
	{
		s->category   = parse_executable_name(so->command);
	}
	else
	{
		s->category   = xxstrdup(DEFAULT_CATEGORY);
		s->command    = xxstrdup(DEFAULT_CATEGORY);
	}

	if(so->task_id)
	{
		s->task_id = xxstrdup(so->task_id);
	}

	to_external(s, so, start);
	to_external(s, so, end);
	to_external(s, so, wall_time);
	to_external(s, so, cpu_time);
	to_external(s, so, cores);

	// use fractional cores if possible
	if(s->wall_time > 0 && s->cpu_time > 0) {
		s->cores_avg = s->cpu_time/s->wall_time;
	} else {
		s->cores_avg = so->cores;
	}

	to_external(s, so, total_processes);
	to_external(s, so, max_concurrent_processes);

	to_external(s, so, memory);
	to_external(s, so, virtual_memory);
	to_external(s, so, swap_memory);

	to_external(s, so, bytes_read);
	to_external(s, so, bytes_written);

	to_external(s, so, bytes_received);
	to_external(s, so, bytes_sent);
	to_external(s, so, bandwidth);

	to_external(s, so, disk);
	to_external(s, so, total_files);

	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		// if a value is negative, set it to zero
		if(value_of_field(s, f) < 0)
		{
			assign_to_field(s, f, 0);
		}
	}

	return s;
}


struct rmDsummary *parse_summary(struct jx_parser *p, char *filename, struct hash_table *categories)
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

	struct category *c = category_lookup_or_create(categories, ALL_SUMMARIES_CATEGORY);
	category_accumulate_summary(c, so, NULL);

	if(categories) {
		if(so->category) {
			c = category_lookup_or_create(categories, so->category);
			category_accumulate_summary(c, so, NULL);
		}
	}

	struct rmDsummary *s  = rmsummary_to_rmDsummary(so);

	s->file = xxstrdup(filename);
	if(!s->task_id) {
		s->task_id = get_rule_number(filename);
	}

	rmsummary_delete(so);

	return s;
}

void parse_summary_from_filelist(struct rmDsummary_set *dest, char *filename, struct hash_table *categories)
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

	struct rmDsummary *s;
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


void parse_summary_recursive(struct rmDsummary_set *dest, char *dirname, struct hash_table *categories)
{

	FTS *hierarchy;
	FTSENT *entry;
	char *argv[] = {dirname, NULL};

	hierarchy = fts_open(argv, FTS_PHYSICAL, NULL);

	if(!hierarchy)
		fatal("fts_open error: %s\n", strerror(errno));

	struct rmDsummary *s;
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

struct rmDsummary_set *make_new_set(char *category)
{
	struct rmDsummary_set *ss = malloc(sizeof(struct rmDsummary_set));

	ss->category  = category;

	ss->histograms = itable_create(0);

	ss->summaries = list_create();

	return ss;
}

#define to_internal(so, s, f, u) rmsummary_to_internal_unit(#f, (so)->f, &(s->f), u)

void rmDsummary_print(FILE *output, struct rmDsummary *so) {
	struct rmsummary *s = rmsummary_create(-1);

	s->command    = xxstrdup(so->command);

	if(so->category)
	{
		s->category   = xxstrdup(so->category);
	}
	else if(so->command)
	{
		s->category   = xxstrdup(so->command);
	}
	else
	{
		s->category   = xxstrdup(DEFAULT_CATEGORY);
		s->command    = xxstrdup(DEFAULT_CATEGORY);
	}

	if(so->task_id) {
		s->task_id = xxstrdup(so->task_id);
	}

	s->start     = so->start;
	s->end       = so->end;
	s->wall_time = so->wall_time;

	to_internal(so, s, start,     "us");
	to_internal(so, s, end,       "us");
	to_internal(so, s, wall_time, "s");
	to_internal(so, s, cpu_time,  "s");

	s->cores = so->cores;

	to_internal(so, s, total_processes,         "procs");
	to_internal(so, s, max_concurrent_processes,"procs");

	to_internal(so, s, memory,         "MB");
	to_internal(so, s, virtual_memory, "MB");
	to_internal(so, s, swap_memory,    "MB");

	to_internal(so, s, bytes_read,    "MB");
	to_internal(so, s, bytes_written, "MB");

	to_internal(so, s, bytes_received, "MB");
	to_internal(so, s, bytes_sent,     "MB");
	to_internal(so, s, bandwidth,      "Mbps");

	to_internal(so, s, total_files, "files");
	to_internal(so, s, disk, "MB");

	rmsummary_print(output, s, /* pprint */ 1, /* extra fields */ 0);
	rmsummary_delete(s);

	return;
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
