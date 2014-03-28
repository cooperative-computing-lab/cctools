#include "rmon_tools.h"

struct field fields[NUM_FIELDS + 1] = {
	[WALL_TIME] = {"t", "wall time",      "s",      1, offsetof(struct rmDsummary, wall_time)},
	[CPU_TIME]  = {"c", "cpu time",        "s",     1, offsetof(struct rmDsummary, cpu_time)},
	[VIRTUAL  ] = {"v", "virtual memory",  "MB",    1, offsetof(struct rmDsummary, virtual_memory)},
	[RESIDENT ] = {"m", "resident memory", "MB",    1, offsetof(struct rmDsummary, resident_memory)},
	[SWAP     ] = {"s", "swap memory",     "MB",    1, offsetof(struct rmDsummary, swap_memory)},
	[B_READ   ] = {"r", "read bytes",      "MB",    1, offsetof(struct rmDsummary, bytes_read)},
	[B_WRITTEN] = {"w", "written bytes",   "MB",    1, offsetof(struct rmDsummary, bytes_written)},
	[FILES    ] = {"n", "num files",       "files", 1, offsetof(struct rmDsummary, workdir_num_files)},
	[FOOTPRINT] = {"z", "footprint",       "MB",    1, offsetof(struct rmDsummary, workdir_footprint)},
	[CORES    ] = {"C", "cores",           "cores", 0, offsetof(struct rmDsummary, cores)},
	[MAX_PROCESSES]   = {"p", "max processes",   "procs", 0, offsetof(struct rmDsummary, max_concurrent_processes)},
	[TOTAL_PROCESSES] = {"P", "total processes", "procs", 0, offsetof(struct rmDsummary, total_processes)},
	[NUM_FIELDS] = {NULL, NULL, NULL, 0, 0}
};

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
	struct buffer b;
	buffer_init(&b);

	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
		if(f->active)
			buffer_printf(&b, "%s: %s%s",  f->abbrev, f->name, separator);

	char *str = xxstrdup(buffer_tostring(&b, NULL));
	buffer_free(&b);

	return str;
}

int get_rule_number(char *filename)
{
	char  name[MAX_LINE];
	const char *base =  path_basename(filename);

	sscanf(base, RULE_PREFIX "%6c" RULE_SUFFIX, name);
	return atoi(name);
}

void parse_fields_options(char *field_str)
{
	struct field *f;
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
		f->active = 0;

	char *c =  field_str;
	while( *c != '\0' )
	{
		switch(*c)
		{
			case 't':
				fields[WALL_TIME].active = 1;
				debug(D_DEBUG, "adding clustering field: wall time\n");
				break;
			case 'p':
				fields[MAX_PROCESSES].active = 1;
				debug(D_DEBUG, "adding clustering field: concurrent processes\n");
				break;
			case 'P':
				fields[TOTAL_PROCESSES].active = 1;
				debug(D_DEBUG, "adding clustering field: total processes\n");
				break;
			case 'c':
				fields[CPU_TIME].active = 1;
				debug(D_DEBUG, "adding clustering field: cpu time\n");
				break;
			case 'v':
				fields[VIRTUAL].active = 1;
				debug(D_DEBUG, "adding clustering field: virtual memory\n");
				break;
			case 'm':
				fields[RESIDENT].active = 1;
				debug(D_DEBUG, "adding clustering field: resident memory\n");
				break;
			case 's':
				fields[SWAP].active = 1;
				debug(D_DEBUG, "adding clustering field: swap memory\n");
				break;
			case 'r':
				fields[B_READ].active = 1;
				debug(D_DEBUG, "adding clustering field: bytes read\n");
				break;
			case 'w':
				fields[B_WRITTEN].active = 1;
				debug(D_DEBUG, "adding clustering field: bytes written\n");
				break;
			case 'n':
				fields[FILES].active = 1;
				debug(D_DEBUG, "adding clustering field: number of files\n");
				break;
			case 'z':
				fields[FOOTPRINT].active = 1;
				debug(D_DEBUG, "adding clustering field: footprint\n");
				break;
			case 'C':
				fields[CORES].active = 1;
				debug(D_DEBUG, "adding clustering field: cores\n");
				break;
			default:
				fatal("'%c' is not a field option\n", c);
				break;
		}
		c++;
	}
}

struct rmDsummary *parse_summary_file(char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");
	if(!stream)
		fatal("Cannot open resources summary file: %s : %s\n", filename, strerror(errno));

	struct rmDsummary *s = parse_summary(stream, filename);

	fclose(stream);

	return s;
}

struct rmDsummary *parse_summary(FILE *stream, char *filename)
{
	static FILE *last_stream = NULL;
	static int   summ_id     = 1;

	if(last_stream != stream)
	{
		last_stream = stream;
		summ_id     = 1;
	}
	else
	{
		summ_id++;
	}

	struct rmsummary  *so = rmsummary_parse_next(stream);

	if(!so)
		return NULL;

	struct rmDsummary *s  = malloc(sizeof(struct rmDsummary));

	s->command    = so->command;

	s->file       = xxstrdup(filename);

	if(so->category)
	{
		s->category   = so->category;
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

	s->start     = usecs_to_secs(so->start);
	s->end       = usecs_to_secs(so->end);
	s->wall_time = usecs_to_secs(so->wall_time);
	s->cpu_time  = usecs_to_secs(so->cpu_time);

	s->cores = so->cores;
	s->total_processes = so->total_processes;
	s->max_concurrent_processes = so->max_concurrent_processes;

	s->virtual_memory = so->virtual_memory; 
	s->resident_memory = so->resident_memory; 
	s->swap_memory = so->swap_memory; 

	s->bytes_read    = bytes_to_Mbytes(so->bytes_read);
	s->bytes_written = bytes_to_Mbytes(so->bytes_written);

	s->workdir_num_files = so->workdir_num_files;
	s->workdir_footprint = so->workdir_footprint;

	s->task_id = so->task_id;
	if(s->task_id < 0)
	{
		s->task_id = get_rule_number(filename);
	}

	struct field *f;	
	for(f = &fields[WALL_TIME]; f->name != NULL; f++)
	{
		if(value_of_field(s, f) < 0)
		{
			assign_to_field(s, f, 0);
		}
	}

	free(so); //we do not free so->command on purpouse.

	return s;
}

void parse_summary_from_filelist(struct rmDsummary_set *dest, char *filename)
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
		if(file_summ[n - 1] == '\n')
		{
			file_summ[n - 1] = '\0';
		}

		stream = fopen(file_summ, "r");
		if(!stream)
			fatal("Cannot open resources summary file: %s : %s\n", file_summ, strerror(errno));

		while((s = parse_summary(stream, file_summ)))
			list_push_tail(dest->summaries, s);

		fclose(stream);
	}
}


void parse_summary_recursive(struct rmDsummary_set *dest, char *dirname)
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

			while((s = parse_summary(stream, entry->fts_accpath)))
				list_push_tail(dest->summaries, s);

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

