
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdarg.h>
#include <ctype.h>

#include "getopt.h"
#include "cctools.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "stringtools.h"
#include "b64.h"

#include "deltadb_query.h"
#include "deltadb_stream.h"
#include "deltadb_reduction.h"

int suffix_to_multiplier( char suffix )
{
	switch(tolower(suffix)) {
	case 'y': return 60*60*24*365;
	case 'w': return 60*60*24*7;
	case 'd': return 60*60*24;
	case 'h': return 60*60;
	case 'm': return 60;
	default: return 1;
	}
}

time_t parse_time( const char *str, time_t current )
{
	struct tm t;
	int count;
	char suffix[2];
	int n;

	memset(&t,0,sizeof(t));

	if(!strcmp(str,"now")) {
		return current;
	}

	n = sscanf(str, "%d%[yYdDhHmMsS]", &count, suffix);
	if(n==2) {
		return current - count*suffix_to_multiplier(suffix[0]);
	}

	n = sscanf(str, "%d-%d-%d %d:%d:%d", &t.tm_year,&t.tm_mon,&t.tm_mday,&t.tm_hour,&t.tm_min,&t.tm_sec);
	if(n==6) {
		if (t.tm_hour>23)
			t.tm_hour = 0;
		if (t.tm_min>23)
			t.tm_min = 0;
		if (t.tm_sec>23)
			t.tm_sec = 0;

		t.tm_year -= 1900;
		t.tm_mon -= 1;

		return mktime(&t);
	}

	n = sscanf(str, "%d-%d-%d", &t.tm_year,&t.tm_mon,&t.tm_mday);
	if(n==3) {
		t.tm_year -= 1900;
		t.tm_mon -= 1;

		return mktime(&t);
	}

	return 0;
}

static struct option long_options[] =
{
	{"db", required_argument, 0, 'D'},
	{"file", required_argument, 0, 'L'},
	{"catalog", required_argument, 0, 'c'},
	{"output", required_argument, 0, 'o'},
	{"where", required_argument, 0,'w'},
	{"filter", required_argument, 0,'f'},
	{"at",required_argument,0,'a'},
	{"from", required_argument, 0, 'F'},
	{"to", required_argument, 0, 'T'},
	{"every", required_argument, 0, 'e'},
	{"json", no_argument, 0, 'j' },
	{"epoch", no_argument, 0, 't'},
	{"version", no_argument, 0, 'v'},
	{"help", no_argument, 0, 'h'},
	{0,0,0,0}
};

void show_help()
{
	printf("use: deltadb_query [options]\n");
	printf("Where options are:\n");
	printf("  --db <path>         Query this database directory.\n");
	printf("  --file <path>       Query this raw data file.\n");
	printf("  --catalog <host>    Query this catalog server.\n");
	printf("  --output <expr>     Output this expression. (multiple)\n");
	printf("  --where <expr>      Only output records matching this expression.\n");
	printf("  --filter <expr>     Only process records matching this expression.\n");
	printf("  --at <time>         Query at this point in time.\n");
	printf("  --from <time>       Begin history query at this absolute time.\n");
	printf("  --to <time>         End history query at this absolute time.\n");
	printf("  --every <interval>  Compute output at this time interval.\n");
	printf("  --json              Output raw JSON objects.\n");
	printf("  --epoch             Display time column in Unix epoch format.\n");
	printf("  --version           Show software version.\n");
	printf("  --help              Show this help text.\n");
}

void time_error( const char *arg )
{
	fprintf(stderr,"deltadb_query: invalid %s time format (must be \"YYYY-MM-DD\" or \"YYYY-MM-DD HH:MM:SS\")\n",arg);
	exit(1);
}

int main( int argc, char *argv[] )
{
	const char *dbdir=0;
	const char *dbfile=0;
	const char *dbhost=0;

	struct jx *where_expr=0;
	struct jx *filter_expr=0;

	time_t start_time = 0;
	time_t stop_time = 0;
	int display_every = 0;
	int epoch_mode = 0;
	int nreduces = 0;
	int noutputs = 0;

	char reduce_name[1024];
	char reduce_attr[1024];

	time_t current = time(0);

	int c;

	struct deltadb_query *query = deltadb_query_create();
	deltadb_query_set_display(query,DELTADB_DISPLAY_STREAM);

	while((c=getopt_long(argc,argv,"D:L:o:w:f:F:T:e:tvh",long_options,0))!=-1) {
		switch(c) {
		case 'D':
			dbdir = optarg;
			break;
		case 'L':
			dbfile = optarg;
			break;
		case 'c':
			dbhost = optarg;
			break;

		case 'o':
			if(2==sscanf(optarg,"%[^(](%[^)])",reduce_name,reduce_attr)) {

				struct jx *reduce_expr = jx_parse_string(reduce_attr);
				if(!reduce_expr) {
					fprintf(stderr,"deltadb_query: invalid expression: %s\n",reduce_attr);
					return 1;
				}

				deltadb_scope_t scope;

				/* If the reduction name begins with GLOBAL, assign it global scope. */
				if(!strncmp(reduce_name,"GLOBAL_",7)) {
					scope = DELTADB_SCOPE_GLOBAL;
					strcpy(reduce_name,&reduce_name[7]);

				/* If the reduction name begins with TIME, assign it temporal scope. */
				} else if (!strncmp(reduce_name, "TIME_",5)) {
					scope = DELTADB_SCOPE_TEMPORAL;
					if (!jx_istype(reduce_expr, JX_SYMBOL)) {
						fprintf(stderr, "deltadb_query: must supply attribute name to temporal reduction: %s\n",reduce_attr);
						return 1;
					}
					strcpy(reduce_name,&reduce_name[5]);
				} else {
					scope = DELTADB_SCOPE_SPATIAL;
				}

				struct deltadb_reduction *r = deltadb_reduction_create(reduce_name,reduce_expr,scope);
				if(!r) {
					fprintf(stderr,"deltadb_query: invalid reduction: %s\n",reduce_name);
					return 1;
				}

				deltadb_query_add_reduction(query,r);
				deltadb_query_set_display(query,DELTADB_DISPLAY_REDUCE);
				nreduces++;
			} else {
				struct jx *j = jx_parse_string(optarg);
				if(!j) {
					fprintf(stderr,"invalid expression: %s\n",optarg);
					return 1;
				}
				deltadb_query_add_output(query,j);
				deltadb_query_set_display(query,DELTADB_DISPLAY_EXPRS);
				noutputs++;
			}
			break;
		case 'w':
			if(where_expr) {
				fprintf(stderr,"Only one --where expression is allowed.  Try joining the expressions with the && (and) operator.");
				return 1;
			}
			where_expr = jx_parse_string(optarg);
			if(!where_expr) {
				fprintf(stderr,"invalid expression: %s\n",optarg);
				return 1;
			}
			deltadb_query_set_where(query,where_expr);
			break;
		case 'j':
			deltadb_query_set_display(query,DELTADB_DISPLAY_OBJECTS);
			break;
		case 'f':
			if(filter_expr) {
				fprintf(stderr,"Only one --filter expression is allowed.  Try joining the expressions with the && (and) operator.");
				return 1;
			}
			filter_expr = jx_parse_string(optarg);
			if(!filter_expr) {
				fprintf(stderr,"invalid expression: %s\n",optarg);
				return 1;
			}
			deltadb_query_set_filter(query,filter_expr);
			break;
		case 'a':
			start_time = stop_time = parse_time(optarg,current);
			if(!start_time) time_error("--at");
			break;
		case 'F':
			start_time = parse_time(optarg,current);
			if(!start_time) time_error("--from");
			break;
		case 'T':
			stop_time = parse_time(optarg,current);
			if(!start_time) time_error("--to");
			break;
		case 'e':
			display_every = string_time_parse(optarg);
			deltadb_query_set_interval(query,display_every);
			break;
		case 't':
			epoch_mode = 1;
			deltadb_query_set_epoch_mode(query,epoch_mode);
			break;
		case 'v':
			cctools_version_print(stdout,"deltadb_query");
			break;
		case 'h':
			show_help();
			break;
		}
	}

	if(!dbdir && !dbfile && !dbhost) {
		fprintf(stderr,"deltadb_query: one of --db or --file or --catalog argument is required\n");
		return 1;
	}

	if(start_time==0) {
		fprintf(stderr,"deltadb_query: one of --at or --from option is required.\n");
		return 1;
	}

	if(stop_time==0) {
		stop_time = time(0);
	}

	if(nreduces>0 && noutputs>0) {
		fprintf(stderr,"deltadb_query: cannot mix reductions and plain outputs.\n");
		return 1;
	}

	if(dbfile) {
		FILE *file = fopen(dbfile,"r");
		if(!file) {
			fprintf(stderr,"deltadb_query: couldn't open %s: %s\n",dbfile,strerror(errno));
			return 1;
		}
		deltadb_query_execute_stream(query,file,start_time,stop_time);
		fclose(file);
	} else if(dbdir) {
		deltadb_query_execute_dir(query,dbdir,start_time,stop_time);
	} else if(dbhost) {

		if(!filter_expr) filter_expr = jx_boolean(1);

		buffer_t buf;
		buffer_init(&buf);
		char *filter_str = jx_print_string(filter_expr);
		b64_encode(filter_str,strlen(filter_str),&buf);
		char *cmd = string_format("curl -s http://%s:9097/updates/%ld/%ld/%s",dbhost,start_time,stop_time,buffer_tostring(&buf));
		buffer_free(&buf);
		free(filter_str);

		FILE *file = popen(cmd,"r");
		if(!file) {
			fprintf(stderr,"deltadb_query: couldn't execute '%s': %s\n",cmd,strerror(errno));
			return 1;
		}
		deltadb_query_execute_stream(query,file,start_time,stop_time);
		free(cmd);
		pclose(file);
	}

	deltadb_query_delete(query);

	return 0;
}
