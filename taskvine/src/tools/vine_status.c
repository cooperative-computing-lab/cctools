/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "taskvine.h"

#include "vine_protocol.h"

#include "cctools.h"
#include "debug.h"
#include "catalog_query.h"
#include "domain_name_cache.h"
#include "jx_table.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "link.h"
#include "getopt.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

typedef enum {
	FORMAT_TABLE,
	FORMAT_LONG
} format_t;

typedef enum {
	NO_QUERY,
	QUERY_QUEUE,
	QUERY_TASKS,
	QUERY_WORKERS,
	QUERY_ABLE_WORKERS,
	QUERY_MASTER_RESOURCES,
	QUERY_CAPACITIES
} query_t;

#define CATALOG_SIZE 50 //size of the array of jx pointers

static format_t format_mode = FORMAT_TABLE;
static query_t query_mode = NO_QUERY;
static int vine_status_timeout = 30;
static char *catalog_host = NULL;
int catalog_size = CATALOG_SIZE;
static struct jx **global_catalog = NULL; //pointer to an array of jx pointers
struct jx *jexpr = NULL;
static int columns = 80;
int manual_ssl_option = 0;

/* negative columns mean a minimum of abs(value), but the column may expand if
 * columns available. */

static struct jx_table queue_headers[] = {
{"project",       "PROJECT", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -18},
{"name",          "HOST",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -21},
{"port",          "PORT",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 5},
{"tasks_waiting", "WAITING", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
{"tasks_running", "RUNNING", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
{"tasks_complete","COMPLETE",JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 8},
{"workers",       "WORKERS", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 7},
{NULL,NULL,0,0,0}
};

static struct jx_table task_headers[] = {
{"task_id",       "ID",      JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 4},
{"state",        "STATE",   JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,  8},
{"priority",     "PRIORITY",JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 8},
{"host",         "HOST",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -10},
{"command",      "COMMAND", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -18},
{"cores",        "CORES",   JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
{"memory",       "MEMORY",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
{"disk",         "DISK",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
{"gpus",         "GPUS",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
{NULL,NULL,0,0,0}
};

static struct jx_table worker_headers[] = {
{"hostname",            "HOST",      JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-10},
{"address_port",        "ADDRESS",   JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-15},
{"total_tasks_complete","COMPLETED", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT,9},
{"total_tasks_running", "RUNNING",   JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,7},
{"cores_inuse",         "CORE_USE",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-6},
{"cores_total",         "CORE_ALL",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-6},
{"memory_inuse",        "MEM_USE",   JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-5},
{"memory_total",        "MEM_ALL",   JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-5},
{"disk_inuse",          "DISK_USE",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-6},
{"disk_total",          "DISK_ALL",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-6},
{"gpus_inuse",          "GPUS_USE",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-6},
{"gpus_total",          "GPUS_ALL",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,-6},
{NULL,NULL,0,0,0}
};

static struct jx_table workers_able_headers[] = {
{"category",      "CATEGORY",     JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT,  -12},
{"tasks_on_workers", "DISPATCHED",      JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 10},
{"tasks_waiting", "WAITING",      JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 10},
{"workers_able",  "FIT-WORKERS",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 12},
{"max_cores",     "MAX-CORES",    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 10},
{"max_memory",    "MAX-MEMORY",   JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 10},
{"max_disk",      "MAX-DISK",     JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 10},
{NULL,NULL,0,0,0}
};

static struct jx_table manager_resource_headers[] = {
{"project",			"MANAGER", 		JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -19},
{"cores_total",		"CORES",        JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 6},
{"cores_inuse",		"INUSE", 	    JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 6},
{"memory_total",	"MEM(GB)",     JX_TABLE_MODE_GIGABYTES, JX_TABLE_ALIGN_RIGHT, 8},
{"memory_inuse",	"INUSE", 	JX_TABLE_MODE_GIGABYTES, JX_TABLE_ALIGN_RIGHT, 8},
{"gpus_total",  	"GPUS",  	JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 9},
{"gpus_inuse",		"INUSE", 	JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_RIGHT, 9},
{NULL,NULL,0,0,0}
};

static struct jx_table capacity_headers[] = {
{"project",     "MANAGER", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 30},
{"capacity_tasks", "TASKS",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
{"capacity_cores", "CORES",  JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
{"capacity_memory","MEMORY", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 15},
{"capacity_disk",  "DISK",   JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 20},
{NULL,NULL,0,0,0}
};

static void show_help(const char *progname)
{
	fprintf(stdout, "usage: %s [manager] [port]\n", progname);
	fprintf(stdout, "If a manager and port are given, get data directly from that manager.\n");
	fprintf(stdout, "Otherwise, contact the catalog server for summary data.\n");
	fprintf(stdout, "Options:\n");
	fprintf(stdout, " %-30s Show queue summary statistics. (default)\n", "-Q,--statistics");
	fprintf(stdout, " %-30s Filter results of -Q for managers matching <name>\n", "-M,--project-name<name>");
	fprintf(stdout, " %-30s List workers connected to the given manager.\n", "-W,--workers");
	fprintf(stdout, " %-30s List tasks of the given manager.\n", "-T,--tasks");
	fprintf(stdout, " %-30s List categories of the given manager, size of\n", "-A,--able-workers");
	fprintf(stdout, " %-30s largest task, and workers that can run it.\n", "");
	fprintf(stdout, " %-30s Shows aggregated resources of all managers.\n", "-R,--resources");
	fprintf(stdout, " %-30s Shows resource capacities of all managers.\n", "   --capacity");
	fprintf(stdout, " %-30s Long text output.\n", "-l,--verbose");
	fprintf(stdout, " %-30s Set catalog server to <catalog>. Format: HOSTNAME:PORT\n", "-C,--catalog=<catalog>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug <flag>");
	fprintf(stdout, " %-30s Filter results by this expression.\n", "   --where=<expr>");
	fprintf(stdout, " %-30s RPC timeout (default is %ds).\n", "-t,--timeout=<time>", vine_status_timeout);
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr,\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s or :stdout)\n", "");
	fprintf(stdout, " %-30s Rotate debug file once it reaches this size.\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Use SSL when directly connecting to a manager.", "--ssl");
	fprintf(stdout, " %-30s Show vine_status version.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

enum {
	LONG_OPT_WHERE=1000,
	LONG_OPT_CAPACITY,
	LONG_OPT_USE_SSL
};

static void vine_status_parse_command_line_arguments(int argc, char *argv[], const char **manager_host, int *manager_port, const char **project_name)
{
	static const struct option long_options[] = {
		{"project-name", required_argument, 0, 'M'},
		{"statistics", no_argument, 0, 'Q'},
		{"workers", no_argument, 0, 'W'},
		{"able-workers", no_argument, 0, 'A'},
		{"tasks", no_argument, 0, 'T'},
		{"verbose", no_argument, 0, 'l'},
		{"resources", no_argument, 0, 'R'},
		{"capacity", no_argument, 0, LONG_OPT_CAPACITY},
		{"catalog", required_argument, 0, 'C'},
		{"debug", required_argument, 0, 'd'},
		{"timeout", required_argument, 0, 't'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"version", no_argument, 0, 'v'},
		{"help", no_argument, 0, 'h'},
		{"where", required_argument, 0, LONG_OPT_WHERE},
		{"ssl", no_argument, 0, LONG_OPT_USE_SSL},
		{0,0,0,0}};

	signed int c;
	int needs_explicit_manager = 0;

	while((c = getopt_long(argc, argv, "AM:QTWC:d:lo:O:Rt:vh", long_options, NULL)) > -1) {
		switch (c) {
		case 'C':
			catalog_host = strdup(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'M':
			*project_name = xxstrdup(optarg);
			break;
		case 'Q':
			if(query_mode != NO_QUERY)
				fatal("Options -A, -Q, -T, and -W, are mutually exclusive, and can be specified only once.");
			needs_explicit_manager = 0;
			query_mode = QUERY_QUEUE;
			break;
		case 'T':
			if(query_mode != NO_QUERY)
				fatal("Options -A, -Q, -T, and -W, are mutually exclusive, and can be specified only once.");
			needs_explicit_manager = 1;
			query_mode = QUERY_TASKS;
			break;
		case 'W':
			if(query_mode != NO_QUERY)
				fatal("Options -A, -Q, -T, and -W, are mutually exclusive, and can be specified only once.");
			needs_explicit_manager = 1;
			query_mode = QUERY_WORKERS;
			break;
		case 'A':
			if(query_mode != NO_QUERY)
				fatal("Options -A, -Q, -T, and -W, are mutually exclusive, and can be specified only once.");
			needs_explicit_manager = 1;
			query_mode = QUERY_ABLE_WORKERS;
			break;
		case 'l':
			format_mode = FORMAT_LONG;
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 't':
			vine_status_timeout = strtol(optarg, NULL, 10);
			break;
		case 'h':
			show_help(argv[0]);
			exit(EXIT_SUCCESS);
			break;
		case 'R':
			query_mode = QUERY_MASTER_RESOURCES;
			break;
		case LONG_OPT_CAPACITY:
			if(query_mode != NO_QUERY)
				fatal("Options -A, -Q, -T, and -W, are mutually exclusive, and can be specified only once.");
			query_mode = QUERY_CAPACITIES;
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
		case LONG_OPT_WHERE:
			jexpr = jx_parse_string(optarg);
			if (!jexpr) {
				fprintf(stderr,"invalid expression: %s\n", optarg);
				exit(1);
			}
			break;
		case LONG_OPT_USE_SSL:
			manual_ssl_option=1;
			break;
		default:
			show_help(argv[0]);
			exit(EXIT_FAILURE);
			break;
		}
	}

	if (!jexpr) jexpr = jx_boolean(true);

	if(query_mode == NO_QUERY)
		query_mode = QUERY_QUEUE;

	if(needs_explicit_manager && optind >= argc)
		fatal("Options -A, -T and -W need an explicit manager to query.");

	if(*project_name && query_mode != QUERY_QUEUE)
		fatal("Option -M,--project-name can only be used together with -Q,--statistics");

	if( optind < argc ) {
		*manager_host = argv[optind];
		optind++;
	}

	if( optind < argc ) {
		*manager_port = atoi(argv[optind]);
		optind++;
	}

	if(optind < argc) {
		fprintf(stderr,"vine_status: Too many arguments.  Try the -h option for help.\n");
		exit(EXIT_FAILURE);
	}

}

void resize_catalog(size_t new_size)
{
	struct jx **temp_ptr = NULL;
	temp_ptr = realloc(global_catalog,(sizeof(*global_catalog)*new_size + 1)); //add one for NULL stub at the end.

	if(!temp_ptr)
		fatal("failed to allocate memory for the catalog.\n");

	global_catalog = temp_ptr;
	catalog_size = new_size;
}

int get_managers( time_t stoptime )
{
	struct catalog_query *cq;
	struct jx *j;
	int i = 0; //jx pointer array iterator

	if(!catalog_host) {
		catalog_host = strdup(CATALOG_HOST);
	}

	jexpr = jx_operator(
		JX_OP_AND,
		jexpr,
		jx_operator(
			JX_OP_EQ,
			jx_symbol("type"),
			jx_string("vine_manager")
		)
	);


	cq = catalog_query_create(catalog_host, jexpr, stoptime );
	if(!cq)
		fatal("failed to query catalog server %s: %s \n",catalog_host,strerror(errno));

	while((j = catalog_query_read(cq,stoptime))) {
		if(i == catalog_size)
			resize_catalog( catalog_size * 2 );

		// make the global catalog point to this memory that j references
		global_catalog[i] = j;
		i++;
	}

	global_catalog[i] = NULL;

	catalog_query_delete(cq);

	return 1;
}

void global_catalog_cleanup()
{

	int i = 0;
	while(global_catalog[i] != NULL)
	{
		jx_delete(global_catalog[i]);
		i++;
	}
	free(global_catalog);
}

/* append '--...--->' to show foremen/workers tree like structure */
void add_child_relation(const char *name, int spaces, char *buffer, size_t max_size)
{
	if(spaces < 1)
		return;

	memset(buffer, '-', spaces - 1);
	buffer[spaces - 1] = '>';
	buffer[spaces]     = '\0';

	strncat(buffer + spaces, name, max_size - spaces);
}

int find_child_relations(int spaces, const char *host, int port, struct jx_table *headers, time_t stoptime)
{
	int i = 0; //global_catalog iterator
	char full_address[VINE_LINE_MAX];

	if(!domain_name_cache_lookup(host, full_address))
	{
		debug(D_VINE,"Could not resolve %s into an ip address\n",host);
		return 0;
	}

	char *port_str = string_format(":%d", port);
	strcat(full_address, port_str);
	free(port_str);

	while(global_catalog[i] != NULL)
	{
		const char *temp_my_manager = jx_lookup_string(global_catalog[i], "my_manager");

		if(temp_my_manager && !strcmp(temp_my_manager, full_address))
		{
			struct jx *jproject = jx_lookup(global_catalog[i],"project");
			const char *project_name = jproject->u.string_value;

			int branch_len = strlen(project_name) + spaces + 2;

			char *branch = malloc(branch_len);
			add_child_relation(project_name, spaces, branch, branch_len);

			// update project_name
			free(jproject->u.string_value);
			jproject->u.string_value = branch;

			if(format_mode==FORMAT_TABLE) {
				jx_table_print(headers,global_catalog[i], stdout, columns);
			}

			find_child_relations(spaces + 1,
					jx_lookup_string(global_catalog[i], "name"),
					jx_lookup_integer(global_catalog[i], "port"),
					headers,
					stoptime);
		}
		i++;
	}

	return 1;
}

int do_catalog_query(const char *project_name, struct jx_table *headers, time_t stoptime )
{
	int i = 0; //global_catalog iterator
	int first = 1;

	if(format_mode==FORMAT_TABLE) {
		jx_table_print_header(headers,stdout,columns);
	} else {
		printf("[\n");
	}

	while(global_catalog[i] != NULL){
		if(format_mode==FORMAT_LONG) {
			if(first) {
				first = 0;
			} else {
				printf(",\n");
			}
			jx_print_stream(global_catalog[i], stdout);
		} else if(format_mode==FORMAT_TABLE) {
			const char *temp_my_manager = jx_lookup_string(global_catalog[i], "my_manager");
			if( !temp_my_manager || !strcmp(temp_my_manager, "127.0.0.1:-1") ) { //this manager has no manager

				if(!project_name || whole_string_match_regex(jx_lookup_string(global_catalog[i], "project"), project_name)) {
					jx_table_print(headers,global_catalog[i], stdout, columns);
					find_child_relations(1,
							jx_lookup_string(global_catalog[i], "name"),
							jx_lookup_integer(global_catalog[i], "port"),
							headers,
							stoptime);
				}
			}
		}
		i++;
	}

	if(format_mode == FORMAT_TABLE){
		jx_table_print_footer(headers,stdout,columns);
	} else {
		printf("\n]\n");
	}
	global_catalog_cleanup();
	return EXIT_SUCCESS;
}

int do_direct_query( const char *manager_host, int manager_port, time_t stoptime )
{
	static struct jx_table *query_headers[] = { [QUERY_QUEUE] = queue_headers, task_headers, worker_headers, workers_able_headers, manager_resource_headers };
	static const char * query_strings[] = { [QUERY_QUEUE] = "queue","task","worker", "wable", "resources"};

	struct jx_table *query_header = query_headers[query_mode];
	const char * query_string = query_strings[query_mode];

	struct link *l;

	char manager_addr[LINK_ADDRESS_MAX];

	if(!domain_name_cache_lookup(manager_host,manager_addr)) {
		fprintf(stderr,"couldn't find address of %s\n",manager_host);
		return 1;
	}

	l = link_connect(manager_addr,manager_port,stoptime);
	if(!l) {
		fprintf(stderr,"couldn't connect to %s port %d: %s\n",manager_host,manager_port,strerror(errno));
		return 1;
	}

	if(manual_ssl_option && link_ssl_wrap_connect(l) < 1) {
		fprintf(stderr,"could not setup ssl connection.\n");
		return 1;
	}

	link_printf(l,stoptime,"%s_status\n",query_string);

	struct jx *jarray = jx_parse_link(l,stoptime);

	if(!jarray || jarray->type != JX_ARRAY) {
		fprintf(stderr,"couldn't read from %s port %d: %s\n",manager_host,manager_port,strerror(errno));
		return 1;
	}

	if(format_mode==FORMAT_TABLE) {
		jx_table_print_header(query_header,stdout,columns);

		struct jx_item *i;
		for(i=jarray->u.items;i;i=i->next) {
			if(format_mode == FORMAT_TABLE) {
				jx_table_print(query_header,i->value,stdout,columns);
			}
		}
	} else {
		jx_print_stream(jarray,stdout);
	}

	jx_delete(jarray);

	if(format_mode == FORMAT_TABLE) {
		jx_table_print_footer(query_header,stdout,columns);
	}

	return EXIT_SUCCESS;
}

#include <sys/ioctl.h>

int terminal_columns( int fd )
{
	struct winsize window;
	int columns = 80;

	char *columns_str = getenv("COLUMNS");
	if(columns_str) {
		int c = atoi(columns_str);
		if(c>=10) columns = c;
	} else if(ioctl(STDOUT_FILENO, TIOCGWINSZ, &window) >= 0) {
		if(window.ws_col>=10) columns = window.ws_col;
	}

	return columns;
}

int main(int argc, char *argv[])
{
	const char *manager_host  = NULL;
	const char *project_name = NULL;
	int manager_port = VINE_DEFAULT_PORT;

	debug_config(argv[0]);

	vine_status_parse_command_line_arguments(argc, argv, &manager_host, &manager_port, &project_name);

	cctools_version_debug(D_DEBUG, argv[0]);

	columns = terminal_columns(1);

	time_t stoptime = time(0) + vine_status_timeout;

	if(manager_host) {
		return do_direct_query(manager_host,manager_port,stoptime);
	} else {
		global_catalog = malloc(sizeof(*global_catalog)*CATALOG_SIZE); //only malloc if catalog queries are being done
		get_managers(stoptime);
		struct jx_table *h;
		if(query_mode==QUERY_MASTER_RESOURCES) {
			h = manager_resource_headers;
		} else if(query_mode==QUERY_CAPACITIES) {
			h = capacity_headers;
		}
		else {
			h = queue_headers;
		}
		return do_catalog_query(project_name,h,stoptime);
	}
}


/* vim: set noexpandtab tabstop=8: */
