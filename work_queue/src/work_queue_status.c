/* 
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#include "cctools.h"
#include "debug.h"
#include "catalog_query.h"
#include "catalog_server.h"
#include "domain_name_cache.h"
#include "work_queue.h"
#include "nvpair.h"
#include "link_nvpair.h"
#include "link.h"
#include "getopt.h"
#include "work_queue_catalog.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>

typedef enum {
	FORMAT_TABLE,
	FORMAT_LONG
} format_t;

typedef enum {
	QUERY_QUEUE,
	QUERY_TASKS,
	QUERY_WORKERS,
	QUERY_MASTER_RESOURCES
} query_t;

#define CATALOG_SIZE 50 //size of the array of nvpair pointers

static format_t format_mode = FORMAT_TABLE;
static query_t query_mode = QUERY_QUEUE;
static int work_queue_status_timeout = 300;
static char *catalog_host = NULL;
static int catalog_port = 0;
static int resource_mode = 0;
int catalog_size = CATALOG_SIZE;
static struct nvpair **global_catalog = NULL; //pointer to an array of nvpair pointers

static struct nvpair_header queue_headers[] = {
	{"project",       "PROJECT", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 18},
	{"name",          "HOST",    NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 22},
	{"port",          "PORT",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 5},
	{"tasks_waiting", "WAITING",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 7},
	{"tasks_running",  "RUNNING",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 7},
	{"tasks_complete","COMPLETE",    NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 8},
	{"workers",       "WORKERS", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 7},
	{NULL,NULL,0,0,0}
};

static struct nvpair_header task_headers[] = {
	{"taskid",       "ID",      NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_LEFT, 8},
	{"state",        "STATE",   NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT,  8},
	{"host",         "HOST",    NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 24},
	{"command",      "COMMAND", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 38},
	{NULL,NULL,0,0,0}
};

static struct nvpair_header worker_headers[] = {
	{"hostname",             "HOST",    NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 24},
	{"address_port",             "ADDRESS", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT,16},
	{"total_tasks_complete",       "COMPLETED",   NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_RIGHT, 9},
	{"total_tasks_running",        "RUNNING",   NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT,8},
	{NULL,NULL,0,0,0}
};

static struct nvpair_header master_resource_headers[] = {
	{"project",	"MASTER",	NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 30},
	{"cores_total",	"CORES",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_LEFT, 10},
	{"memory_total",	"MEMORY",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_LEFT, 15},
	{"disk_total",	"DISK",	NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_LEFT, 20},
	{NULL,NULL,0,0,0}
};

static void show_help(const char *progname)
{
	fprintf(stdout, "usage: %s [master] [port]\n", progname);
	fprintf(stdout, "If a master and port are given, get data directly from that master.\n");
	fprintf(stdout, "Otherwise, contact the catalog server for summary data.\n");
	fprintf(stdout, "Options:\n");
	fprintf(stdout, " %-30s Show queue summary statistics. (default)\n", "-Q,--statistics");
	fprintf(stdout, " %-30s List workers connected to the master.\n", "-W,--workers");
	fprintf(stdout, " %-30s List tasks of a given master.\n", "-T,--tasks");
	fprintf(stdout, " %-30s Long text output.\n", "-l,--verbose");
	fprintf(stdout, " %-30s Shows aggregated resources of all masters.\n", "-R,--resources");
	fprintf(stdout, " %-30s Set catalog server to <catalog>. Format: HOSTNAME:PORT\n", "-C,--catalog=<catalog>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug <flag>");
	fprintf(stdout, " %-30s RPC timeout (default is %ds).\n", "-t,--timeout=<time>", work_queue_status_timeout);
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

static void work_queue_status_parse_command_line_arguments(int argc, char *argv[])
{
	signed int c;

	static struct option long_options[] = {
		{"statistics", no_argument, 0, 'Q'},
		{"workers", no_argument, 0, 'W'},
		{"tasks", no_argument, 0, 'T'},
		{"verbose", no_argument, 0, 'l'},
		{"resources", no_argument, 0, 'R'},
		{"catalog", required_argument, 0, 'C'},
		{"debug", required_argument, 0, 'd'},
		{"timeout", required_argument, 0, 't'},
		{"help", no_argument, 0, 'h'},
        {0,0,0,0}};

	while((c = getopt_long(argc, argv, "QTWC:d:lo:O:Rt:vh", long_options, NULL)) > -1) {
		switch (c) {
		case 'C':
			if(!parse_catalog_server_description(optarg, &catalog_host, &catalog_port)) {
				fprintf(stderr, "Cannot parse catalog description: %s. \n", optarg);
				exit(EXIT_FAILURE);
			}
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'Q':
			query_mode = QUERY_QUEUE;
			break;
		case 'T':
			query_mode = QUERY_TASKS;
			break;
		case 'W':
			query_mode = QUERY_WORKERS;
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
			work_queue_status_timeout = strtol(optarg, NULL, 10);
			break;
		case 'h':
			show_help(argv[0]);
			exit(EXIT_SUCCESS);
			break;
		case 'R':
			query_mode = QUERY_MASTER_RESOURCES;
			resource_mode = 1;
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
		default:
			show_help(argv[0]);
			exit(EXIT_FAILURE);
			break;
		}
	}
}

void resize_catalog(size_t new_size)
{
	struct nvpair **temp_ptr = NULL;
	temp_ptr = realloc(global_catalog,(sizeof(*global_catalog)*new_size + 1)); //add one for NULL stub at the end.

	if(!temp_ptr)
		fatal("failed to allocate memory for the catalog.\n");

	global_catalog = temp_ptr;
	catalog_size = new_size;
}

int get_masters(time_t stoptime)
{
	struct catalog_query *cq;
	struct nvpair *nv;
	int i = 0; //nvpair pointer array iterator
	if(!catalog_host) {
		catalog_host = strdup(CATALOG_HOST);
		catalog_port = CATALOG_PORT;
	}

	cq = catalog_query_create(catalog_host, catalog_port, stoptime );
	if(!cq)
		fatal("failed to query catalog server %s:%d: %s \n",catalog_host,catalog_port,strerror(errno));

	while((nv = catalog_query_read(cq,stoptime))) {
		if(i == catalog_size)
			resize_catalog( catalog_size * 2 );

		if(strcmp(nvpair_lookup_string(nv, "type"), CATALOG_TYPE_WORK_QUEUE_MASTER) == 0) {
			global_catalog[i] = nv; //make the global catalog point to this memory that nv references
			i++; //only increment i when a master nvpair is found
		}else{
			nvpair_delete(nv); //free the memory so something valid can take its place
		}
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
		nvpair_delete(global_catalog[i]);
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

int find_child_relations(int spaces, const char *host, int port, time_t stoptime)
{
	int i = 0; //global_catalog iterator
	char full_address[1024];

	if(!domain_name_cache_lookup(host, full_address))
	{
		debug(D_WQ,"Could not resolve %s into an ip address\n",host);
		return 0;
	}

	sprintf(full_address, "%s:%d", full_address, port);

	while(global_catalog[i] != NULL)
	{
		const char *temp_my_master = nvpair_lookup_string(global_catalog[i], "my_master");

		if(temp_my_master && !strcmp(temp_my_master, full_address))
		{
			const char *project_name = nvpair_lookup_string(global_catalog[i], "project");
			int branch_len = strlen(project_name) + spaces + 2; 

			char *branch = malloc(branch_len);
			add_child_relation(project_name, spaces, branch, branch_len);

			// update project_name
			nvpair_remove(global_catalog[i], project_name); 
			nvpair_insert_string(global_catalog[i], "project", branch);

			if(resource_mode)
				nvpair_print_table(global_catalog[i], stdout, master_resource_headers);
			else if(format_mode == FORMAT_TABLE)
				nvpair_print_table(global_catalog[i], stdout, queue_headers);
			
			find_child_relations(spaces + 1,
					nvpair_lookup_string(global_catalog[i], "name"),
					atoi(nvpair_lookup_string(global_catalog[i], "port")),
					stoptime);
		}
		i++;
	}
	
	return 1;
}

int do_catalog_query( time_t stoptime )
{
	int i = 0; //global_catalog iterator

	if(resource_mode == 0 && format_mode == FORMAT_TABLE) nvpair_print_table_header(stdout, queue_headers);
	else if(resource_mode) nvpair_print_table_header(stdout, master_resource_headers);

	while(global_catalog[i] != NULL){
		if(!(resource_mode || format_mode == FORMAT_TABLE)){ //long options
			nvpair_print_text(global_catalog[i], stdout);
		}else{
			const char *temp_my_master = nvpair_lookup_string(global_catalog[i], "my_master");
			if( !temp_my_master || !strcmp(temp_my_master, "127.0.0.1:-1") ) { //this master has no master
				if(resource_mode) {
					debug(D_WQ,"%s resources -- cores:%s memory:%s disk:%s\n",nvpair_lookup_string(global_catalog[i],"project"),nvpair_lookup_string(global_catalog[i],"cores_total"),nvpair_lookup_string(global_catalog[i],"memory_total"),nvpair_lookup_string(global_catalog[i],"disk_total"));
					nvpair_print_table(global_catalog[i], stdout, master_resource_headers);
				}else if(format_mode == FORMAT_TABLE){
					nvpair_print_table(global_catalog[i], stdout, queue_headers);
				}
				find_child_relations(1, 
						nvpair_lookup_string(global_catalog[i], "name"), 
						atoi(nvpair_lookup_string(global_catalog[i], "port")), 
						stoptime);
			}
		}
		i++;	
	}
	
	if(format_mode == FORMAT_TABLE){
		nvpair_print_table_footer(stdout, queue_headers);
	}else if(resource_mode){
		nvpair_print_table_footer(stdout, master_resource_headers);
	}
	global_catalog_cleanup();
	return EXIT_SUCCESS;
}

int do_direct_query( const char *master_host, int master_port, time_t stoptime )
{
	static struct nvpair_header *query_headers[4] = { queue_headers, task_headers, worker_headers, master_resource_headers };
	static const char * query_strings[4] = {"queue","task","worker", "master_resource"};

	struct nvpair_header *query_header = query_headers[query_mode];
	const char * query_string = query_strings[query_mode];

	struct link *l;
	struct nvpair *nv;

	char master_addr[LINK_ADDRESS_MAX];

	if(!domain_name_cache_lookup(master_host,master_addr)) {
		fprintf(stderr,"couldn't find address of %s\n",master_host);
		return 1;
	}

	l = link_connect(master_addr,master_port,stoptime);
	if(!l) {
		fprintf(stderr,"couldn't connect to %s port %d: %s\n",master_host,master_port,strerror(errno));
		return 1;
	}

	link_putfstring(l,"%s_status\n",stoptime,query_string);

	if(format_mode==FORMAT_TABLE) {
		nvpair_print_table_header(stdout, query_header);
	}

	while((nv = link_nvpair_read(l,stoptime))) {
		if(format_mode == FORMAT_TABLE) {
			nvpair_print_table(nv, stdout, query_header);
		} else {
			nvpair_print_text(nv, stdout);
		}
		nvpair_delete(nv);
	}

	if(format_mode == FORMAT_TABLE) {
		nvpair_print_table_footer(stdout, query_header);
	}

	return EXIT_SUCCESS;
}

int main(int argc, char *argv[])
{
	const char *master_host = 0;
	int master_port = WORK_QUEUE_DEFAULT_PORT;

	debug_config(argv[0]);

	work_queue_status_parse_command_line_arguments(argc, argv);

	cctools_version_debug(D_DEBUG, argv[0]);

	if( optind < argc ) {
		master_host = argv[optind];
		optind++;
	}

	if( optind < argc ) {
		master_port = atoi(argv[optind]);
		optind++;
	}

	if(optind < argc) {
		fprintf(stderr,"work_queue_status: Too many arguments.  Try the -h option for help.\n");
		exit(EXIT_FAILURE);
	}

	time_t stoptime = time(0) + work_queue_status_timeout;

	if(master_host) {
		return do_direct_query(master_host,master_port,stoptime);
	} else {
		global_catalog = malloc(sizeof(*global_catalog)*CATALOG_SIZE); //only malloc if catalog queries are being done
		get_masters(stoptime);
		return do_catalog_query(stoptime);
	}
}

