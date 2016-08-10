/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


/* 
 * File:   makeflow_status.c
 * Author: Kyle D. Sweeney
 *
 * Created on July 21, 2016
 */


#include "timestamp.h"
#include "jx_table.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "stringtools.h"
#include "list.h"
#include "catalog_query.h"
#include "json.h"
#include "json_aux.h"
#include "getopt_aux.h"
#include "cctools.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

/*static struct jx_table headers[] = {
	{"type", "TYPE", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
	{"name", "NAME", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, -25},
	{"port", "PORT", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 5},
	{"owner", "OWNER", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
	{"version", "VERSION", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
        {"project", "PROJECT", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 8},
        {"total", "TOTAL", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
        {"running", "RUNNING", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
        {"waiting", "WAITING", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
        {"aborted", "ABORTED", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
        {"completed", "COMPLETED", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
        {"failed", "FAILED", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
        {"time_started", "TIME_STARTED", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
        {"batch_type", "BATCH_TYPE", JX_TABLE_MODE_PLAIN, JX_TABLE_ALIGN_LEFT, 10},
        
	{0, 0, 0, 0, 0}
};*/

static void show_help(const char *cmd)
{
	fprintf(stdout, "makeflow_status [options]\n");
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Find all from this user.\n", "-u,--username=<user>");
        fprintf(stdout, " %-30s Find this project name.\n", "-N, --project=<proj>");
        fprintf(stdout, " %-30s Use this Server address.\n", "-s, --server=<server>");
        fprintf(stdout, " %-30s Use this Port on the server.\n", "-p, --port=<port>");
	fprintf(stdout, " %-30s Timeout.\n", "-t,--timeout=<time>");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

int compare_entries(struct jx **a, struct jx **b)
{
	int result;
	const char *x, *y;

        // by project
	x = jx_lookup_string(*a, "project");
	if(!x)
		x = "unknown";

	y = jx_lookup_string(*b, "project");
	if(!y)
		y = "unknown";

	result = strcasecmp(x, y);
	if(result != 0)
		return result;
        
        //by owner
        x = jx_lookup_string(*a, "owner");
	if(!x)
		x = "unknown";

	y = jx_lookup_string(*b, "owner");
	if(!y)
		y = "unknown";

	result = strcasecmp(x, y);
	if(result != 0)
		return result;

        //failsafe by servername
	x = jx_lookup_string(*a, "name");
	if(!x)
		x = "unknown";

	y = jx_lookup_string(*b, "name");
	if(!y)
		y = "unknown";

	return strcasecmp(x, y);
}

static struct jx *table[10000];

/*
 * Obtains information from the Catalog, format it, and make return it to user.
 */
int main(int argc, char** argv) {

    static const struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"port", required_argument, 0, 'p'},
        {"project", required_argument, 0, 'N'},
        {"server", required_argument, 0, 's'},
        {"timeout", required_argument, 0, 't'},
        {"username", required_argument, 0, 'u'},
        {0, 0, 0, 0}
    };

    struct catalog_query *q;
    struct jx *j;
    int c;
    unsigned int i;
    time_t timeout = 60;
    char* catalog_host = NULL;
    
    char* username = NULL;
    char* project = NULL;
    
    char* server = NULL;
    int port = 0;
    long long porthelp = 0;
    
    while ((c = getopt_long(argc, argv, "N:t:u:w:p:s:h", long_options, NULL)) > -1) {
        switch (c) {
            case 'N':
                project = xxstrdup(optarg);
                break;
            case 't':
                timeout = string_time_parse(optarg);
                break;
            case 'u':
                username = xxstrdup(optarg);
                break;
            case 's':
                server = xxstrdup(optarg);
                break;
            case 'p':
                if(string_is_integer(optarg, &porthelp)){
                    port = (int)porthelp;
                }
                break;
            case 'h':
            default:
                show_help(argv[0]);
                return 1;
        }
    }
    
    //setup address
    if(port != 0 && server!=NULL)
        catalog_host = string_format("%s:%i",server,port);
    else if(port == 0 && server != NULL)
        catalog_host =string_format("%s:%i",server,CATALOG_PORT);
    else if(port != 0 && server == NULL)
        catalog_host = string_format("%s:%i",CATALOG_HOST,port);
    else
        catalog_host = string_format("%s:%i",CATALOG_HOST,CATALOG_PORT);
    
    //make query string
    const char* query_expr = "type==\"makeflow\"";
    if(project && username){
        query_expr = string_format("%s && project==\"%s\" && username==\"%s\"",query_expr,project,username);
    }else if(project) {
        query_expr = string_format("%s && project==\"%s\"", query_expr, project);
    } else if (username) {
        query_expr = string_format("%s && username==\"%s\"", query_expr, username);
    }

    //turn query into jx
    struct jx *jexpr = jx_parse_string(query_expr);
    if (!jexpr) {
        fprintf(stderr, "invalid expression: %s\n", query_expr);
        return 1;
    }

    time_t stoptime = time(0) + timeout;
    unsigned int count = 0;
    
    //create catalog_query from jx
    q = catalog_query_create(catalog_host, jexpr, stoptime);
    if (!q) {
        fprintf(stderr, "couldn't query catalog: %s\n", strerror(errno));
        return 1;
    }


    while ((j = catalog_query_read(q, stoptime))) {
        table[count++] = j;
    }
    
    catalog_query_delete(q);//all done with connection

    //sort
    qsort(table, count, sizeof(*table), (int (*)(const void *, const void *)) compare_entries);
    
    //print them out
    //printf("Owner      Project    total      running    waiting    aborted    completed  failed     time_started                batch_type   \n");
    printf("%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-24s %-10s\n",
            "Owner", "Projct", "total", "running", "waiting", "aborted", "completed", "failed", "time_started", "batch_type");
    for(i=0; i<count; i++){
        const char *jxowner, *jxproject, *jxbatchtype;
        int64_t  jxtotal, jxrunning, jxwaiting, jxaborted, jxcompleted, jxfailed, jxtimestart;

	jxowner = jx_lookup_string(table[i], "owner");
        jxproject = jx_lookup_string(table[i], "project");
        jxtotal = jx_lookup_integer(table[i], "total");
        jxrunning = jx_lookup_integer(table[i], "running");
        jxwaiting = jx_lookup_integer(table[i], "waiting");
        jxaborted = jx_lookup_integer(table[i], "aborted");
        jxcompleted = jx_lookup_integer(table[i], "completed");
        jxfailed = jx_lookup_integer(table[i], "failed");
        jxtimestart = jx_lookup_integer(table[i], "time_started");
        jxbatchtype = jx_lookup_string(table[i], "batch_type");
        
        printf("%-10s %-10s %-10" PRId64 " %-10" PRId64 " %-10" PRId64 " %-10" PRId64 " %-10" PRId64 " %-10" PRId64 " %-24" PRId64 " %-10s\n",
                jxowner, jxproject, jxtotal, jxrunning, jxwaiting, jxaborted, jxcompleted, jxfailed, jxtimestart, jxbatchtype);
        
	//jx_print_stream(table[i],stdout);
    }
    printf("\n");//be polite
    
    //cleanup
    for(i=0;i<count;i++) {
	jx_delete(table[i]);
    }

    //done
    return (EXIT_SUCCESS);
}

