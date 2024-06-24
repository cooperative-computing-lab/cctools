/*
Copyright (C) 2022 The University of Notre Dame
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
#include "getopt_aux.h"
#include "cctools.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

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
    
    while ((c = getopt_long(argc, argv, "N:t:u:w:s:h", long_options, NULL)) > -1) {
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
            case 'h':
            default:
                show_help(argv[0]);
                return 1;
        }
    }
    
    //setup address
    if(server==NULL){
        catalog_host = CATALOG_HOST;
    }

    //make query
    struct jx *jexpr = jx_operator(
        JX_OP_EQ,
        jx_symbol("type"),
        jx_string("makeflow")
    );

    if (project) {
        jexpr = jx_operator(
            JX_OP_AND,
            jexpr,
            jx_operator(
                JX_OP_EQ,
                jx_symbol("project"),
                jx_string(project)
            )
        );
    }

    if (username) {
        jexpr = jx_operator(
            JX_OP_AND,
            jexpr,
            jx_operator(
                JX_OP_EQ,
                jx_symbol("username"),
                jx_string(username)
            )
        );
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
    printf("%-10s %-18s %6s %6s %6s %6s %6s %6s %6s\n",
	   "OWNER", "PROJECT", "JOBS", "WAIT", "RUN", "COMP", "ABRT", "FAIL", "TYPE");
    for(i=0; i<count; i++){
        printf("%-10s %-18s %6" PRId64 " %6" PRId64 " %6" PRId64 " %6" PRId64 " %6" PRId64 " %6" PRId64 " %6s\n",
		jx_lookup_string(table[i], "owner"),
		jx_lookup_string(table[i], "project"),
		jx_lookup_integer(table[i], "total"),
		jx_lookup_integer(table[i], "waiting"),
		jx_lookup_integer(table[i], "running"),
		jx_lookup_integer(table[i], "completed"),
		jx_lookup_integer(table[i], "aborted"),
		jx_lookup_integer(table[i], "failed"),
		jx_lookup_string(table[i], "batch_type")
	);
    }
    printf("\n");//be polite
    
    //cleanup
    for(i=0;i<count;i++) {
	jx_delete(table[i]);
    }

    //done
    return (EXIT_SUCCESS);
}

