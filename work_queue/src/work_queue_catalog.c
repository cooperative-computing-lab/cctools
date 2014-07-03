/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue_catalog.h"

#include "catalog_query.h"
#include "nvpair.h"
#include "list.h"
#include "debug.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>

int work_queue_catalog_parse( char *server_string, char **host, int *port )
{
	char *colon;

	colon = strchr(server_string, ':');

	if(!colon) {
		*host = NULL;
		*port = 0;
		return 0;
	}

	*colon = '\0';

	*host = strdup(server_string);
	*port = atoi(colon + 1);

	*colon = ':';

	// if (*port) == 0, parsing failed, thus return 0
	return *port;
}

/*
Query the catalog for all WQ masters whose project name matches the given regex.
Return a linked list of nvpairs describing the masters.
*/

struct list * work_queue_catalog_query( const char *catalog_host, int catalog_port, const char *project_regex )
{
	time_t stoptime = time(0) + 60;

	struct catalog_query *q = catalog_query_create(catalog_host, catalog_port, stoptime);
	if(!q) {
		debug(D_NOTICE,"unable to contact catalog server at %s:%d\n", catalog_host, catalog_port);
		return 0;
	}

	struct list *masters_list = list_create();

	// for each nvpair returned by the query
	struct nvpair *nv;
	while((nv = catalog_query_read(q, stoptime))) {

		// if it is a WQ master...
		const char *nv_type = nvpair_lookup_string(nv,"type");
		if(nv_type && !strcmp(nv_type,"wq_master")) {

			// and the project name matches...
			const char *nv_project = nvpair_lookup_string(nv,"project");
			if(nv_project && whole_string_match_regex(nv_project,project_regex)) {

				// put the item in the list.
				list_push_head(masters_list,nv);
			}
		}
	}

	catalog_query_delete(q);

	return masters_list;
}

struct list * work_queue_catalog_query_cached( const char *catalog_host, int catalog_port, const char *project_regex )
{
	static struct list * masters_list = 0;
	static time_t masters_list_timestamp = 0;

	if(masters_list && (time(0)-masters_list_timestamp)<60) {
		return masters_list;
	}

	if(masters_list) {
		struct nvpair *nv;
		while((nv=list_pop_head(masters_list))) {
			nvpair_delete(nv);
		}
	}

	while(1) {
		debug(D_WQ,"querying catalog for masters with project=%s",project_regex);
		masters_list = work_queue_catalog_query(catalog_host,catalog_port,project_regex);
		if(masters_list) break;
		debug(D_WQ,"unable to contact catalog, still trying...");
		sleep(5);
	}

	masters_list_timestamp = time(0);

	return masters_list;
}

