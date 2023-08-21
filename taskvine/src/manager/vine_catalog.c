/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_catalog.h"

#include "catalog_query.h"
#include "debug.h"
#include "domain_name.h"
#include "jx.h"
#include "list.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/*
Query the catalog for all DS managers whose project name matches the given regex.
Return a linked list of jx expressions describing the managers.
*/

struct list *vine_catalog_query(const char *catalog_host, int catalog_port, const char *project_regex)
{
	char hostport[DOMAIN_NAME_MAX + 8];
	time_t stoptime = time(0) + 60;
	struct catalog_query *q;

	if (catalog_port > 0) {
		sprintf(hostport, "%s:%d", catalog_host, catalog_port);
		q = catalog_query_create(hostport, 0, stoptime);
	} else {
		q = catalog_query_create(catalog_host, 0, stoptime);
	}
	if (!q) {
		debug(D_NOTICE, "unable to contact catalog server at %s:%d\n", catalog_host, catalog_port);
		return 0;
	}

	struct list *managers_list = list_create();

	// for each expression returned by the query
	struct jx *j;
	while ((j = catalog_query_read(q, stoptime))) {
		// if it is a WQ manager...
		const char *type = jx_lookup_string(j, "type");
		if (type && (!strcmp(type, "vine_manager"))) {
			// and the project name matches...
			const char *project = jx_lookup_string(j, "project");
			if (project && whole_string_match_regex(project, project_regex)) {
				// put the item in the list.
				list_push_head(managers_list, j);
				continue;
			}
		}
		// we reach here unless j is push into managers_list
		jx_delete(j);
	}
	catalog_query_delete(q);

	return managers_list;
}

struct list *vine_catalog_query_cached(const char *catalog_host, int catalog_port, const char *project_regex)
{
	static struct list *managers_list = 0;
	static time_t managers_list_timestamp = 0;
	static char *prev_regex = 0;

	if (prev_regex && !strcmp(project_regex, prev_regex) && managers_list &&
			(time(0) - managers_list_timestamp) < 60) {
		return managers_list;
	}

	if (prev_regex) {
		free(prev_regex);
	}
	prev_regex = xxstrdup(project_regex);

	if (managers_list) {
		list_clear(managers_list, (void *)jx_delete);
		list_delete(managers_list);
	}

	while (1) {
		debug(D_VINE, "querying catalog for managers with project=%s", project_regex);
		managers_list = vine_catalog_query(catalog_host, catalog_port, project_regex);
		if (managers_list)
			break;
		debug(D_VINE, "unable to contact catalog, still trying...");
		sleep(5);
	}

	managers_list_timestamp = time(0);

	return managers_list;
}

/* vim: set noexpandtab tabstop=8: */
