/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catalog_query.h"
#include "catalog_server.h"

#include "link_nvpair.h"
#include "http_query.h"
#include "nvpair.h"
#include "xxmalloc.h"
#include "stringtools.h"

struct catalog_query {
	struct link *link;
};

struct catalog_query *catalog_query_create(const char *host, int port, time_t stoptime)
{
	struct catalog_query *q = xxmalloc(sizeof(*q));
	char url[1024];

	if(!host)
		host = CATALOG_HOST;
	if(!port)
		port = CATALOG_PORT;

	sprintf(url, "http://%s:%d/query.text", host, port);

	q->link = http_query(url, "GET", stoptime);
	if(!q->link) {
		free(q);
		return 0;
	}

	return q;
}

struct nvpair *catalog_query_read(struct catalog_query *q, time_t stoptime)
{
	return link_nvpair_read(q->link,stoptime);
}

void catalog_query_delete(struct catalog_query *q)
{
	link_close(q->link);
	free(q);
}

/* vim: set noexpandtab tabstop=4: */
