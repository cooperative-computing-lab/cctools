/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catalog_query.h"
#include "http_query.h"
#include "jx.h"
#include "jx_parse.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "debug.h"

struct catalog_query {
	struct jx *data;
	struct jx_item *next_item;
};

struct catalog_query *catalog_query_create(const char *host, int port, time_t stoptime)
{
	char url[1024];

	if(!host)
		host = CATALOG_HOST;
	if(!port)
		port = CATALOG_PORT;

	sprintf(url, "http://%s:%d/query.text", host, port);

       	struct link *link = http_query(url, "GET", stoptime);
	if(!link) return 0;


	struct jx *j = jx_parse_link(link,stoptime);
	if(!j) {
		debug(D_DEBUG,"query result failed to parse as JSON");
		link_close(link);
		return 0;
	}

	if(!jx_istype(j,JX_ARRAY)) {
		debug(D_DEBUG,"query result is not a JSON array");
		jx_delete(j);
		return 0;
	}

	struct catalog_query *q = xxmalloc(sizeof(*q));
	q->data = j;
	q->next_item = j->items;
	return q;
}

struct jx *catalog_query_read(struct catalog_query *q, time_t stoptime)
{
	if(!q || !q->next_item) return 0;

	struct jx *result = jx_copy(q->next_item->value);
	q->next_item = q->next_item->next;

	return result;
}

void catalog_query_delete(struct catalog_query *q)
{
	jx_delete(q->data);
	free(q);
}

/* vim: set noexpandtab tabstop=4: */
