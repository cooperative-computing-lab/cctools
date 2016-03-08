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
#include "jx_eval.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "debug.h"

struct catalog_query {
	struct jx *data;
	struct jx *filter_expr;
	struct jx_item *current;
};

struct catalog_query *catalog_query_create(const char *host, int port, const char *filter_expr, time_t stoptime)
{
	if(!host)
		host = CATALOG_HOST;
	if(!port)
		port = CATALOG_PORT;

	char *url = string_format("http://%s:%d/query.json", host, port);
	struct link *link = http_query(url, "GET", stoptime);
	free(url);

	if(!link) return 0;

	struct jx *j = jx_parse_link(link,stoptime);

	link_close(link);

	if(!j) {
		debug(D_DEBUG,"query result failed to parse as JSON");
		return 0;
	}

	if(!jx_istype(j,JX_ARRAY)) {
		debug(D_DEBUG,"query result is not a JSON array");
		jx_delete(j);
		return 0;
	}

	struct catalog_query *q = xxmalloc(sizeof(*q));
	q->data = j;
	q->current = j->u.items;
	if(filter_expr) {
		q->filter_expr = jx_parse_string(filter_expr);
	}
	return q;
}

struct jx *catalog_query_read(struct catalog_query *q, time_t stoptime)
{
	while(q && q->current) {

		int keepit = 1;

		if(q->filter_expr) {
			struct jx * b;
			b = jx_eval(q->filter_expr,q->current->value);
			if(b && b->type && b->u.boolean_value) {
				keepit = 1;
			} else {
				keepit = 0;
			}
			jx_delete(b);
		} else {
			keepit = 1;
		}

		if(keepit) {
			struct jx *result = jx_copy(q->current->value);
			q->current = q->current->next;
			return result;
		}

		q->current = q->current->next;
	}

	return 0;
}

void catalog_query_delete(struct catalog_query *q)
{
	jx_delete(q->filter_expr);
	jx_delete(q->data);
	free(q);
}

/* vim: set noexpandtab tabstop=4: */
