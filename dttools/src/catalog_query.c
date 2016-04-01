/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include "catalog_query.h"
#include "http_query.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_eval.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "debug.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "domain_name.h"

struct catalog_query {
	struct jx *data;
	struct jx *filter_expr;
	struct jx_item *current;
};

/* Given a semicolon delimited list of host:port or host, set the values pointed
   to by host and port, using the default port if not provided. Return the address
   of the next hostport in the string, or NULL if there are no more
*/
const char *parse_hostlist(const char *hosts, char *host, int *port)
{
	const char *next = strchr(hosts, ';');
	switch (sscanf(hosts, "%[^:;]:%d", host, port)) {
	case 1:
		*port = CATALOG_PORT;
		break;
	case 2:
		break;
	default:
		debug(D_DEBUG, "bad host specification: %s", hosts);
		return NULL;
		break;
	}
	return next ? next + 1 : NULL;
}

struct catalog_query *catalog_query_create(const char *hosts, struct jx *filter_expr, time_t stoptime)
{
	int port;
	const char *next_host = hosts;
	char host[DOMAIN_NAME_MAX];
	
	do {
		next_host = parse_hostlist(next_host, host, &port);

		char *url = string_format("http://%s:%d/query.json", host, port);
		struct link *link = http_query(url, "GET", stoptime);
		free(url);

		if(!link) continue;

		struct jx *j = jx_parse_link(link,stoptime);

		link_close(link);

		if(!j) {
			debug(D_DEBUG,"query result failed to parse as JSON");
			continue;
		}

		if(!jx_istype(j,JX_ARRAY)) {
			debug(D_DEBUG,"query result is not a JSON array");
			jx_delete(j);
			continue;
		}

		struct catalog_query *q = xxmalloc(sizeof(*q));
		q->data = j;
		q->current = j->u.items;
		q->filter_expr = filter_expr;
		return q;
	} while (next_host);
	return NULL;
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

int catalog_query_send_update(const char *hosts, const char *text)
{
	int port;
	int sent = 0;
	const char *next_host = hosts;
	char address[DATAGRAM_ADDRESS_MAX];
	char host[DOMAIN_NAME_MAX];
	struct datagram *d = datagram_create(DATAGRAM_PORT_ANY);

	if (!d) {
		fatal("could not create datagram port!");
	}

	do {
		next_host = parse_hostlist(next_host, host, &port);
		if (domain_name_cache_lookup(host, address)) {
			debug(D_DEBUG, "sending update to %s(%s):%d", host, address, port);
			datagram_send(d, text, strlen(text), address, port);
			sent++;
		} else {
			debug(D_DEBUG, "unable to lookup address of host: %s", host);
		}
	} while (next_host);

	datagram_delete(d);
	return sent;
}

/* vim: set noexpandtab tabstop=4: */
