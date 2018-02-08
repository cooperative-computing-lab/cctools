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
#include "set.h"
#include "list.h"
#include "address.h"
#include "zlib.h"

struct catalog_query {
	struct jx *data;
	struct jx *filter_expr;
	struct jx_item *current;
};

struct catalog_host {
	char *host;
	char *url;
	int down;
};

static struct set *down_hosts = NULL;

/* Given a comma delimited list of host:port or host, set the values pointed
   to by host and port, using the default port if not provided. Return the address
   of the next hostport in the string, or NULL if there are no more
*/
const char *parse_hostlist(const char *hosts, char *host, int *port)
{
	char hostport[DOMAIN_NAME_MAX];

	const char *next = strchr(hosts, ',');

	int length = next ? next-hosts+1 : (int) strlen(hosts)+1;

	strncpy(hostport,hosts,length);
	hostport[length-1] = 0;

	if(!address_parse_hostport(hostport,host,port,CATALOG_PORT)) {
		debug(D_DEBUG, "bad host specification: %s",hostport);
		return NULL;
	}

	return next ? next + 1 : NULL;
}

struct jx *catalog_query_send_query(const char *url, time_t stoptime) {
	struct link *link = http_query(url, "GET", stoptime);

	if(!link) {
		return NULL;
	}

	struct jx *j = jx_parse_link(link,stoptime);

	link_close(link);

	if(!j) {
		debug(D_DEBUG,"query result failed to parse as JSON");
		return NULL;
	}

	if(!jx_istype(j,JX_ARRAY)) {
		debug(D_DEBUG,"query result is not a JSON array");
		jx_delete(j);
		return NULL;
	}

	return j;
}

struct list *catalog_query_sort_hostlist(const char *hosts) {
	const char *next_host;
	char *n;
	struct catalog_host *h;
	struct list *previously_up = list_create();
	struct list *previously_down = list_create();

	if(string_null_or_empty(hosts)) {
		next_host = CATALOG_HOST;
	} else {
		next_host = hosts;
	}

	if(!down_hosts) {
		down_hosts = set_create(0);
	}

	do {
		int port;
		char host[DOMAIN_NAME_MAX];
		h = xxmalloc(sizeof(*h));
		next_host = parse_hostlist(next_host, host, &port);

		h->host = xxstrdup(host);
		h->url = string_format("http://%s:%d/query.json", host, port);
		h->down = 0;

		set_first_element(down_hosts);
		while((n = set_next_element(down_hosts))) {
			if(!strcmp(n, host)) {
				h->down = 1;
			}
		}
		if(h->down) {
			list_push_tail(previously_down, h);
		} else {
			list_push_tail(previously_up, h);
		}
	} while (next_host);

	return list_splice(previously_up, previously_down);
}

struct catalog_query *catalog_query_create(const char *hosts, struct jx *filter_expr, time_t stoptime)
{
	struct catalog_query *q = NULL;
	char *n;
	struct catalog_host *h;
	struct list *sorted_hosts = catalog_query_sort_hostlist(hosts);

	list_first_item(sorted_hosts);
	while(time(NULL) < stoptime) {
		if(!(h = list_next_item(sorted_hosts))) {
			list_first_item(sorted_hosts);
			continue;
		}
		struct jx *j = catalog_query_send_query(h->url, time(NULL) + 5);

		if(j) {
			q = xxmalloc(sizeof(*q));
			q->data = j;
			q->current = j->u.items;
			q->filter_expr = filter_expr;

			if(h->down) {
				debug(D_DEBUG,"catalog server at %s is back up", h->host);
				set_first_element(down_hosts);
				while((n = set_next_element(down_hosts))) {
					if(!strcmp(n, h->host)) {
						free(n);
						set_remove(down_hosts, n);
						break;
					}
				}
			}
			break;
		} else {
			if(!h->down) {
				debug(D_DEBUG,"catalog server at %s seems to be down", h->host);
				set_insert(down_hosts, xxstrdup(h->host));
			}
		}
	}

	list_first_item(sorted_hosts);
	while((h = list_next_item(sorted_hosts))) {
		free(h->host);
		free(h->url);
		free(h);
	}
	list_delete(sorted_hosts);
	return q;
}

struct jx *catalog_query_read(struct catalog_query *q, time_t stoptime)
{
	while(q && q->current) {

		int keepit = 1;

		if(q->filter_expr) {
			struct jx * b;
			b = jx_eval(q->filter_expr,q->current->value);
			if(jx_istype(b, JX_BOOLEAN) && b->u.boolean_value) {
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

char *catalog_query_compress_update(const char *text, unsigned long *data_length)
{
	unsigned long compress_data_length;
	/* Default to buffer error incase we don't compress. */
	int success = Z_BUF_ERROR;

	/* Estimates the bounds for the compressed data. */
	compress_data_length = compressBound(*data_length);
	char* compress_data= malloc(compress_data_length);

	success = compress((Bytef*)compress_data+1, &compress_data_length, (const Bytef*)text, *data_length);
	/* Prefix the data with 0x1A (Control-Z) to indicate a compressed packet. */
	compress_data[0] = 0x1A;

	/* Copy data over if not compressing or compression failed. */
	if(success!=Z_OK) {
		/* Compression failed, fall back to original uncompressed update. */
		debug(D_DEBUG,"warning: Unable to compress data for update.\n");
		free(compress_data);
		return NULL;
	} else {
		/* Add 1 to the compresed data length to account for the leading 0x1A. */
		*data_length = compress_data_length + 1;
		return compress_data;
	}
}

static int catalog_query_send_update_internal(const char *hosts, const char *text, int fail_if_too_big )
{
	int sent = 0;
	unsigned long data_length;
	const char *next_host = hosts;
	char *update_data = 0;
	struct datagram *d = datagram_create(DATAGRAM_PORT_ANY);

	if (!d) {
		fatal("could not create datagram port!");
	}
	
	data_length = strlen(text)+1;

	size_t compress_limit = 1200;
	const char *compress_limit_str = getenv("CATALOG_UPDATE_LIMIT");
	if(compress_limit_str) compress_limit = atoi(compress_limit_str);

	if(strlen(text)<compress_limit) {
		update_data = strdup(text);
	} else {
		update_data = catalog_query_compress_update(text, &data_length);
		if(!update_data) {
			datagram_delete(d);
			return 0;
		}

		debug(D_DEBUG,"compressed update message from %d to %d bytes",(int)strlen(text),(int)data_length);

		if(data_length>compress_limit && fail_if_too_big) {
			debug(D_DEBUG,"compressed update message exceeds limit of %d bytes (CATALOG_UPDATE_LIMIT)",(int)compress_limit);
			datagram_delete(d);
			return 0;
		}
	}

	do {
		char address[DATAGRAM_ADDRESS_MAX];
		char host[DOMAIN_NAME_MAX];
		int port;

		next_host = parse_hostlist(next_host, host, &port);
		if (domain_name_cache_lookup(host, address)) {
			debug(D_DEBUG, "sending update to %s(%s):%d", host, address, port);
			datagram_send(d, update_data, data_length, address, port);
			sent++;
		} else {
			debug(D_DEBUG, "unable to lookup address of host: %s", host);
		}
	} while (next_host);

	free(update_data);
	datagram_delete(d);
	return sent;
}

int catalog_query_send_update(const char *hosts, const char *text)
{
	return catalog_query_send_update_internal(hosts,text,0);
}

int catalog_query_send_update_conditional(const char *hosts, const char *text)
{
	return catalog_query_send_update_internal(hosts,text,1);
}

/* vim: set noexpandtab tabstop=4: */
