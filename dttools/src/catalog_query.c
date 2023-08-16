/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <errno.h>
#include <sys/wait.h>

#include "catalog_query.h"
#include "http_query.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_eval.h"
#include "jx_print.h"
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
#include "macros.h"
#include "b64.h"
#include "fd.h"

struct catalog_query {
	struct jx *data;
	struct jx *filter_expr;
	struct jx_item *current;
};

struct catalog_host {
	char *host;
	int port;
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

/*
This function is a bit complex for backwards compatibility.
Ideally, we send the query string to /query/XXX, where XXX
is the b64 encoded filter expression.

However, we could be dealing with an old catalog server that
does not understand this syntax.  In this case, it will respond
to the query with non-JSON data.  If that happens, fall back
to the old URL to see if it works.
*/

struct jx *catalog_query_send_query( struct catalog_host *h, struct jx *expr, time_t stoptime )
{
	char *expr_str = expr ? jx_print_string(expr) : strdup("true");

	buffer_t buf;
	buffer_init(&buf);
	b64_encode(expr_str,strlen(expr_str),&buf);

	char *url = string_format("http://%s:%d/query/%s",h->host,h->port,buffer_tostring(&buf));
	debug(D_DEBUG,"trying catalog query: %s",url);

	struct link *link = http_query(url, "GET", stoptime);

	free(url);
	buffer_free(&buf);
	free(expr_str);

	if(!link) return 0;

	struct jx *j = jx_parse_link(link,stoptime);

	link_close(link);

	if(!j) {
		url = string_format("http://%s:%d/query.json",h->host,h->port);
		debug(D_DEBUG,"falling back to old query: %s",url);
		link = http_query(url, "GET", stoptime);
		free(url);
		if(!link) return 0;

		j = jx_parse_link(link,stoptime);
		link_close(link);
		if(!j) {
			debug(D_DEBUG,"query result failed to parse as JSON");
			return NULL;
		}

		// fall through here
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
		char host[DOMAIN_NAME_MAX];
		int port;

		h = xxmalloc(sizeof(*h));
		next_host = parse_hostlist(next_host, host, &port);

		h->host = xxstrdup(host);
		h->port = port;
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

	int backoff_interval = 1;

	list_first_item(sorted_hosts);
	while(time(NULL) < stoptime) {
		if(!(h = list_next_item(sorted_hosts))) {
			list_first_item(sorted_hosts);
			sleep(backoff_interval);

			int max_backoff_interval = MAX(0, stoptime - time(NULL));
			backoff_interval = MIN(backoff_interval * 2, max_backoff_interval);

			continue;
		}
		struct jx *j = catalog_query_send_query(h, filter_expr, time(NULL) + 5);

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
						set_remove(down_hosts, n);
						free(n);
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

/*
Determine what protocol to use for catalog updates.
If none is given, default to TCP updates, which are
more likely to make it through various network
translation devices.
*/

static int catalog_update_protocol()
{
	const char *protocol = getenv("CATALOG_UPDATE_PROTOCOL");
	if(!protocol) {
		return 0;
	} else if(!strcmp(protocol,"udp")) {
		return 1;
	} else if(!strcmp(protocol,"tcp")) {
		return 0;
	} else {
		debug(D_NOTICE,"CATALOG_UPDATE_PROTOCOL=%s but should be 'udp' or 'tcp' instead.",protocol);
		return 0;
	}
}

/*
Send a catalog update via a single udp message.
This is inherently a non-blocking action.
*/

static void catalog_update_udp( const char *host, const char *address, int port, const char *text )
{
	debug(D_DEBUG, "sending update via udp to %s(%s):%d", host, address, port);

	struct datagram *d = datagram_create(DATAGRAM_PORT_ANY);
	if(!d) return;
	datagram_send(d, text, strlen(text), address, port);
	datagram_delete(d);
}

/*
Send a catalog update via a tcp connection.
This is inherently a blocking action and could
take some time under non-ideal conditions.
*/

static int catalog_update_tcp( const char *host, const char *address, int port, const char *text )
{
	debug(D_DEBUG, "sending update via tcp to %s(%s):%d", host, address, port);

	time_t stoptime = time(0) + 15;
	struct link *l = link_connect(address,port,stoptime);
	if(!l) {
		debug(D_DEBUG, "failed to connect to %s(%s):%d: %s",host,address,port,strerror(errno));
		return 0;
	}

	link_write(l,text,strlen(text),stoptime);
	link_close(l);
	return 1;
}

/*
Send a catalog update via a tcp connection in the background.
This uses the double-fork technique to ensure that the update
process runs completely independently from the main process,
and that the main process will not have to handle an asynchronous
"child completed" message at any later point.
*/

static int catalog_update_tcp_background( const char *host, const char *address, int port, const char *text )
{
	pid_t pid = fork();
	if(pid==0) {
		pid_t grandpid = fork();
		if(grandpid==0) {
			/* grandchild sends catalog update. */
			catalog_update_tcp(host,address,port,text);
			/* grandchild process exits after sending update. */
			_exit(0);
		} else {
			/* child process exits right away. */
			_exit(0);
		}
	} else if(pid>0) {
		debug(D_DEBUG, "sending update via tcp to %s(%s):%d (background pid %d)", host, address, port, (int)pid);
		pid_t result = waitpid(pid,0,0);
		if(result!=pid) {
			debug(D_DEBUG,"unable to wait for child process %d! (%s)",pid,strerror(errno));
		}
		return 1;
	} else {
		debug(D_DEBUG, "unable to fork update process: %s",strerror(errno));
		return 0;
	}
}

int catalog_query_send_update( const char *hosts, const char *text, catalog_update_flags_t flags )
{
	size_t compress_limit = 1200;
	const char *compress_limit_str = getenv("CATALOG_UPDATE_LIMIT");
	if(compress_limit_str) compress_limit = atoi(compress_limit_str);

	unsigned long data_length = strlen(text);
	char *update_data = 0;
	
	// Ask which protocol should be used.
	int use_udp = catalog_update_protocol();

	// Decide whether to compress the data.
	if(strlen(text)<compress_limit) {
		// Don't bother compressing small updates
		update_data = strdup(text);
	} else {
		// Compress updates above a certain limit.
		update_data = catalog_query_compress_update(text, &data_length);
		if(!update_data) return 0;

		debug(D_DEBUG,"compressed update message from %d to %d bytes",(int)strlen(text),(int)data_length);

		if(data_length>compress_limit && (flags&CATALOG_UPDATE_CONDITIONAL) && !use_udp) {
			debug(D_DEBUG,"compressed update message exceeds limit of %d bytes (CATALOG_UPDATE_LIMIT)",(int)compress_limit);
			return 0;
		}
	}

	int sent = 0;
	const char *next_host = hosts;

	// Send an update to each catalog in the list...
	do {
		char address[DATAGRAM_ADDRESS_MAX];
		char host[DOMAIN_NAME_MAX];
		int port;

		next_host = parse_hostlist(next_host, host, &port);
		if (domain_name_cache_lookup(host, address)) {
			if(use_udp) {
				catalog_update_udp( host, address, port, text );
				sent++;
			} else {
				if(flags&CATALOG_UPDATE_BACKGROUND) {
					sent += catalog_update_tcp_background( host, address, port+1, text );
				} else {
					sent += catalog_update_tcp( host, address, port+1, text );
				}
			}
		} else {
			debug(D_DEBUG, "unable to lookup address of host: %s", host);
		}
	} while (next_host);

	free(update_data);
	return sent;
}

/* vim: set noexpandtab tabstop=8: */
