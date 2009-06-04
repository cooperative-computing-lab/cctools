/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "catalog_query.h"
#include "catalog_server.h"

#include "http_query.h"
#include "nvpair.h"
#include "xmalloc.h"
#include "stringtools.h"

#define QUERY "GET HTTP/1.0 /query.text\n\n"

struct catalog_query {
	struct link *link;
};

struct catalog_query * catalog_query_create( const char *host, int port, time_t stoptime )
{
	struct catalog_query *q = xxmalloc(sizeof(*q));
	char url[1024];

	if(!host) host = CATALOG_HOST;
	if(!port) port = CATALOG_PORT;

	sprintf(url,"http://%s:%d/query.text",host,port);

	q->link = http_query(url,"GET",stoptime);
	if(!q->link) {
		free(q);
		return 0;
	}

	return q;
}

struct nvpair * catalog_query_read( struct catalog_query *q, time_t stoptime )
{
	struct nvpair *nv = nvpair_create();
	char line[65536];
	int lines = 0;

	while(link_readline(q->link,line,sizeof(line),stoptime)) {
		string_chomp(line);
		if(!line[0]) break;
		nvpair_parse(nv,line);
		lines++;
	}

	if(lines) {
		return nv;
	} else {
		nvpair_delete(nv);
		return 0;
	}
}

void catalog_query_delete( struct catalog_query *q )
{
	link_close(q->link);
	free(q);
}

