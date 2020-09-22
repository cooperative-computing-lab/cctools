#include <stdlib.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <getopt.h>
#include <errno.h>

#include "link.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"
#include "stringtools.h"
#include "cctools.h"
#include "hash_table.h"
#include "itable.h"
#include "username.h"
#include "catalog_query.h"

#include "dataswarm_message.h"
#include "dataswarm_worker_rep.h"
#include "dataswarm_manager.h"

struct dataswarm_worker_rep * dataswarm_worker_rep_create( struct link *l )
{
	struct dataswarm_worker_rep *w = malloc(sizeof(*w));
	w->link = l;
	link_address_remote(w->link,w->addr,&w->port);

	w->blobs = hash_table_create(0,0);
	w->tasks = hash_table_create(0,0);

	w->blob_of_rpc = itable_create(0);
	w->task_of_rpc = itable_create(0);

	return w;
}

void dataswarm_worker_rep_async_update( struct dataswarm_worker_rep *w, struct jx *msg )
{
	/* do something with the message ! */
}


/* vim: set noexpandtab tabstop=4: */
