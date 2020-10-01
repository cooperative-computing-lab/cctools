#include <stdlib.h>

#include "ds_client_rep.h"
#include "xxmalloc.h"

struct ds_client_rep * ds_client_rep_create( struct mq *conn )
{
	struct ds_client_rep *c = xxmalloc(sizeof(*c));
	c->connection = conn;
	buffer_init(&c->recv_buffer);
	return c;
}

