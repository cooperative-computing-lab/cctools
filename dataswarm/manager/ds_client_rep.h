#ifndef DATASWARM_CLIENT_REP_H
#define DATASWARM_CLIENT_REP_H

/*
ds_client_rep is the data structure representation
of the actual client process that runs somewhere else.
*/

#include "mq.h"
#include "buffer.h"
#include "link.h"

struct ds_client_rep {
	struct mq *connection;
	buffer_t recv_buffer;
	char addr[LINK_ADDRESS_MAX];
	int port;
};

struct ds_client_rep * ds_client_rep_create( struct mq *conn );

#endif
