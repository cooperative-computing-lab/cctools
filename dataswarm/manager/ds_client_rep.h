#ifndef DATASWARM_CLIENT_REP_H
#define DATASWARM_CLIENT_REP_H

/*
ds_client_rep is the data structure representation
of the actual client process that runs somewhere else.
*/

#include <stdbool.h>
#include "ds_task.h"
#include "mq.h"
#include "buffer.h"
#include "link.h"
#include "jx.h"

struct ds_client_rep {
	struct mq *connection;
	buffer_t recv_buffer;
	char addr[LINK_ADDRESS_MAX];
	int port;
	struct jx *mailbox;
	bool waiting;
	jx_int_t wait_id;
};

struct ds_client_rep * ds_client_rep_create( struct mq *conn );
void ds_client_rep_notify(struct ds_client_rep *c, struct jx *msg);
void ds_client_rep_flush_notifications(struct ds_client_rep *c);
void ds_client_rep_disconnect(struct ds_client_rep *c);

//XXX awkward to define this here, but need to be able to link against ds_client_rep_notify
void ds_task_notify( struct ds_task *t, struct jx *msg);

#endif
