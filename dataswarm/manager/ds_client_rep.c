#include <stdlib.h>
#include <assert.h>

#include "ds_client_rep.h"
#include "ds_message.h"
#include "xxmalloc.h"

struct ds_client_rep * ds_client_rep_create( struct mq *conn )
{
	struct ds_client_rep *c = xxcalloc(1, sizeof(*c));
	c->connection = conn;
	buffer_init(&c->recv_buffer);
	return c;
}

void ds_client_rep_disconnect(struct ds_client_rep *c) {
	if (!c) return;
	mq_close(c->connection);
	buffer_free(&c->recv_buffer);
	free(c);
}

void ds_client_rep_notify(struct ds_client_rep *c, struct jx *msg) {
	assert(c);
	assert(msg);
	if (!c->mailbox) {
		c->mailbox = jx_array(NULL);
	}

	assert(jx_istype(c->mailbox, JX_ARRAY));
	jx_array_insert(c->mailbox, msg);
	ds_client_rep_flush_notifications(c);
}

void ds_client_rep_flush_notifications(struct ds_client_rep *c) {
	assert(c);
	if (!c->waiting) return;
	if (!c->mailbox) return;

	struct jx *response = ds_message_response(c->wait_id, DS_RESULT_SUCCESS, c->mailbox);
	ds_json_send(c->connection, response);
	jx_delete(response);

	c->mailbox = NULL;
	c->waiting = false;
}

void ds_task_notify( struct ds_task *t, struct jx *msg) {
	assert(t);
	assert(msg);

	struct ds_client_rep *c;
	set_first_element(t->subscribers);
	while ((c = set_next_element(t->subscribers))) {
		ds_client_rep_notify(c, jx_copy(msg));
	}
	jx_delete(msg);
}
