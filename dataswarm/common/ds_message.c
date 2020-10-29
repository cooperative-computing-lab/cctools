#include "ds_message.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "buffer.h"
#include "debug.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "xxmalloc.h"

int ds_message_send(struct mq *mq, const char *str, int length)
{
	buffer_t *buf = xxmalloc(sizeof(*buf));
	buffer_init(buf);
	buffer_putlstring(buf, str, length);
	debug(D_DATASWARM, "msg  tx: %s", str);

	int rc = mq_send_buffer(mq, buf, 0);
	if (rc == -1) {
		buffer_free(buf);
		free(buf);
	}
	return rc;
}

int ds_json_send(struct mq *mq, struct jx *j)
{
	buffer_t *buf = xxmalloc(sizeof(*buf));
	buffer_init(buf);
	jx_print_buffer(j, buf);
	debug(D_DATASWARM, "json tx: %s", buffer_tostring(buf));

	int rc = mq_send_buffer(mq, buf, 0);
	if (rc == -1) {
		buffer_free(buf);
		free(buf);
	}
	return rc;
}

int ds_fd_send(struct mq *mq, int fd, size_t length)
{
	debug(D_DATASWARM, "fd   tx: %i", fd);
	int rc = mq_send_fd(mq, fd, length);
	if (rc == -1) {
		close(fd);
	}
	return rc;
}

struct jx * ds_message_standard_response( int64_t id, ds_result_t code, struct jx *params )
{
	struct jx *message = jx_objectv(
            "method", jx_string("response"),
            "id",     jx_integer(id),
            "result", jx_integer(code),
            NULL);

	if(code!=DS_RESULT_SUCCESS) {
		// XXX send string instead?
		jx_insert_integer(message,"error",code);
	}

	if(params) {
		jx_insert(message,jx_string("params"),jx_copy(params));
	} else {
		jx_insert(message,jx_string("params"),jx_object(0));
    }

	return message;
}

struct jx * ds_message_task_update( const struct ds_task *t )
{
	struct jx *message = jx_objectv(
            "method", jx_string("task-update"),
            "params", jx_objectv(
                "task-id", jx_string(t->taskid),
                "state", jx_integer(t->state),
                "result", jx_integer(t->result),
                0),
            0);

	return message;
}

struct jx * ds_message_blob_update( const char *blobid, ds_blob_state_t state )
{
	struct jx *params = jx_object(0);
	jx_insert_string(params,"blob-id",blobid);
	jx_insert_integer(params,"state",state);

	struct jx *message = jx_object(0);
	jx_insert_string(message, "method", "blob-update");
	jx_insert(message,jx_string("params"),params);

	return message;
}

struct jx *ds_parse_message(buffer_t *buf) {
	assert(buf);
    const char *contents = buffer_tostring(buf);
	debug(D_DATASWARM, "rx: %s", contents);

	struct jx *out = jx_parse_string(contents);
	buffer_rewind(buf, 0);
	return out;
}
