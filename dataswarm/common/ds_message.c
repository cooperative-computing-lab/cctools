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
	debug(D_DATASWARM, "msg  tx: %s", str);
	buffer_t *buf = xxmalloc(sizeof(*buf));
	buffer_init(buf);
	buffer_putlstring(buf, str, length);
	int rc = mq_send_buffer(mq, buf, 0);
	if (rc == -1) {
		buffer_free(buf);
		free(buf);
	}
	return rc;
}

int ds_json_send(struct mq *mq, struct jx *j)
{
	debug(D_DATASWARM, "json tx: %p", j);
	buffer_t *buf = xxmalloc(sizeof(*buf));
	buffer_init(buf);
	jx_print_buffer(j, buf);
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
	struct jx *message = jx_object(0);

	jx_insert_string(message, "method", "response");
	jx_insert_integer(message, "id", id );
	jx_insert_integer(message, "result", code );

	if(code!=DS_RESULT_SUCCESS) {
		// XXX send string instead?
		jx_insert_integer(message,"error",code);
	}

	if(params) {
		jx_insert(message,jx_string("params"),jx_copy(params));
	}

	return message;
}

struct jx * ds_message_task_update( const char *taskid, const char *state )
{
	struct jx *params = jx_object(0);
	jx_insert_string(params,"task-id",taskid);
	jx_insert_string(params,"state",state);

	struct jx *message = jx_object(0);
	jx_insert_string(message, "method", "task-update");
	jx_insert(message,jx_string("params"),params);

	return message;
}

struct jx * ds_message_blob_update( const char *blobid, const char *state )
{
	struct jx *params = jx_object(0);
	jx_insert_string(params,"blob-id",blobid);
	jx_insert_string(params,"state",state);

	struct jx *message = jx_object(0);
	jx_insert_string(message, "method", "blob-update");
	jx_insert(message,jx_string("params"),params);

	return message;
}

struct jx *ds_parse_message(buffer_t *buf) {
	assert(buf);
	struct jx *out = jx_parse_string(buffer_tostring(buf));
	buffer_rewind(buf, 0);
	return out;
}
