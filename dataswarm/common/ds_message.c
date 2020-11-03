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

struct jx * ds_message_standard_response(int64_t id, ds_result_t code, ...)
{
    va_list keys_values;
    va_start(keys_values, code);

    struct jx *params = jx_object(0);

	struct jx *message = jx_objectv(
            "method", jx_string("response"),
            "id",     jx_integer(id),
            "result", jx_integer(code),
            "params", params,
            NULL);

    char *key = va_arg(keys_values, char *);
    while(key) {
        struct jx *value = va_arg(keys_values, struct jx *);
        assert(value);
        jx_insert(params, jx_string(key), value);
        key = va_arg(keys_values, char *);
    }

    if(code!=DS_RESULT_SUCCESS) {
        jx_insert_integer(message, "error", code);
        jx_insert_string(message, "error_string", ds_message_result_string(code));
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

const char *ds_message_result_string(ds_result_t code) {

    const char *result = NULL;
    switch(code) {
        case DS_RESULT_SUCCESS:
            result = "success";
            break;
        case DS_RESULT_BAD_METHOD:
            result = "method does not specify a known message in the given context";
            break;
        case DS_RESULT_BAD_ID:
            result = "method that needs a reply is missing the id field";
            break;
        case DS_RESULT_BAD_PARAMS:
            result = "params keys missing or of incorrect type";
            break;
        case DS_RESULT_NO_SUCH_TASKID:
            result = "requested task-id does not exist";
            break;
        case DS_RESULT_NO_SUCH_BLOBID:
            result = "requested blob-id does not exist";
            break;
        case DS_RESULT_TOO_FULL:
            result = "insufficient resources to complete request";
            break;
        case DS_RESULT_BAD_PERMISSION:
            result = "insufficient privileges to complete request";
            break;
        case DS_RESULT_UNABLE:
            result = "could not complete request for internal reason";
            break;
        case DS_RESULT_PENDING:
            result = "rpc not completed yet.";
            break;
        case DS_RESULT_BAD_STATE:
            result = "cannot take that action in this state.";
            break;
        case DS_RESULT_TASKID_EXISTS:
            result = "attempt to create a task which already exists.";
            break;
        case DS_RESULT_BLOBID_EXISTS:
            result = "attempt to create a blob which already exists.";
            break;
        default:
            result = "unknown result code";
    }

    return result;
}

