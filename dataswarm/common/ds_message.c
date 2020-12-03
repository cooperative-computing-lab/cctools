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

#define JSONRPC_VERSION "2.0"

int ds_bytes_send(struct mq *mq, const char *str, int length)
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

struct jx * ds_message_notification(const char *method, struct jx *params) {
	assert(method);
	assert(!params || jx_istype(params, JX_OBJECT) || jx_istype(params, JX_ARRAY));

	struct jx *out = jx_objectv(
		"jsonrpc", jx_string(JSONRPC_VERSION),
		"method", jx_string(method),
		NULL
	);

	if (params) {
		jx_insert(out, jx_string("params"), params);
	}

	return out;
}

struct jx * ds_message_request(const char *method, struct jx *params) {
	static jx_int_t id = 0;
	struct jx *out = ds_message_notification(method, params);
	jx_insert_integer(out, "id", id++);
	return out;
}

struct jx * ds_message_response(jx_int_t id, ds_result_t code, struct jx *data)
{
	struct jx *message = jx_objectv(
		"jsonrpc", jx_string(JSONRPC_VERSION),
		"id",     jx_integer(id),
		NULL);

	if (code == DS_RESULT_SUCCESS) {
		if (!data) {
			data = jx_integer(code); //XXX or send {} or null to indicate no data?
		}
		jx_insert(message, jx_string("result"), data);
	} else {
		struct jx *err = jx_object(NULL);
		jx_insert_integer(err, "code", code);
		jx_insert_string(err, "message", ds_message_result_string(code));
		if (data) {
			jx_insert(err, jx_string("data"), data);
		}
		jx_insert(message, jx_string("error"), err);
	}

	return message;
}

struct jx * ds_message_task_update( const struct ds_task *t )
{
	return ds_message_notification(
		"task-update",
		jx_objectv(
			"task-id", jx_string(t->taskid),
			"state", jx_integer(t->state),
			"result", jx_integer(t->result),
			NULL)
	);
}

struct jx * ds_message_blob_update( const char *blobid, ds_blob_state_t state )
{
	struct jx *params = jx_object(0);
	jx_insert_string(params,"blob-id",blobid);
	jx_insert_integer(params,"state",state);

	return ds_message_notification("blob-update", params);
}

struct jx *ds_parse_message(buffer_t *buf) {
	assert(buf);
    const char *contents = buffer_tostring(buf);
	debug(D_DATASWARM, "rx: %s", contents);

	struct jx *out = jx_parse_string(contents);
	buffer_rewind(buf, 0);
	return out;
}

static ds_result_t unpack_error(struct jx *err, jx_int_t *code, const char **message, struct jx **data) {
	assert(jx_istype(err, JX_OBJECT));
	assert(message);
	assert(data);

	struct jx *c = NULL;
	struct jx *m = NULL;
	struct jx *d = NULL;

	void *iter = NULL;
	const char *k;
	while ((k = jx_iterate_keys(err, &iter))) {
		if (!strcmp(k, "code")) {
			c = jx_get_value(&iter);
		} else if (!strcmp(k, "message")) {
			m = jx_get_value(&iter);
		} else if (!strcmp(k, "data")) {
			d = jx_get_value(&iter);
		} else {
			return false;
		}
	}

	if (!jx_istype(c, JX_INTEGER)) return DS_RESULT_BAD_MESSAGE;
	if (!jx_istype(m, JX_STRING)) return DS_RESULT_BAD_MESSAGE;

	*code = c->u.integer_value;
	*message = m->u.string_value;
	*data = d;
	return DS_RESULT_SUCCESS;
}

static ds_result_t unpack_rpc(struct jx *msg, struct jx **id, struct jx **method, struct jx **params, struct jx **result, struct jx **error) {
	assert(jx_istype(msg, JX_OBJECT));

	bool has_rpc = false;

	void *iter = NULL;
	const char *k;
	struct jx *v;
	while ((k = jx_iterate_keys(msg, &iter))) {
		if (!strcmp(k, "jsonrpc")) {
			v = jx_get_value(&iter);
			if (!jx_istype(v, JX_STRING)) return DS_RESULT_BAD_MESSAGE;
			if (strcmp(v->u.string_value, JSONRPC_VERSION)) return DS_RESULT_BAD_MESSAGE;
			has_rpc = true;
		} else if (!strcmp(k, "id")) {
			v = jx_get_value(&iter);
			// JSON-RPC also allows string IDs, but we don't do that
			if (!jx_istype(v, JX_INTEGER)) return DS_RESULT_BAD_ID;
			if (!id) return DS_RESULT_BAD_MESSAGE;
			*id = v;
		} else if (!strcmp(k, "method")) {
			v = jx_get_value(&iter);
			if (!jx_istype(v, JX_STRING)) return DS_RESULT_BAD_METHOD;
			if (!method) return DS_RESULT_BAD_MESSAGE;
			*method = v;
		} else if (!strcmp(k, "params")) {
			v = jx_get_value(&iter);
			if (!jx_istype(v, JX_OBJECT) && !jx_istype(v, JX_ARRAY)) return DS_RESULT_BAD_PARAMS;
			if (!params) return DS_RESULT_BAD_MESSAGE;
			*params = v;
		} else if (!strcmp(k, "result")) {
			v = jx_get_value(&iter);
			if (!result) return DS_RESULT_BAD_MESSAGE;
			*result = v;
		} else if (!strcmp(k, "error")) {
			v = jx_get_value(&iter);
			if (!jx_istype(v, JX_OBJECT)) return DS_RESULT_BAD_MESSAGE;
			if (!error) return DS_RESULT_BAD_MESSAGE;
			*error = v;
		} else {
			return DS_RESULT_BAD_MESSAGE;
		}
	}

	if (!has_rpc) return DS_RESULT_BAD_MESSAGE;
	return DS_RESULT_SUCCESS;
}

ds_result_t ds_unpack_notification(struct jx *msg, const char **method, struct jx **params) {
	assert(method);
	assert(params);

	struct jx *m = NULL;
	struct jx *p = NULL;
	ds_result_t rc = unpack_rpc(msg, NULL, &m, &p, NULL, NULL);
	if (rc != DS_RESULT_SUCCESS) return rc;
	if (!m) return DS_RESULT_BAD_MESSAGE;

	*method = m->u.string_value;
	*params = p;
	return DS_RESULT_SUCCESS;
}

ds_result_t ds_unpack_request(struct jx *msg, const char **method, jx_int_t *id, struct jx **params) {
	assert(method);
	assert(id);
	assert(params);

	struct jx *m = NULL;
	struct jx *i = NULL;
	struct jx *p = NULL;
	ds_result_t rc = unpack_rpc(msg, &i, &m, &p, NULL, NULL);
	if (rc != DS_RESULT_SUCCESS) return rc;
	if (!m) return DS_RESULT_BAD_MESSAGE;
	if (!i) return DS_RESULT_BAD_MESSAGE;

	*method = m->u.string_value;
	*id = i->u.integer_value;
	*params = p;
	return DS_RESULT_SUCCESS;
}

ds_result_t ds_unpack_result(struct jx *msg, jx_int_t *id, struct jx **result) {
	assert(id);
	assert(result);

	struct jx *i = NULL;
	struct jx *r = NULL;
	ds_result_t rc = unpack_rpc(msg, &i, NULL, NULL, &r, NULL);
	if (rc != DS_RESULT_SUCCESS) return rc;
	if (!i) return DS_RESULT_BAD_MESSAGE;
	if (!r) return DS_RESULT_BAD_MESSAGE;

	*id = i->u.integer_value;
	*result = r;
	return DS_RESULT_SUCCESS;
}

ds_result_t ds_unpack_error(struct jx *msg, jx_int_t *id, jx_int_t *code, const char **message, struct jx **data) {
	assert(id);
	assert(code);
	assert(message);
	assert(data);

	struct jx *i = NULL;
	struct jx *err = NULL;
	ds_result_t rc = unpack_rpc(msg, &i, NULL, NULL, NULL, &err);
	if (rc != DS_RESULT_SUCCESS) return rc;
	if (!i) return DS_RESULT_BAD_MESSAGE;
	if (!err) return DS_RESULT_BAD_MESSAGE;

	jx_int_t c = 0;
	const char *m = NULL;
	struct jx *d = NULL;
	rc = unpack_error(err, &c, &m, &d);
	if (rc != DS_RESULT_SUCCESS) return rc;

	*id = i->u.integer_value;
	*code = c;
	*message = m;
	*data = d;
	return DS_RESULT_SUCCESS;
}

const char *ds_message_result_string(ds_result_t code) {

    const char *result = NULL;
    switch(code) {
        case DS_RESULT_SUCCESS:
            result = "success";
            break;
        case DS_RESULT_BAD_MESSAGE:
            result = "invalid/malformed RPC message";
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
