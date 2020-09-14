
#include "dataswarm_message.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "debug.h"
#include "jx_print.h"
#include "jx_parse.h"

int dataswarm_message_send(struct link *l, const char *str, int length, time_t stoptime)
{
	char lenstr[16];
	sprintf(lenstr, "%d\n", length);
	int lenstrlen = strlen(lenstr);
	int result = link_write(l, lenstr, lenstrlen, stoptime);
	if(result != lenstrlen)
		return 0;
	debug(D_DATASWARM, "tx: %s", str);
	result = link_write(l, str, length, stoptime);
	return result == length;
}

char *dataswarm_message_recv(struct link *l, time_t stoptime)
{
	char lenstr[16];
	int result = link_readline(l, lenstr, sizeof(lenstr), stoptime);
	if(!result)
		return 0;

	int length = atoi(lenstr);
	char *str = malloc(length + 1);
	result = link_read(l, str, length, stoptime);
	if(result != length) {
		free(str);
		return 0;
	}
	str[length] = 0;
	debug(D_DATASWARM, "rx: %s", str);
	return str;
}

int dataswarm_json_send(struct link *l, struct jx *j, time_t stoptime)
{
	char *str = jx_print_string(j);
	int result = dataswarm_message_send(l, str, strlen(str), stoptime);
	free(str);
	return result;
}

struct jx *dataswarm_json_recv(struct link *l, time_t stoptime)
{
	char *str = dataswarm_message_recv(l, stoptime);
	if(!str)
		return 0;
	struct jx *j = jx_parse_string(str);
	free(str);
	return j;
}

struct jx * dataswarm_message_standard_response( int64_t id, dataswarm_result_t code, struct jx *params )
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

struct jx * dataswarm_message_task_update( const char *taskid, const char *state )
{
	struct jx *params = jx_object(0);
	jx_insert_string(params,"task-id",taskid);
	jx_insert_string(params,"state",state);

	struct jx *message = jx_object(0);
	jx_insert_string(message, "method", "task-update");
	jx_insert(message,jx_string("params"),params);

	return message;
}

struct jx * dataswarm_message_blob_update( const char *blobid, const char *state )
{
	struct jx *params = jx_object(0);
	jx_insert_string(params,"blob-id",blobid);
	jx_insert_string(params,"state",state);

	struct jx *message = jx_object(0);
	jx_insert_string(message, "method", "blob-update");
	jx_insert(message,jx_string("params"),params);

	return message;
}



