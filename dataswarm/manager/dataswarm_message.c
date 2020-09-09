
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

struct jx *dataswarm_message_error_response(enum dataswarm_message_error code, struct jx *evidence)
{
	struct jx *response = jx_object(0);

	jx_insert_integer(response, "error", code);

	if(evidence) {
		jx_insert(response, jx_string("result"), evidence);
	}

	return response;
}

struct jx *dataswarm_message_state_response(const char *state, const char *reason)
{
	struct jx *response = jx_object(0);
	struct jx *result = jx_object(0);

	jx_insert(response, jx_string("error"), jx_null());
	jx_insert(response, jx_string("result"), result);

	jx_insert_string(result, "state", state);

	if(reason) {
		jx_insert_string(result, "reason", reason);
	}

	return response;
}
