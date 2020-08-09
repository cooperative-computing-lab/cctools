#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="mq_json.test"

prepare()
{
	${CC} -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <string.h>

#include "mq.h"
#include "jx.h"
#include "jx_parse.h"


int main (int argc, char *argv[]) {
	struct jx *test = jx_parse_string("{\"test\": \"message\"}");

	int rc;
	struct mq_msg *msg;
	struct jx *got_json;

	struct mq *server = mq_serve("127.0.0.1", 65000);
	assert(server);
	struct mq *client = mq_connect("127.0.0.1", 65000);
	assert(client);
	struct mq *conn = mq_accept(server);
	assert(!conn);

	msg = mq_wrap_json(test);
	assert(msg);
	mq_send(client, msg);

	rc = mq_wait(server, time(NULL) + 1);
	assert(rc != -1);
	conn = mq_accept(server);
	assert(conn);

	rc = mq_wait(client, time(NULL) + 1);
	assert(rc != -1);
	rc = mq_wait(conn, time(NULL) + 1);
	assert(rc != -1);

	msg = mq_recv(conn);
	assert(msg);

	got_json = mq_unwrap_json(msg);
	assert(got_json);
	assert(jx_equals(test, got_json));
	jx_delete(got_json);

	jx_delete(test);
	mq_close(client);
	mq_close(conn);
	mq_close(server);
	return 0;
}
EOF
	return $?
}

run()
{
	./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"
