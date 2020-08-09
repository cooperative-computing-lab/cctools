#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="mq_poll.test"

prepare()
{
	${CC} -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <string.h>

#include "mq.h"

// 10 MiB (should be bigger than any send/recv buffers)
#define MSG_SIZE 10485760

int main (int argc, char *argv[]) {
	char *test1 = malloc(MSG_SIZE);
	const char *test2 = "test message";
	assert(test1);
	memset(test1, 'a', MSG_SIZE);

	int rc;
	struct mq_msg *msg;
	char *got_string;
	size_t len;

	struct mq *server = mq_serve("127.0.0.1", 65000);
	assert(server);

	struct mq_poll *p = mq_poll_create();
	assert(p);
	rc = mq_poll_add(p, server, NULL);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 1);
	assert(rc == 0);

	struct mq *client = mq_connect("127.0.0.1", 65000);
	assert(client);

	rc = mq_poll_add(p, client, NULL);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 1);
	assert(rc == 1);

	struct mq *conn = mq_accept(server);
	assert(conn);
	rc = mq_poll_add(p, conn, NULL);
	assert(rc == 0);

	msg = mq_wrap_buffer(test1, MSG_SIZE);
	assert(msg);
	mq_send(client, msg);

	msg = mq_wrap_buffer(test2, strlen(test2));
	assert(msg);
	mq_send(client, msg);

	rc = mq_poll_wait(p, time(NULL) + 5);
	assert(rc == 1);

	msg = mq_recv(conn);
	assert(msg);

	got_string = mq_unwrap_buffer(msg, &len);
	assert(got_string);
	assert(len == MSG_SIZE);
	assert(!memcmp(test1, got_string, MSG_SIZE));
	free(got_string);

	msg = mq_recv(conn);
	assert(!msg);

	rc = mq_poll_wait(p, time(NULL) + 1);
	assert(rc == 1);

	msg = mq_recv(conn);
	assert(msg);

	got_string = mq_unwrap_buffer(msg, NULL);
	assert(got_string);
	assert(!strcmp(test2, got_string));
	free(got_string);

	mq_close(client);
	mq_close(conn);
	mq_close(server);
	mq_poll_delete(p);
	free(test1);
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
