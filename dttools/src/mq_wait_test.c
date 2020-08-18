#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <string.h>

#include "mq.h"

int main (int argc, char *argv[]) {
	const char *test1 = "test message";
	const char *test2 = "another one";

	int rc;
	struct mq_msg *msg;
	char *got_string;

	struct mq *server = mq_serve("127.0.0.1", 65000);
	assert(server);
	struct mq *client = mq_connect("127.0.0.1", 65000);
	assert(client);
	struct mq *conn = mq_accept(server);
	assert(!conn);

	msg = mq_wrap_buffer(test1, strlen(test1));
	assert(msg);
	rc = mq_send(client, msg);
	assert(rc != -1);

	msg = mq_wrap_buffer(test2, strlen(test2));
	assert(msg);
	rc = mq_send(client, msg);
	assert(rc != -1);

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

	got_string = mq_unwrap_buffer(msg, NULL);
	assert(got_string);
	assert(!strcmp(test1, got_string));
	free(got_string);

	msg = mq_recv(conn);
	assert(!msg);

	rc = mq_wait(conn, time(NULL) + 1);
	assert(rc != -1);

	msg = mq_recv(conn);
	assert(msg);

	got_string = mq_unwrap_buffer(msg, NULL);
	assert(got_string);
	assert(!strcmp(test2, got_string));
	free(got_string);

	mq_close(client);
	mq_close(conn);
	mq_close(server);
	return 0;
}
