#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <string.h>

#include "mq.h"
#include "buffer.h"
#include "xxmalloc.h"

int main (int argc, char *argv[]) {
	const char *string1 = "test message";
	const char *string2 = "another one";

	buffer_t *test1 = xxmalloc(sizeof(*test1));
	buffer_t *test2 = xxmalloc(sizeof(*test2));
	buffer_t got_string;
	buffer_init(test1);
	buffer_init(test2);
	buffer_init(&got_string);
	buffer_putstring(test1, string1);
	buffer_putstring(test2, string2);

	int rc;

	struct mq *server = mq_serve("127.0.0.1", 65000);
	assert(server);
	struct mq *client = mq_connect("127.0.0.1", 65000);
	assert(client);
	struct mq *conn = mq_accept(server);
	assert(!conn);

	rc = mq_send_buffer(client, test1, 0);
	assert(rc != -1);

	rc = mq_send_buffer(client, test2, 0);
	assert(rc != -1);

	rc = mq_wait(server, time(NULL) + 1);
	assert(rc != -1);
	conn = mq_accept(server);
	assert(conn);

	rc = mq_store_buffer(conn, &got_string, 0);
	assert(rc == 0);

	rc = mq_wait(client, time(NULL) + 1);
	assert(rc != -1);
	rc = mq_wait(conn, time(NULL) + 1);
	assert(rc != -1);

	rc = mq_recv(conn, NULL);
	assert(rc == MQ_MSG_BUFFER);
	assert(!strcmp(string1, buffer_tostring(&got_string)));

	rc = mq_store_buffer(conn, &got_string, 0);
	assert(rc == 0);

	rc = mq_recv(conn, NULL);
	assert(rc == MQ_MSG_NONE);

	rc = mq_wait(conn, time(NULL) + 1);
	assert(rc != -1);

	rc = mq_recv(conn, NULL);
	assert(rc == MQ_MSG_BUFFER);
	assert(!strcmp(string2, buffer_tostring(&got_string)));

	buffer_free(&got_string);
	mq_close(client);
	mq_close(conn);
	mq_close(server);
	return 0;
}
