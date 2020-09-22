#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <string.h>

#include "mq.h"
#include "xxmalloc.h"
#include "buffer.h"

// 10 MiB (should be bigger than any send/recv buffers)
#define MSG_SIZE 10485760

int main (int argc, char *argv[]) {
	char *string1 = xxmalloc(MSG_SIZE + 1);
	memset(string1, 'a', MSG_SIZE);
	string1[MSG_SIZE] = 0;
	const char *string2 = "test message";

	buffer_t *test1 = xxmalloc(sizeof(*test1));
	buffer_t *test2 = xxmalloc(sizeof(*test2));
	buffer_t got_string;
	buffer_init(test1);
	buffer_init(test2);
	buffer_init(&got_string);
	buffer_putstring(test1, string1);
	buffer_putstring(test2, string2);

	int rc;
	size_t got_length;

	struct mq *server = mq_serve("127.0.0.1", 65000);
	assert(server);

	struct mq_poll *p = mq_poll_create();
	assert(p);
	rc = mq_poll_add(p, server);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 1);
	assert(rc == 0);

	struct mq *client = mq_connect("127.0.0.1", 65000);
	assert(client);

	rc = mq_poll_add(p, client);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 1);
	assert(rc == 1);

	struct mq *conn = mq_accept(server);
	assert(conn);
	rc = mq_poll_add(p, conn);
	assert(rc == 0);

	rc = mq_store_buffer(conn, &got_string, 0);
	assert(rc == 0);

	rc = mq_send_buffer(client, test1, 0);
	assert(rc != -1);

	rc = mq_send_buffer(client, test2, 0);
	assert(rc != -1);

	rc = mq_poll_wait(p, time(NULL) + 5);
	assert(rc == 1);

	rc = mq_recv(conn, &got_length);
	assert(rc == MQ_MSG_BUFFER);
	assert(got_length == MSG_SIZE);
	assert(!memcmp(string1, buffer_tostring(&got_string), MSG_SIZE));

	rc = mq_recv(conn, NULL);
	assert(rc == MQ_MSG_NONE);

	rc = mq_store_buffer(conn, &got_string, 0);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 1);
	assert(rc == 1);

	rc = mq_recv(conn, NULL);
	assert(rc == MQ_MSG_BUFFER);
	assert(!strcmp(string2, buffer_tostring(&got_string)));

	buffer_free(&got_string);
	mq_close(client);
	mq_close(conn);
	mq_close(server);
	mq_poll_delete(p);
	free(string1);
	return 0;
}
