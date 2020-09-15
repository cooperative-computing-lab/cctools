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

	buffer_t *test1 = xxmalloc(sizeof(*test1));
	buffer_init(test1);
	buffer_putstring(test1, string1);

	int rc;
	buffer_t got_string;
	buffer_init(&got_string);

	struct mq *server = mq_serve("127.0.0.1", 65000);
	assert(server);
	struct mq *client = mq_connect("127.0.0.1", 65000);
	assert(client);

	rc = mq_send_buffer(client, test1);
	assert(rc != -1);

	rc = mq_wait(server, time(NULL) + 1);
	assert(rc != -1);
	struct mq *conn = mq_accept(server);
	assert(conn);

	rc = mq_store_buffer(conn, &got_string);
	assert(rc != -1);

	rc = mq_wait(client, time(NULL) + 1);
	assert(rc != -1);
	rc = mq_wait(conn, time(NULL) + 1);
	assert(rc != -1);

	rc = mq_recv(conn, NULL);
	assert(rc == MQ_MSG_BUFFER);

	assert(!strcmp(string1, buffer_tostring(&got_string)));
	buffer_free(&got_string);

	mq_close(client);
	mq_close(conn);
	mq_close(server);
	return 0;
}
