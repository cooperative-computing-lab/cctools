#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "mq.h"
#include "buffer.h"
#include "xxmalloc.h"

int main (int argc, char *argv[]) {
	const char *string1 = "test message";

	int srcfd = open(argv[0], O_RDONLY);
	assert(srcfd != -1);
	int dstfd = open(argv[1], O_WRONLY|O_CREAT|O_TRUNC, 0777);
	assert(dstfd != -1);

	buffer_t *test1 = xxmalloc(sizeof(*test1));
	buffer_init(test1);
	buffer_putstring(test1, string1);
	buffer_t *got = xxcalloc(1, sizeof(*got));
	buffer_init(got);

	size_t got_len;
	int rc;
	buffer_t got_string;
	buffer_init(&got_string);

	struct mq *server = mq_serve("127.0.0.1", 65000);
	assert(server);
	struct mq *client = mq_connect("127.0.0.1", 65000);
	assert(client);

	rc = mq_send_buffer(client, test1, 0);
	assert(rc != -1);

	rc = mq_wait(server, time(NULL) + 1);
	assert(rc != -1);
	struct mq *conn = mq_accept(server);
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

	struct mq_poll *p = mq_poll_create();
	assert(p);
	rc = mq_poll_add(p, conn);
	assert(rc == 0);
	rc = mq_poll_add(p, client);
	assert(rc == 0);

	rc = mq_send_fd(conn, srcfd, 0);
	assert(rc == 0);
	rc = mq_store_fd(client, dstfd, 0);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 5);
	assert(rc == 1);
	rc = mq_recv(client, NULL);
	assert(rc == MQ_MSG_FD);

	srcfd = open(argv[0], O_RDONLY);
	assert(srcfd != -1);
	dstfd = open(argv[2], O_WRONLY|O_CREAT|O_TRUNC, 0777);
	assert(dstfd != -1);

	rc = mq_send_fd(client, srcfd, 0);
	assert(rc == 0);
	rc = mq_store_buffer(conn, got, 0);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 5);
	assert(rc == 1);
	rc = mq_recv(conn, NULL);
	assert(rc == MQ_MSG_BUFFER);

	rc = mq_send_buffer(client, got, 0);
	assert(rc != -1);
	rc = mq_store_fd(conn, dstfd, 0);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 5);
	assert(rc == 1);
	rc = mq_recv(conn, NULL);
	assert(rc == MQ_MSG_FD);

	srcfd = open(argv[3], O_RDONLY);
	assert(srcfd != -1);

	rc = mq_send_fd(conn, srcfd, 0);
	assert(rc == 0);

	rc = mq_store_buffer(client, &got_string, 0);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 15);
	assert(rc == 1);
	rc = mq_recv(client, &got_len);
	assert(rc == MQ_MSG_BUFFER);
	assert(got_len == 10);

	srcfd = open(argv[0], O_RDONLY);
	assert(srcfd != -1);

	rc = mq_send_fd(conn, srcfd, 256);
	assert(rc == 0);

	rc = mq_store_buffer(client, &got_string, 0);
	assert(rc == 0);

	rc = mq_poll_wait(p, time(NULL) + 5);
	assert(rc == 1);
	rc = mq_recv(client, &got_len);
	assert(rc == MQ_MSG_BUFFER);
	assert(got_len == 256);

	buffer_free(&got_string);
	mq_poll_delete(p);
	mq_close(client);
	mq_close(conn);
	mq_close(server);
	return 0;
}
