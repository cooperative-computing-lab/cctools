/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <endian.h>
#include <fcntl.h>
#include <signal.h>
#include <poll.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "mq.h"
#include "list.h"
#include "link.h"
#include "xxmalloc.h"
#include "debug.h"

#define HDR_SIZE sizeof(struct mq_msg_header)
#define HDR_MAGIC "DSmsg"

enum mq_msg_type {
	MQ_MSG_BUFFER = 0,
};

struct mq_msg_header {
	char magic[5];
	char pad[2]; // necessary for alignment
	uint8_t type;
	uint64_t length;
};

struct mq_msg {
	enum mq_msg_type type;
	size_t len;
	void *buf;
	struct mq_msg_header hdr;
	bool parsed_header;
	size_t hdr_pos;
	size_t buf_pos;
};

struct mq {
	struct link *link;
	struct list *send;
	struct mq_msg *recv;
	struct mq_msg *send_buf;
	struct mq_msg *recv_buf;
};

static bool errno_is_temporary(void) {
	if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN || errno == EINPROGRESS || errno == EALREADY || errno == EISCONN) {
		return true;
	} else {
		return false;
	}
}

static struct mq_msg *msg_create (void) {
	return xxcalloc(1, sizeof(struct mq_msg));
}

static void delete_msg(struct mq_msg *msg) {
	if (!msg) return;
	free(msg->buf);
	free(msg);
}

static void write_header(struct mq_msg *msg) {
	assert(msg);
	memcpy(msg->hdr.magic, HDR_MAGIC, sizeof(msg->hdr.magic));
	msg->hdr.type = msg->type;
	msg->hdr.length = htobe64(msg->len);
}

static struct mq *mq_create(void) {
	struct mq *out = xxcalloc(1, sizeof(*out));
	out->send = list_create();
	return out;
}

void mq_close(struct mq *mq) {
	if (!mq) return;

	link_close(mq->link);
	mq_msg_delete(mq->send_buf);
	mq_msg_delete(mq->recv_buf);
	mq_msg_delete(mq->recv);

	struct list_cursor *cur = list_cursor_create(mq->send);
	list_seek(cur, 0);
	for (struct mq_msg *msg; list_get(cur, (void **) &msg); list_next(cur)) {
		mq_msg_delete(msg);
	}
	list_cursor_destroy(cur);
	list_delete(mq->send);

	free(mq);
}

void mq_msg_delete(struct mq_msg *msg) {
	if (!msg) return;
	// once blobs are implemented, check for on-disk stuff to delete
	delete_msg(msg);
}

static int flush_send(struct mq *mq) {
	assert(mq);

	int socket = link_fd(mq->link);

	while (true) {
		if (!mq->send_buf) {
			mq->send_buf = list_pop_head(mq->send);
			if (!mq->send_buf) return 0;
			write_header(mq->send_buf);
		}
		struct mq_msg *snd = mq->send_buf;

		if (snd->hdr_pos < HDR_SIZE) {
			ssize_t s = send(socket, &snd->hdr, HDR_SIZE - snd->hdr_pos, 0);
			if (s == -1 && errno_is_temporary()) {
				return 0;
			} else if (s <= 0) {
				return -1;
			}
			snd->hdr_pos += s;
		} else if (snd->buf_pos < snd->len) {
			ssize_t s = send(socket, snd->buf, snd->len - snd->buf_pos, 0);
			if (s == -1 && errno_is_temporary()) {
				return 0;
			} else if (s <= 0) {
				return -1;
			}
			snd->buf_pos += s;
		} else {
			delete_msg(snd);
			mq->send_buf = NULL;
		}
	}
}

static int flush_recv(struct mq *mq) {
	assert(mq);

	int socket = link_fd(mq->link);

	while (true) {
		if (mq->recv) {
			return 0;
		}

		if (!mq->recv_buf) {
			mq->recv_buf = msg_create();
		}
		struct mq_msg *rcv = mq->recv_buf;

		if (rcv->hdr_pos < HDR_SIZE) {
			ssize_t r = recv(socket, &rcv->hdr, HDR_SIZE - rcv->hdr_pos, 0);
			if (r == -1 && errno_is_temporary()) {
				return 0;
			} else if (r <= 0) {
				return -1;;
			}
			rcv->hdr_pos += r;
		} else if (!rcv->parsed_header) {
			if (memcmp(rcv->hdr.magic, HDR_MAGIC, sizeof(rcv->hdr.magic))) {
				return -1;
			}
			rcv->type = rcv->hdr.type;
			rcv->len = be64toh(rcv->hdr.length);
			rcv->buf = xxmalloc(rcv->len + 1);
			((char *) rcv->buf)[rcv->len] = 0;
			rcv->parsed_header = true;
		} else if (rcv->buf_pos < rcv->len) {
			ssize_t r = recv(socket, rcv->buf, rcv->len - rcv->buf_pos, 0);
			if (r == -1 && errno_is_temporary()) {
				return 0;
			} else if (r <= 0) {
				return -1;;
			}
			rcv->buf_pos += r;
		} else {
			// parse JX, etc.
			mq->recv = mq->recv_buf;
			mq->recv_buf = NULL;
		}
	}
}

struct mq_msg *mq_wrap_buffer(const void *b, size_t size) {
	assert(b);
	struct mq_msg *out = msg_create();
	out->type = MQ_MSG_BUFFER;
	out->len = size;
	out->buf = xxmalloc(size);
	memcpy(out->buf, b, size);
	return out;
}

void *mq_unwrap_buffer(struct mq_msg *msg) {
	assert(msg);
	if (msg->type != MQ_MSG_BUFFER) return NULL;
	void *out = msg->buf;
	free(msg);
	return out;
}

void mq_send(struct mq *mq, struct mq_msg *msg) {
	assert(mq);
	assert(msg);
	list_push_tail(mq->send, msg);
}

struct mq_msg *mq_recv(struct mq *mq) {
	assert(mq);
	struct mq_msg *out = mq->recv;
	mq->recv = NULL;
	return out;
}

struct mq *mq_serve(const char *addr, int port) {
	struct link *link = link_serve_address(addr, port);
	if (!link) return NULL;
	struct mq *out = mq_create();
	out->link = link;
	return out;
}

struct mq *mq_connect(const char *addr, int port) {
	struct link *link = link_connect(addr, port, LINK_NOWAIT);
	if (!link) return NULL;
	struct mq *out = mq_create();
	out->link = link;
	return out;
}

struct mq *mq_accept(struct mq *server) {
	assert(server);
	struct link *link = link_accept(server->link, LINK_NOWAIT);
	if (!link) return NULL;
	struct mq *out = mq_create();
	out->link = link;
	return out;
}

int mq_wait(struct mq *mq, time_t stoptime) {
	assert(mq);

	int rc;
	struct pollfd pfd;
	struct timespec stop;
	sigset_t mask;
	pfd.fd = link_fd(mq->link);

	do {
		if (flush_send(mq) == -1) return -1;
		if (flush_recv(mq) == -1) return -1;
		if (mq->recv) return 1;

		stop.tv_nsec = 0;
		stop.tv_sec = stoptime - time(NULL);

		pfd.events = POLLIN;
		if (mq->send_buf || list_length(mq->send)) {
			pfd.events |= POLLOUT;
		}

		sigemptyset(&mask);
	} while ((rc = ppoll(&pfd, 1, &stop, &mask)) > 0);

	if (rc == 0 || (rc == -1 && errno == EINTR)) {
		return 0;
	} else {
		return -1;
	}
}
