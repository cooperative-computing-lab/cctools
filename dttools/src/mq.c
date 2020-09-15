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
#include <fcntl.h>
#include <sys/socket.h>

#include "mq.h"
#include "buffer.h"
#include "list.h"
#include "itable.h"
#include "set.h"
#include "link.h"
#include "xxmalloc.h"
#include "debug.h"
#include "ppoll_compat.h"
#include "cctools_endian.h"


#define HDR_SIZE (sizeof(struct mq_msg) - offsetof(struct mq_msg, magic))
#define HDR_MAGIC "MQmsg"

#define HDR_MSG_CONT 0
#define HDR_MSG_START (1<<0)
#define HDR_MSG_END (1<<1)
#define HDR_MSG_SINGLE (HDR_MSG_START | HDR_MSG_END)

#define MQ_PIPEBUF_SIZE (1<<16)

enum mq_socket {
	MQ_SOCKET_SERVER,
	MQ_SOCKET_INPROGRESS,
	MQ_SOCKET_CONNECTED,
	MQ_SOCKET_ERROR,
};

struct mq_msg {
	size_t len;
	bool parsed_header;
	ptrdiff_t hdr_pos;
	ptrdiff_t buf_pos;
	mq_msg_t storage;
	buffer_t *buffer;
	int pipefd;
	bool buffering;
	bool seen_initial;

	/* Here be dragons!
	 *
	 * Since we need to be able allow send/recv to be interrupted at any time
	 * (even in the middle of an int), we can't rely on reading/writing
	 * multi-byte header fields all in one go. The following fields are
	 * arranged to match the wire format of the header. DO NOT add additional
	 * struct fields below here! (If you do change the header format, be sure
	 * to update the sanity check in msg_create.) First is some padding
	 * (void pointer is self-aligned) so we don't need to worry too much
	 * about the alignment of the earlier fields. Then the actual header
	 * follows. Note that the length in the header needs to be in network
	 * byte order, so that gets stored separately.
	 *
	 *  0    1    2    3    4    5    6    7
	 * +----+----+----+----+----+----+----+----+
	 * |           magic        |   pad   |type|
	 * +----+----+----+----+----+----+----+----+
	 * |                 length                |
	 * +----+----+----+----+----+----+----+----+
	 */
	void *pad1;

	char magic[5];
	char pad2[2]; // necessary for alignment, should be 0
	uint8_t type;
	uint64_t hdr_len;
};

struct mq {
	struct link *link;
	enum mq_socket state;
	struct mq *acc;
	struct list *send;
	int err;
	struct mq_msg *recv;
	struct mq_msg *sending;
	struct mq_msg *recving;
	struct mq_poll *poll_group;
};

struct mq_poll {
	struct itable *members;
	struct set *acceptable;
	struct set *readable;
	struct set *error;
};

static int set_nonblocking (int fd) {
	int out = fcntl(fd, F_GETFL);
	if (out < 0) return out;
	out |= O_NONBLOCK;
	out = fcntl(fd, F_SETFL, out);
	if(out < 0) return out;
	return 0;
}

static struct mq_msg *msg_create (void) {
	// sanity check
	assert(HDR_SIZE == 16);
	struct mq_msg *out = xxcalloc(1, sizeof(struct mq_msg));
	memcpy(out->magic, HDR_MAGIC, sizeof(out->magic));
	out->pipefd = -1;
	return out;
}

static void mq_msg_delete(struct mq_msg *msg) {
	if (!msg) return;
	switch (msg->storage) {
		case MQ_MSG_NONE:
		case MQ_MSG_BUFFER:
			break;
		case MQ_MSG_FD:
			if (msg->pipefd >= 0) close(msg->pipefd);
			// falls through
		case MQ_MSG_NEWBUFFER:
			buffer_free(msg->buffer);
			free(msg->buffer);
			break;
	}
	free(msg);
}

static void mq_die(struct mq *mq, int err) {
	assert(mq);
	mq->state = MQ_SOCKET_ERROR;
	mq->err = err;

	mq_close(mq->acc);
	mq_msg_delete(mq->sending);
	mq_msg_delete(mq->recving);
	mq_msg_delete(mq->recv);

	struct list_cursor *cur = list_cursor_create(mq->send);
	list_seek(cur, 0);
	for (struct mq_msg *msg; list_get(cur, (void **) &msg); list_next(cur)) {
		mq_msg_delete(msg);
	}
	list_cursor_destroy(cur);

	if (mq->poll_group) {
		set_remove(mq->poll_group->acceptable, mq);
		set_remove(mq->poll_group->readable, mq);
		if (err == 0) {
			set_remove(mq->poll_group->error, mq);
		} else {
			set_insert(mq->poll_group->error, mq);
		}
	}
}

static struct mq *mq_create(enum mq_socket state, struct link *link) {
	struct mq *out = xxcalloc(1, sizeof(*out));
	out->send = list_create();
	out->state = state;
	out->link = link;
	return out;
}

void mq_close(struct mq *mq) {
	if (!mq) return;

	mq_die(mq, 0);
	if (mq->poll_group) {
		itable_remove(mq->poll_group->members, (uintptr_t) mq);
	}
	link_close(mq->link);
	list_delete(mq->send);
	free(mq);
}

int mq_geterror(struct mq *mq) {
	assert(mq);
	if (mq->state != MQ_SOCKET_ERROR) {
		return 0;
	} else {
		return mq->err;
	}
}

static int validate_header(struct mq_msg *msg) {
	assert(msg);
	errno = EBADF;

	if (memcmp(msg->magic, HDR_MAGIC, sizeof(msg->magic))) {
		return -1;
	}
	if (memcmp(msg->pad2, "\x00\x00", sizeof(msg->pad2))) {
		return -1;
	}
	if (msg->type>>2) {
			return -1;
	}
	if (!!msg->seen_initial == !!(msg->type & HDR_MSG_START)) {
		return -1;
	}

	return 0;
}

static int flush_send(struct mq *mq) {
	assert(mq);

	int socket = link_fd(mq->link);

	while (true) {
		if (!mq->sending) {
			mq->sending = list_pop_head(mq->send);
		}
		struct mq_msg *snd = mq->sending;
		if (!snd) return 0;

		if (snd->buffering) {
			// make sure the cast below won't overflow
			assert(snd->len < PTRDIFF_MAX);
			if (snd->buf_pos < (ptrdiff_t) snd->len) {
				ssize_t rc = read(snd->pipefd,
					(char *) buffer_tostring(snd->buffer) + snd->buf_pos,
					snd->len - snd->buf_pos);
				if (rc == -1 && errno_is_temporary(errno)) {
					return 0;
				} else if (rc == 0) {
					snd->len = snd->buf_pos;
					snd->hdr_len = htonll(snd->len);
				} else if (rc < 0) {
					return -1;
				}
				snd->buf_pos += rc;
				continue;
			} else {
				snd->buffering = false;
				snd->buf_pos = 0;
				snd->hdr_pos = 0;
				if (snd->len < MQ_PIPEBUF_SIZE) {
					snd->type |= HDR_MSG_END;
				}
				continue;
			}
		}

		// make sure the cast below won't overflow
		assert(HDR_SIZE < PTRDIFF_MAX);
		assert(snd->len < PTRDIFF_MAX);
		if (snd->hdr_pos < (ptrdiff_t) HDR_SIZE) {
			ssize_t rc = send(socket, &snd->magic + snd->hdr_pos,
					HDR_SIZE - snd->hdr_pos, 0);
			if (rc == -1 && errno_is_temporary(errno)) {
				return 0;
			} else if (rc <= 0) {
				return -1;
			}
			snd->hdr_pos += rc;
			continue;
		} else if (snd->buf_pos < (ptrdiff_t) snd->len) {
			ssize_t rc = send(socket,
				buffer_tostring(snd->buffer) + snd->buf_pos,
				snd->len - snd->buf_pos, 0);
			if (rc == -1 && errno_is_temporary(errno)) {
				return 0;
			} else if (rc <= 0) {
				return -1;
			}
			snd->buf_pos += rc;
			continue;
		} else {
			if (snd->type & HDR_MSG_END) {
				mq_msg_delete(snd);
				mq->sending = NULL;
			} else {
				snd->buffering = true;
				snd->buf_pos = 0;
				snd->type = HDR_MSG_CONT;
			}
			continue;
		}
	}
}

static int flush_recv(struct mq *mq) {
	assert(mq);

	int socket = link_fd(mq->link);

	while (!mq->recv) {
		if (!mq->recving) {
			mq->recving = msg_create();
			mq->recving->buffer = xxcalloc(1, sizeof(*mq->recving->buffer));
			buffer_init(mq->recving->buffer);
			buffer_abortonfailure(mq->recving->buffer, true);
			mq->recving->storage = MQ_MSG_NEWBUFFER;
		}
		struct mq_msg *rcv = mq->recving;

		if (!rcv->buffering) {
			// make sure the cast below won't overflow
			assert(HDR_SIZE < PTRDIFF_MAX);
			assert(rcv->len < PTRDIFF_MAX);
			if (rcv->hdr_pos < (ptrdiff_t) HDR_SIZE) {
				ssize_t rc = recv(socket, &rcv->magic + rcv->hdr_pos,
						HDR_SIZE - rcv->hdr_pos, 0);
				if (rc == -1 && errno_is_temporary(errno)) {
					return 0;
				} else if (rc <= 0) {
					return -1;;
				}
				rcv->hdr_pos += rc;
				continue;
			} else if (!rcv->parsed_header) {
				rcv->buf_pos = rcv->len;
				// check overflow
				assert(rcv->len + ntohll(rcv->hdr_len) >= rcv->len);
				rcv->len += ntohll(rcv->hdr_len);
				if (validate_header(rcv) == -1) return -1;
				buffer_seek(rcv->buffer, rcv->len);
				rcv->parsed_header = true;
				continue;
			} else if (rcv->buf_pos < (ptrdiff_t) rcv->len) {
				ssize_t rc = recv(socket,
					(char *) buffer_tostring(rcv->buffer) + rcv->buf_pos,
					rcv->len - rcv->buf_pos, 0);

				if (rc == -1 && errno_is_temporary(errno)) {
					return 0;
				} else if (rc <= 0) {
					return -1;;
				}
				rcv->buf_pos += rc;
				continue;
			} else {
				rcv->seen_initial = true;
				rcv->buffering = true;
				rcv->buf_pos = 0;
				rcv->hdr_pos = 0;
				rcv->parsed_header = false;
				continue;
			}
		}

		if (rcv->storage == MQ_MSG_FD) {
			// make sure the cast below won't overflow
			assert(rcv->len < PTRDIFF_MAX);
			if (rcv->buf_pos < (ptrdiff_t) rcv->len) {
				ssize_t rc = write(rcv->pipefd,
					(char *) buffer_tostring(rcv->buffer) + rcv->buf_pos,
					rcv->len - rcv->buf_pos);
				if (rc == -1 && errno_is_temporary(errno)) {
					return 0;
				} else if (rc <= 0) {
					return -1;
				}
				rcv->buf_pos += rc;
				continue;
			} else {
				rcv->len = 0;
			}
		}
		rcv->buffering = false;
		if (rcv->type & HDR_MSG_END) {
			mq->recv = rcv;
			mq->recving = NULL;
		}
	}
	return 0;
}

static short poll_events(struct mq *mq) {
	assert(mq);

	short out = 0;

	switch (mq->state) {
		case MQ_SOCKET_INPROGRESS:
			out |= POLLOUT;
			break;
		case MQ_SOCKET_CONNECTED:
			if (mq->sending || list_length(mq->send)) {
				out |= POLLOUT;
			}
			// falls through
		case MQ_SOCKET_SERVER:
			if (!mq->acc && !mq->recv) {
				out |= POLLIN;
			}
			break;
		case MQ_SOCKET_ERROR:
			break;
	}
	return out;
}

static void update_poll_group(struct mq *mq) {
	assert(mq);

	if (!mq->poll_group) return;
	if (mq->state == MQ_SOCKET_ERROR) {
		set_insert(mq->poll_group->error, mq);
	}
	if (mq->recv) {
		set_insert(mq->poll_group->readable, mq);
	}
	if (mq->acc) {
		set_insert(mq->poll_group->acceptable, mq);
	}
}

static int handle_revents(struct pollfd *pfd, struct mq *mq) {
	assert(pfd);
	assert(mq);

	int rc = 0;
	int err;
	socklen_t size = sizeof(err);

	switch (mq->state) {
		case MQ_SOCKET_ERROR:
			break;
		case MQ_SOCKET_INPROGRESS:
			if (pfd->revents & POLLOUT) {
				rc = getsockopt(link_fd(mq->link), SOL_SOCKET, SO_ERROR,
						&err, &size);
				assert(rc == 0);
				if (err == 0) {
					mq->state = MQ_SOCKET_CONNECTED;
				} else {
					mq_die(mq, err);
				}
			}
			break;
		case MQ_SOCKET_CONNECTED:
			if (pfd->revents & POLLOUT) {
				rc = flush_send(mq);
			}
			if (rc == -1) {
				mq_die(mq, errno);
				goto DONE;
			}
			if (pfd->revents & POLLIN) {
				rc = flush_recv(mq);
			}
			if (rc == -1) {
				mq_die(mq, errno);
				goto DONE;
			}
			break;
		case MQ_SOCKET_SERVER:
			if (pfd->revents & POLLIN) {
				struct link *link = link_accept(mq->link, LINK_NOWAIT);
				// If the server socket polls readable,
				// this should never block.
				assert(link);
				// Should only poll on read if accept slot is free
				assert(!mq->acc);
				struct mq *out = mq_create(MQ_SOCKET_CONNECTED, link);
				mq->acc = out;
			}
			break;
	}

DONE:
	update_poll_group(mq);
	return rc;
}

struct mq *mq_serve(const char *addr, int port) {
	struct link *link = link_serve_address(addr, port);
	if (!link) return NULL;
	struct mq *out = mq_create(MQ_SOCKET_SERVER, link);
	return out;
}

struct mq *mq_connect(const char *addr, int port) {
	struct link *link = link_connect(addr, port, LINK_NOWAIT);
	if (!link) return NULL;
	struct mq *out = mq_create(MQ_SOCKET_INPROGRESS, link);
	return out;
}

struct mq *mq_accept(struct mq *mq) {
	assert(mq);
	struct mq *out = mq->acc;
	mq->acc = NULL;
	if (mq->poll_group) {
		set_remove(mq->poll_group->acceptable, mq);
	}
	return out;
}

int mq_wait(struct mq *mq, time_t stoptime) {
	assert(mq);

	int rc;
	struct pollfd pfd;
	pfd.fd = link_fd(mq->link);
	pfd.revents = 0;

	do {
		// NB: we're using revents from the *previous* iteration
		if (handle_revents(&pfd, mq) == -1) {
			return -1;
		}
		pfd.events = poll_events(mq);

		if (mq->recv || mq->acc) {
			return 1;
		}
	} while ((rc = ppoll_compat(&pfd, 1, stoptime)) > 0);

	if (rc == 0 || (rc == -1 && errno == EINTR)) {
		return 0;
	} else {
		return -1;
	}
}

struct mq_poll *mq_poll_create(void) {
	struct mq_poll *out = xxcalloc(1, sizeof(*out));
	out->members = itable_create(0);
	out->acceptable = set_create(0);
	out->readable = set_create(0);
	out->error = set_create(0);
	return out;
}

void mq_poll_delete(struct mq_poll *p) {
	if (!p) return;

	uint64_t key;
	uintptr_t ptr;
	void *value;
	itable_firstkey(p->members);
	while (itable_nextkey(p->members, &key, &value)) {
		ptr = key;
		struct mq *mq = (struct mq *) ptr;
		mq->poll_group = NULL;
	}
	itable_delete(p->members);
	set_delete(p->readable);
	set_delete(p->acceptable);
	set_delete(p->error);
	free(p);
}

int mq_poll_add(struct mq_poll *p, struct mq *mq, void *tag) {
	assert(p);
	assert(mq);

	if (mq->poll_group == p) {
		errno = EEXIST;
		return -1;
	}
	if (mq->poll_group) {
		errno = EINVAL;
		return -1;
	}

	if (!tag) tag = mq;
	mq->poll_group = p;
	itable_insert(p->members, (uintptr_t) mq, tag);

	return 0;
}

int mq_poll_rm(struct mq_poll *p, struct mq *mq) {
	assert(p);
	assert(mq);

	if (mq->poll_group != p) {
		errno = ENOENT;
		return -1;
	}
	mq->poll_group = NULL;
	itable_remove(p->members, (uintptr_t) mq);
	set_remove(p->acceptable, mq);
	set_remove(p->readable, mq);
	set_remove(p->error, mq);

	return 0;
}

void *mq_poll_acceptable(struct mq_poll *p) {
	assert(p);

	set_first_element(p->acceptable);
	struct mq *mq = set_next_element(p->acceptable);
	assert(mq);
	void *tag = itable_lookup(p->members, (uintptr_t) mq);
	assert(tag);
	return tag;
}

void *mq_poll_readable(struct mq_poll *p) {
	assert(p);

	set_first_element(p->readable);
	struct mq *mq = set_next_element(p->readable);
	assert(mq);
	void *tag = itable_lookup(p->members, (uintptr_t) mq);
	assert(tag);
	return tag;
}

void *mq_poll_error(struct mq_poll *p) {
	assert(p);

	set_first_element(p->error);
	struct mq *mq = set_next_element(p->error);
	assert(mq);
	void *tag = itable_lookup(p->members, (uintptr_t) mq);
	assert(tag);
	return tag;
}

int mq_poll_wait(struct mq_poll *p, time_t stoptime) {
	assert(p);

	int rc;
	int count = itable_size(p->members);
	struct pollfd *pfds = xxcalloc(count, sizeof(*pfds));

	do {
		uint64_t key;
		uintptr_t ptr;
		void *value;
		itable_firstkey(p->members);
		// This assumes that iterating over an itable does not
		// change the order of the elements.
		for (int i = 0; itable_nextkey(p->members, &key, &value); i++) {
			ptr = key;
			struct mq *mq = (struct mq *) ptr;
			pfds[i].fd = link_fd(mq->link);

			// NB: we're using revents from the *previous* iteration
			rc = handle_revents(&pfds[i], mq);
			if (rc == -1) {
				goto DONE;
			}
			pfds[i].events = poll_events(mq);
		}

		rc = 0;
		rc += set_size(p->acceptable);
		rc += set_size(p->readable);
		rc += set_size(p->error);
		if (rc > 0) goto DONE;
	} while ((rc = ppoll_compat(pfds, count, stoptime)) > 0);

DONE:
	free(pfds);
	if (rc >= 0) {
		return rc;
	} else if (rc == -1 && errno == EINTR) {
		return 0;
	} else {
		return -1;
	}
}

int mq_send_buffer(struct mq *mq, buffer_t *buf) {
	assert(mq);
	assert(buf);

	errno = mq_geterror(mq);
	if (errno != 0) return -1;

	struct mq_msg *msg = msg_create();
	msg->storage = MQ_MSG_NEWBUFFER;
	msg->type = HDR_MSG_SINGLE;
	msg->buffer = buf;
	buffer_tolstring(buf, &msg->len);
	msg->hdr_len = htonll(msg->len);
	list_push_tail(mq->send, msg);

	return 0;
}

int mq_send_fd(struct mq *mq, int fd) {
	assert(mq);
	assert(fd >= 0);

	errno = mq_geterror(mq);
	if (errno != 0) return -1;

	if (set_nonblocking(fd) < 0) return -1;

	struct mq_msg *msg = msg_create();
	msg->storage = MQ_MSG_FD;
	msg->buffering = true;
	msg->buffer = xxcalloc(1, sizeof(*msg->buffer));
	buffer_init(msg->buffer);
	buffer_abortonfailure(msg->buffer, true);
	buffer_grow(msg->buffer, MQ_PIPEBUF_SIZE);
	msg->type = HDR_MSG_START;
	msg->pipefd = fd;
	msg->len = MQ_PIPEBUF_SIZE;
	msg->hdr_len = htonll(msg->len);
	list_push_tail(mq->send, msg);

	return 0;
}

mq_msg_t mq_recv(struct mq *mq, buffer_t **out) {
	assert(mq);

	if (!mq->recv) return MQ_MSG_NONE;
	struct mq_msg *msg = mq->recv;
	mq->recv = NULL;
	mq_msg_t storage = msg->storage;
	if (mq->poll_group) {
		set_remove(mq->poll_group->readable, mq);
	}

	switch (storage) {
		case MQ_MSG_NEWBUFFER:
			assert(out);
			*out = msg->buffer;
			msg->buffer = NULL;
			msg->storage = MQ_MSG_NONE;
		case MQ_MSG_FD:
		case MQ_MSG_BUFFER:
			break;
		case MQ_MSG_NONE:
			abort();
	}

	mq_msg_delete(msg);
	return storage;
}

int mq_store_buffer(struct mq *mq, buffer_t *buf) {
	assert(mq);
	assert(buf);

	assert(!mq->recving);
	buffer_abortonfailure(buf, true);
	buffer_rewind(buf, 0);
	mq->recving = msg_create();
	mq->recving->buffer = buf;
	mq->recving->storage = MQ_MSG_BUFFER;

	return 0;
}

int mq_store_fd(struct mq *mq, int fd) {
	assert(mq);
	assert(fd >= 0);

	if (set_nonblocking(fd) < 0) return -1;

	assert(!mq->recving);
	mq->recving = msg_create();
	struct mq_msg *rcv = mq->recving;
	rcv->pipefd = fd;
	rcv->storage = MQ_MSG_FD;
	rcv->buffer = xxcalloc(1, sizeof(*rcv->buffer));
	buffer_init(rcv->buffer);
	buffer_abortonfailure(rcv->buffer, true);

	return 0;
}
