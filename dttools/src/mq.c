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
#include <signal.h>
#include <poll.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "mq.h"
#include "buffer.h"
#include "list.h"
#include "itable.h"
#include "set.h"
#include "link.h"
#include "xxmalloc.h"
#include "debug.h"
#include "jx_print.h"
#include "jx_parse.h"


#define HDR_SIZE (sizeof(struct mq_msg) - offsetof(struct mq_msg, magic))
#define HDR_MAGIC "MQmsg"

enum mq_msg_type {
	MQ_MSG_BUFFER = 1,
	MQ_MSG_JSON = 2,
};

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
	buffer_t buf;
	struct jx *j;

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
	 *  0 1 2 3 4 5 6 7
	 * +-+-+-+-+-+-+-+-+
	 * |  magic  |pad|*|    *type
	 * +-+-+-+-+-+-+-+-+
	 * |    length     |
	 * +-+-+-+-+-+-+-+-+
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
	struct mq_msg *send_buf;
	struct mq_msg *recv_buf;
	struct mq_poll *poll_group;
};

struct mq_poll {
	struct itable *members;
	struct set *acceptable;
	struct set *readable;
	struct set *error;
};


#ifndef htonll
static uint64_t htonll(uint64_t hostlonglong) {
	uint64_t out = 0;
	uint8_t *d = (uint8_t *) &out;
	d[7] = hostlonglong>>0;
	d[6] = hostlonglong>>8;
	d[5] = hostlonglong>>16;
	d[4] = hostlonglong>>24;
	d[3] = hostlonglong>>32;
	d[2] = hostlonglong>>40;
	d[1] = hostlonglong>>48;
	d[0] = hostlonglong>>56;
	return out;
}
#endif

#ifndef ntohll
static uint64_t ntohll(uint64_t netlonglong) {
	uint64_t out = 0;
	uint8_t *d = (uint8_t *) &netlonglong;
	out |= (uint64_t) d[7]<<0;
	out |= (uint64_t) d[6]<<8;
	out |= (uint64_t) d[5]<<16;
	out |= (uint64_t) d[4]<<24;
	out |= (uint64_t) d[3]<<32;
	out |= (uint64_t) d[2]<<40;
	out |= (uint64_t) d[1]<<48;
	out |= (uint64_t) d[0]<<56;
	return out;
}
#endif

static int ppoll_compat(struct pollfd fds[], nfds_t nfds, int stoptime) {
	assert(fds);
	sigset_t mask;
	sigemptyset(&mask);
	int timeout = stoptime - time(NULL);
	if (timeout < 0) return 0;

#ifdef HAS_PPOLL
	struct timespec s;
	s.tv_nsec = 0;
	s.tv_sec = timeout;
	return ppoll(fds, nfds, &s, &mask);
#else
	sigset_t origmask;
	sigprocmask(SIG_SETMASK, &mask, &origmask);
	int rc = poll(fds, nfds, timeout*1000);
	int saved_errno = errno;
	sigprocmask(SIG_SETMASK, &origmask, NULL);
	errno = saved_errno;
	return rc;
#endif
}


static bool errno_is_temporary(void) {
	if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN || errno == EINPROGRESS || errno == EALREADY || errno == EISCONN) {
		return true;
	} else {
		return false;
	}
}

static struct mq_msg *msg_create (enum mq_msg_type type) {
	// sanity check
	assert(HDR_SIZE == 16);
	struct mq_msg *out = xxcalloc(1, sizeof(struct mq_msg));
	buffer_init(&out->buf);
	buffer_abortonfailure(&out->buf, true);
	memcpy(out->magic, HDR_MAGIC, sizeof(out->magic));
	out->type = type;
	return out;
}

static void mq_die(struct mq *mq, int err) {
	assert(mq);
	mq->state = MQ_SOCKET_ERROR;
	mq->err = err;

	mq_close(mq->acc);
	mq_msg_delete(mq->send_buf);
	mq_msg_delete(mq->recv_buf);
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

static void delete_msg(struct mq_msg *msg) {
	if (!msg) return;
	buffer_free(&msg->buf);
	jx_delete(msg->j);
	free(msg);
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

void mq_msg_delete(struct mq_msg *msg) {
	if (!msg) return;
	// once blobs are implemented, check for on-disk stuff to delete
	delete_msg(msg);
}

static int validate_header(struct mq_msg *msg) {
	assert(msg);
	errno = EBADF;
	if (memcmp(msg->magic, HDR_MAGIC, sizeof(msg->magic))) {
		return -1;
	}
	switch (msg->type) {
		case MQ_MSG_BUFFER:
		case MQ_MSG_JSON:
			break;
		default:
			return -1;
	}
	return 0;
}

static int unpack_msg(struct mq_msg *msg) {
	assert(msg);
	switch (msg->type) {
		case MQ_MSG_JSON:
			msg->j = jx_parse_string(buffer_tostring(&msg->buf));
			if (!msg->j) {
				errno = EBADMSG;
				return -1;
			}
			break;
		case MQ_MSG_BUFFER:
			// nothing more to do
			break;
	}
	return 0;
}

static int flush_send(struct mq *mq) {
	assert(mq);

	int socket = link_fd(mq->link);

	while (true) {
		if (!mq->send_buf) {
			mq->send_buf = list_pop_head(mq->send);
			if (!mq->send_buf) return 0;
			buffer_tolstring(&mq->send_buf->buf, &mq->send_buf->len);
			mq->send_buf->hdr_len = htonll(mq->send_buf->len);
		}
		struct mq_msg *snd = mq->send_buf;

		// make sure the cast below won't overflow
		assert(HDR_SIZE < PTRDIFF_MAX);
		assert(snd->len < PTRDIFF_MAX);
		if (snd->hdr_pos < (ptrdiff_t) HDR_SIZE) {
			ssize_t rc = send(socket, &snd->magic + snd->hdr_pos,
					HDR_SIZE - snd->hdr_pos, 0);
			if (rc == -1 && errno_is_temporary()) {
				return 0;
			} else if (rc <= 0) {
				return -1;
			}
			snd->hdr_pos += rc;
		} else if (snd->buf_pos < (ptrdiff_t) snd->len) {
			ssize_t rc = send(socket,
					buffer_tostring(&snd->buf) + snd->buf_pos,
					snd->len - snd->buf_pos, 0);
			if (rc == -1 && errno_is_temporary()) {
				return 0;
			} else if (rc <= 0) {
				return -1;
			}
			snd->buf_pos += rc;
		} else {
			delete_msg(snd);
			mq->send_buf = NULL;
		}
	}
}

static int flush_recv(struct mq *mq) {
	assert(mq);

	int socket = link_fd(mq->link);

	while (!mq->recv) {
		if (!mq->recv_buf) {
			mq->recv_buf = msg_create(0);
		}
		struct mq_msg *rcv = mq->recv_buf;

		// make sure the cast below won't overflow
		assert(HDR_SIZE < PTRDIFF_MAX);
		assert(rcv->len < PTRDIFF_MAX);
		if (rcv->hdr_pos < (ptrdiff_t) HDR_SIZE) {
			ssize_t rc = recv(socket, &rcv->magic + rcv->hdr_pos,
					HDR_SIZE - rcv->hdr_pos, 0);
			if (rc == -1 && errno_is_temporary()) {
				return 0;
			} else if (rc <= 0) {
				return -1;;
			}
			rcv->hdr_pos += rc;
		} else if (!rcv->parsed_header) {
			rcv->len = ntohll(rcv->hdr_len);
			if (validate_header(rcv) == -1) return -1;
			buffer_grow(&rcv->buf, rcv->len);
			rcv->parsed_header = true;
		} else if (rcv->buf_pos < (ptrdiff_t) rcv->len) {
			ssize_t rc = recv(socket,
					(char *) buffer_tostring(&rcv->buf) + rcv->buf_pos,
					rcv->len - rcv->buf_pos, 0);
			if (rc == -1 && errno_is_temporary()) {
				return 0;
			} else if (rc <= 0) {
				return -1;;
			}
			rcv->buf_pos += rc;
		} else {
			if (unpack_msg(rcv) == -1) return -1;
			mq->recv = mq->recv_buf;
			mq->recv_buf = NULL;
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
			if (mq->send_buf || list_length(mq->send)) {
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

struct mq_msg *mq_wrap_buffer(const void *b, size_t size) {
	assert(b);
	struct mq_msg *out = msg_create(MQ_MSG_BUFFER);
	buffer_putlstring(&out->buf, b, size);
	return out;
}

struct mq_msg *mq_wrap_json(struct jx *j) {
	assert(j);
	struct mq_msg *out = msg_create(MQ_MSG_JSON);
	jx_print_buffer(j, &out->buf);
	return out;
}

void *mq_unwrap_buffer(struct mq_msg *msg, size_t *len) {
	assert(msg);
	if (msg->type != MQ_MSG_BUFFER) return NULL;
	char *out = xxmalloc(msg->len + 1);
	out[msg->len] = 0;
	memcpy(out, buffer_tostring(&msg->buf), msg->len);
	if (len) {
		*len = msg->len;
	}
	delete_msg(msg);
	return out;
}

struct jx *mq_unwrap_json(struct mq_msg *msg) {
	assert(msg);
	if (msg->type != MQ_MSG_JSON) return NULL;
	struct jx *out = msg->j;
	msg->j = NULL;
	delete_msg(msg);
	return out;
}

int mq_send(struct mq *mq, struct mq_msg *msg) {
	assert(mq);
	assert(msg);
	errno = mq_geterror(mq);
	if (errno != 0) return -1;
	list_push_tail(mq->send, msg);
	return 0;
}

struct mq_msg *mq_recv(struct mq *mq) {
	assert(mq);
	struct mq_msg *out = mq->recv;
	mq->recv = NULL;
	if (mq->poll_group) {
		set_remove(mq->poll_group->readable, mq);
	}
	return out;
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
