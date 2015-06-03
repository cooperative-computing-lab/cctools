/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "buffer.h"
#include "debug.h"
#include "domain_name.h"
#include "full_io.h"
#include "link.h"
#include "macros.h"
#include "stringtools.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/file.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/utsname.h>

#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef TCP_LOW_PORT_DEFAULT
#define TCP_LOW_PORT_DEFAULT 1024
#endif

#ifndef TCP_HIGH_PORT_DEFAULT
#define TCP_HIGH_PORT_DEFAULT 32767
#endif

static FILE *stats;

enum link_type {
	LINK_TYPE_STANDARD,
	LINK_TYPE_FILE,
};

struct link {
	int fd;
	enum link_type type;
	uint64_t read, written;
	char *buffer_start;
	size_t buffer_length;
	char buffer[1<<16];
	char raddr[LINK_ADDRESS_MAX];
	int rport;
};

static int link_send_window = 65536;
static int link_recv_window = 65536;
static int link_override_window = 0;

static void stats_update (int fd, const char type, size_t count)
{
	if (stats)
		fprintf(stats, "%" PRIu64 " %d %c %zu\n", (uint64_t)time(NULL), fd, (int)type, count);
}

void link_window_set(int send_buffer, int recv_buffer)
{
	link_send_window = send_buffer;
	link_recv_window = recv_buffer;
}

void link_window_get(struct link *l, int *send_buffer, int *recv_buffer)
{
	if(l->type == LINK_TYPE_FILE) {
		return;
	}

	socklen_t length = sizeof(*send_buffer);
	getsockopt(l->fd, SOL_SOCKET, SO_SNDBUF, (void *) send_buffer, &length);
	getsockopt(l->fd, SOL_SOCKET, SO_RCVBUF, (void *) recv_buffer, &length);
}

static void link_window_configure(struct link *l)
{
	const char *s = getenv("TCP_WINDOW_SIZE");

	if(l->type == LINK_TYPE_FILE) {
		return;
	}

	if(s) {
		link_send_window = atoi(s);
		link_recv_window = atoi(s);
		link_override_window = 1;
	}

	if(link_override_window) {
		setsockopt(l->fd, SOL_SOCKET, SO_SNDBUF, (void *) &link_send_window, sizeof(link_send_window));
		setsockopt(l->fd, SOL_SOCKET, SO_RCVBUF, (void *) &link_recv_window, sizeof(link_recv_window));
	}
}

/*
When a link is dropped, we do not want to deal with a signal,
but we want the current system call to abort.  To accomplish this, we
send SIGPIPE to a dummy function instead of just blocking or ignoring it.
*/

static void signal_swallow(int num)
{
}

static int link_squelch()
{
	signal(SIGPIPE, signal_swallow);
	return 1;
}

int link_keepalive(struct link *link, int onoff) {
	int result, value;

	if(link->type == LINK_TYPE_FILE) {
		return 0;
	}

	if(onoff > 0) {
		value = 1;
	} else {
		value = 0;
	}

	result = setsockopt(link->fd, SOL_SOCKET, SO_KEEPALIVE, (void *) &value, sizeof(value));
	if(result!= 0)
		return 0;
	return 1;
}

int link_nonblocking(struct link *link, int onoff)
{
	int result;

	result = fcntl(link->fd, F_GETFL);
	if(result < 0)
		return 0;

	if(onoff) {
		result |= O_NONBLOCK;
	} else {
		result &= ~(O_NONBLOCK);
	}

	result = fcntl(link->fd, F_SETFL, result);
	if(result < 0)
		return 0;

	return 1;
}

int link_buffer_empty(struct link *link) {
	return link->buffer_length > 0 ? 0 : 1;
}

static int errno_is_temporary(int e)
{
	if(e == EINTR || e == EWOULDBLOCK || e == EAGAIN || e == EINPROGRESS || e == EALREADY || e == EISCONN) {
		return 1;
	} else {
		return 0;
	}
}

static int link_internal_sleep(struct link *link, struct timeval *timeout, sigset_t *mask, int reading, int writing)
{
	int result;
	struct pollfd pfd;
	int msec;
	sigset_t cmask;

	if(timeout) {
		msec = (timeout->tv_sec * 1000.0) + (timeout->tv_usec/1000.0);
	} else {
		msec = -1;
	}

	if(reading && link->buffer_length) {
		return 1;
	}

	while (1) {
		pfd.fd = link->fd;
		pfd.revents = 0;

		if (reading) pfd.events = POLLIN;
		if (writing) pfd.events = POLLOUT;

		sigprocmask(SIG_UNBLOCK, mask, &cmask);
		result = poll(&pfd, 1, msec);
		sigprocmask(SIG_SETMASK, &cmask, NULL);

		if (result > 0) {
			if (reading && (pfd.revents & POLLIN)) {
				return 1;
			}
			if (writing && (pfd.revents & POLLOUT)) {
				return 1;
			}
			if (pfd.revents & POLLHUP) {
				return 0;
			}
			continue;
		} else if (result == 0) {
			return 0;
		} else if (mask && errno == EINTR) {
			return 0;
		} else if (errno_is_temporary(errno)) {
			continue;
		} else {
			return 0;
		}
	}
}

int link_sleep(struct link *link, time_t stoptime, int reading, int writing)
{
	struct timeval tm, *tptr;

	if(stoptime == LINK_FOREVER) {
		tptr = 0;
	} else {
		time_t timeout = stoptime - time(0);
		if(timeout <= 0) {
			errno = ECONNRESET;
			return 0;
		}
		tm.tv_sec = timeout;
		tm.tv_usec = 0;
		tptr = &tm;
	}

	return link_internal_sleep(link, tptr, NULL, reading, writing);
}

int link_usleep(struct link *link, int usec, int reading, int writing)
{
	struct timeval tm;

	tm.tv_sec = 0;
	tm.tv_usec = usec;

	return link_internal_sleep(link, &tm, NULL, reading, writing);
}

int link_usleep_mask(struct link *link, int usec, sigset_t *mask, int reading, int writing) {
	struct timeval tm;
	sigset_t emptymask;

	tm.tv_sec = 0;
	tm.tv_usec = usec;

	if(!mask) {
		sigemptyset(&emptymask);
		mask = &emptymask;
	}

	return link_internal_sleep(link, &tm, mask, reading, writing);
}

static struct link *link_create()
{
	struct link *link;

	link = malloc(sizeof(*link));
	if(!link)
		return 0;

	link->read = link->written = 0;
	link->fd = -1;
	link->buffer_start = link->buffer;
	link->buffer_length = 0;
	link->raddr[0] = 0;
	link->rport = 0;
	link->type = LINK_TYPE_STANDARD;

	return link;
}

struct link *link_attach(int fd)
{
	struct link *l = link_create();
	if(!l)
		return 0;

	l->fd = fd;

	if(link_address_remote(l, l->raddr, &l->rport)) {
		debug(D_TCP, "attached to %s:%d", l->raddr, l->rport);
		stats_update(l->fd, 'o', 0);
		return l;
	} else {
		l->fd = -1;
		link_close(l);
		return 0;
	}
}

struct link *link_attach_to_file(FILE *f)
{
	struct link *l = link_create();
	int fd = fileno(f);

	if(fd < 0) {
		link_close(l);
		return NULL;
	}

	l->fd = fd;
	l->type = LINK_TYPE_FILE;
	stats_update(l->fd, 'o', 0);
	return l;
}

struct link *link_attach_to_fd(int fd)
{
	struct link *l = link_create();

	if(fd < 0) {
		link_close(l);
		return NULL;
	}

	l->fd = fd;
	l->type = LINK_TYPE_FILE;
	return l;
}

struct link *link_serve(int port)
{
	return link_serve_address(0, port);
}

struct link *link_serve_address(const char *addr, int port)
{
	struct link *link = 0;
	struct sockaddr_in address;
	int success;
	int value;

	link = link_create();
	if(!link)
		goto failure;

	link->fd = socket(AF_INET, SOCK_STREAM, 0);
	if(link->fd < 0)
		goto failure;

	value = fcntl(link->fd, F_GETFD);
	if (value == -1)
		goto failure;
	value |= FD_CLOEXEC;
	if (fcntl(link->fd, F_SETFD, value) == -1)
		goto failure;

	value = 1;
	setsockopt(link->fd, SOL_SOCKET, SO_REUSEADDR, (void *) &value, sizeof(value));

	link_window_configure(link);

	memset(&address, 0, sizeof(address));
#if defined(CCTOOLS_OPSYS_DARWIN)
	address.sin_len = sizeof(address);
#endif
	address.sin_family = AF_INET;

	if(addr) {
		string_to_ip_address(addr, (unsigned char *) &address.sin_addr.s_addr);
	} else {
		address.sin_addr.s_addr = htonl(INADDR_ANY);
	}

	int low = TCP_LOW_PORT_DEFAULT;
	int high = TCP_HIGH_PORT_DEFAULT;
	if(port < 1) {
		const char *lowstr = getenv("TCP_LOW_PORT");
		if (lowstr)
			low = atoi(lowstr);
		const char *highstr = getenv("TCP_HIGH_PORT");
		if (highstr)
			high = atoi(highstr);
	} else {
		low = high = port;
	}

	if(high < low)
		fatal("high port %d is less than low port %d in range", high, low);

	for (port = low; port <= high; port++) {
		address.sin_port = htons(port);
		success = bind(link->fd, (struct sockaddr *) &address, sizeof(address));
		if(success == -1) {
			if(errno == EADDRINUSE) {
				//If a port is specified, fail!
				if (low == high) {
					goto failure;
				} else {
					continue;
				}
			} else {
				goto failure;
			}
		}
		break;
	}

	success = listen(link->fd, 5);
	if(success < 0)
		goto failure;

	if(!link_nonblocking(link, 1))
		goto failure;

	debug(D_TCP, "listening on port %d", port);

	stats_update(link->fd, 'o', 0);

	return link;

failure:
	if(link)
		link_close(link);
	return 0;
}

struct link *link_accept(struct link *master, time_t stoptime)
{
	struct link *link = 0;

	if(master->type == LINK_TYPE_FILE) {
		return NULL;
	}

	link = link_create();
	if(!link)
		goto failure;

	while(1) {
		if(!link_sleep(master, stoptime, 1, 0))
			goto failure;
		link->fd = accept(master->fd, 0, 0);
		break;
	}

	if(!link_nonblocking(link, 1))
		goto failure;
	if(!link_address_remote(link, link->raddr, &link->rport))
		goto failure;
	link_squelch();

	debug(D_TCP, "got connection from %s:%d", link->raddr, link->rport);

	stats_update(link->fd, 'o', 0);

	return link;

failure:
	if(link)
		link_close(link);
	return 0;
}

struct link *link_connect(const char *addr, int port, time_t stoptime)
{
	struct sockaddr_in address;
	struct link *link = 0;
	int result;
	int save_errno;

	link = link_create();
	if(!link)
		goto failure;

	link_squelch();

	memset(&address, 0, sizeof(address));
#if defined(CCTOOLS_OPSYS_DARWIN)
	address.sin_len = sizeof(address);
#endif
	address.sin_family = AF_INET;
	address.sin_port = htons(port);

	if(!string_to_ip_address(addr, (unsigned char *) &address.sin_addr))
		goto failure;

	link->fd = socket(AF_INET, SOCK_STREAM, 0);
	if(link->fd < 0)
		goto failure;

	link_window_configure(link);

	/* sadly, cygwin does not do non-blocking connect correctly */
#ifdef CCTOOLS_OPSYS_CYGWIN
	if(!link_nonblocking(link, 0))
		goto failure;
#else
	if(!link_nonblocking(link, 1))
		goto failure;
#endif

	debug(D_TCP, "connecting to %s:%d", addr, port);

	while(1) {
		// First attempt a non-blocking connect
		result = connect(link->fd, (struct sockaddr *) &address, sizeof(address));

		// On many platforms, non-blocking connect sets errno in unexpected ways:

		// On OSX, result=-1 and errno==EISCONN indicates a successful connection.
		if(result<0 && errno==EISCONN) result=0;

		// On BSD-derived systems, failure to connect is indicated by errno = EINVAL.
		// Set it to something more explanatory.
		if(result<0 && errno==EINVAL) errno=ECONNREFUSED;

		// Otherwise, a non-temporary errno should cause us to bail out.
		if(result<0 && !errno_is_temporary(errno)) break;

		// If the remote address is valid, we are connected no matter what.
		if(link_address_remote(link, link->raddr, &link->rport)) {
			debug(D_TCP, "made connection to %s:%d", link->raddr, link->rport);
#ifdef CCTOOLS_OPSYS_CYGWIN
			link_nonblocking(link, 1);
#endif
			stats_update(link->fd, 'o', 0);
			return link;
		}

		// if the time has expired, bail out
		if( time(0) >= stoptime ) {
			errno = ETIMEDOUT;
			break;
		}

		// wait for some activity on the socket.
		link_sleep(link, stoptime, 0, 1);

		// No matter how the sleep ends, we want to go back to the top
		// and call connect again to get a proper errno.
	}


	debug(D_TCP, "connection to %s:%d failed (%s)", addr, port, strerror(errno));

failure:
	save_errno = errno;
	if(link)
		link_close(link);
	errno = save_errno;
	return 0;
}

static ssize_t fill_buffer(struct link *link, time_t stoptime)
{
	if(link->buffer_length > 0)
		return link->buffer_length;

	while(1) {
		ssize_t chunk = read(link->fd, link->buffer, sizeof(link->buffer));
		if(chunk > 0) {
			stats_update(link->fd, 'r', chunk);
			link->read += chunk;
			link->buffer_start = link->buffer;
			link->buffer_length = chunk;
			return chunk;
		} else if(chunk == 0) {
			link->buffer_start = link->buffer;
			link->buffer_length = 0;
			return 0;
		} else {
			if(errno_is_temporary(errno)) {
				if(link_sleep(link, stoptime, 1, 0)) {
					continue;
				} else {
					return -1;
				}
			} else {
				return -1;
			}
		}
	}
}

/* link_read blocks until all the requested data is available */

ssize_t link_read(struct link *link, char *data, size_t count, time_t stoptime)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	if(count == 0)
		return 0;

	/* If this is a small read, attempt to fill the buffer */
	if(count < sizeof(link->buffer)) {
		chunk = fill_buffer(link, stoptime);
		if(chunk <= 0)
			return chunk;
	}

	/* Then, satisfy the read from the buffer, if any. */

	if(link->buffer_length > 0) {
		chunk = MIN(link->buffer_length, count);
		memcpy(data, link->buffer_start, chunk);
		data += chunk;
		total += chunk;
		count -= chunk;
		link->buffer_start += chunk;
		link->buffer_length -= chunk;
	}

	/* Otherwise, pull it all off the wire. */

	while(count > 0) {
		chunk = read(link->fd, data, count);
		if(chunk < 0) {
			if(errno_is_temporary(errno)) {
				if(link_sleep(link, stoptime, 1, 0)) {
					continue;
				} else {
					break;
				}
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			stats_update(link->fd, 'r', chunk);
			link->read += chunk;
			total += chunk;
			count -= chunk;
			data += chunk;
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

/* link_read_avail returns whatever is available, blocking only if nothing is */

ssize_t link_read_avail(struct link *link, char *data, size_t count, time_t stoptime)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	/* First, satisfy anything from the buffer. */

	if(link->buffer_length > 0) {
		chunk = MIN(link->buffer_length, count);
		memcpy(data, link->buffer_start, chunk);
		data += chunk;
		total += chunk;
		count -= chunk;
		link->buffer_start += chunk;
		link->buffer_length -= chunk;
	}

	/* Next, read what is available off the wire */

	while(count > 0) {
		chunk = read(link->fd, data, count);
		if(chunk < 0) {
			/* ONLY BLOCK IF NOTHING HAS BEEN READ */
			if(errno_is_temporary(errno) && total == 0) {
				if(link_sleep(link, stoptime, 1, 0)) {
					continue;
				} else {
					break;
				}
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			stats_update(link->fd, 'r', chunk);
			link->read += chunk;
			total += chunk;
			count -= chunk;
			data += chunk;
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

int link_readline(struct link *link, char *line, size_t length, time_t stoptime)
{
	while(1) {
		while(length > 0 && link->buffer_length > 0) {
			*line = *link->buffer_start;
			link->buffer_start++;
			link->buffer_length--;
			if(*line == '\n') {
				*line = '\0';
				return 1;
			} else if(*line == '\r') {
				continue;
			} else {
				line++;
				length--;
			}
		}
		if(length == 0)
			break;
		if(fill_buffer(link, stoptime) <= 0)
			break;
	}

	return 0;
}

ssize_t link_write(struct link *link, const char *data, size_t count, time_t stoptime)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	if (!link)
		return errno = EINVAL, -1;

	while(count > 0) {
		chunk = write(link->fd, data, count);
		if(chunk < 0) {
			if(errno_is_temporary(errno)) {
				if(link_sleep(link, stoptime, 0, 1)) {
					continue;
				} else {
					break;
				}
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			stats_update(link->fd, 'w', chunk);
			link->written += chunk;
			total += chunk;
			count -= chunk;
			data += chunk;
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

ssize_t link_putlstring(struct link *link, const char *data, size_t count, time_t stoptime)
{
	ssize_t total = 0;

	if (!link)
		return errno = EINVAL, -1;

	/* Loop because, unlike link_write, we do not allow partial writes. */
	while (count > 0) {
		ssize_t w = link_write(link, data, count, stoptime);
		if (w == -1)
			return -1;
		count -= w;
		total += w;
		data += w;
	}

	return total;
}

ssize_t link_putvfstring(struct link *link, const char *fmt, time_t stoptime, va_list va)
{
	ssize_t rc;
	size_t l;
	const char *str;
	buffer_t B;

	buffer_init(&B);
	if (buffer_putvfstring(&B, fmt, va) == -1)
		return -1;
	str = buffer_tolstring(&B, &l);
	rc = link_putlstring(link, str, l, stoptime);
	buffer_free(&B);

	return rc;
}

ssize_t link_putfstring(struct link *link, const char *fmt, time_t stoptime, ...)
{
	ssize_t rc;
	va_list va;

	va_start(va, stoptime);
	rc = link_putvfstring(link, fmt, stoptime, va);
	va_end(va);

	return rc;
}

void link_close(struct link *link)
{
	if(link) {
		stats_update(link->fd, 'c', 0);
		if(link->fd >= 0)
			close(link->fd);
		if(link->rport)
			debug(D_TCP, "disconnected from %s:%d", link->raddr, link->rport);
		free(link);
	}
}

void link_detach(struct link *link)
{
	if(link) {
		free(link);
	}
}

int link_fd(struct link *link)
{
	return link->fd;
}

#ifndef SOCKLEN_T
#if defined(__GLIBC__) || defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_AIX)
#define SOCKLEN_T socklen_t
#else
#define SOCKLEN_T int
#endif
#endif

int link_address_local(struct link *link, char *addr, int *port)
{
	struct sockaddr_in iaddr;
	SOCKLEN_T length;
	int result;

	if(link->type == LINK_TYPE_FILE) {
		return 0;
	}

	length = sizeof(iaddr);
	result = getsockname(link->fd, (struct sockaddr *) &iaddr, &length);
	if(result != 0)
		return 0;

	*port = ntohs(iaddr.sin_port);
	string_from_ip_address((unsigned char *) &iaddr.sin_addr, addr);

	return 1;
}

int link_address_remote(struct link *link, char *addr, int *port)
{
	struct sockaddr_in iaddr;
	SOCKLEN_T length;
	int result;

	if(link->type == LINK_TYPE_FILE) {
		return 0;
	}

	length = sizeof(iaddr);
	result = getpeername(link->fd, (struct sockaddr *) &iaddr, &length);
	if(result != 0)
		return 0;

	*port = ntohs(iaddr.sin_port);
	string_from_ip_address((unsigned char *) &iaddr.sin_addr, addr);

	return 1;
}

ssize_t link_stream_to_buffer(struct link * link, char **buffer, time_t stoptime)
{
	ssize_t total = 0;
	buffer_t B;
	buffer_init(&B);

	while(1) {
		char buf[1<<16];
		ssize_t actual = link_read(link, buf, sizeof(buf), stoptime);
		if(actual <= 0)
			break;
		if (buffer_putlstring(&B, buf, actual) == -1) {
			buffer_free(&B);
			return -1;
		}
		total += actual;
	}

	if (buffer_dup(&B, buffer) == -1)
		total = -1;
	buffer_free(&B);

	return total;
}

int64_t link_stream_to_fd(struct link * link, int fd, int64_t length, time_t stoptime)
{
	int64_t total = 0;

	while(length > 0) {
		char buffer[1<<16];
		size_t chunk = MIN(sizeof(buffer), (size_t)length);

		ssize_t ractual = link_read(link, buffer, chunk, stoptime);
		if(ractual <= 0)
			break;

		ssize_t wactual = full_write(fd, buffer, ractual);
		if(wactual != ractual) {
			total = -1;
			break;
		}

		total += ractual;
		length -= ractual;
	}

	return total;
}

int64_t link_stream_to_file(struct link * link, FILE * file, int64_t length, time_t stoptime)
{
	int64_t total = 0;

	while(length > 0) {
		char buffer[1<<16];
		size_t chunk = MIN(sizeof(buffer), (size_t)length);

		ssize_t ractual = link_read(link, buffer, chunk, stoptime);
		if(ractual <= 0)
			break;

		ssize_t wactual = full_fwrite(file, buffer, ractual);
		if(wactual != ractual) {
			total = -1;
			break;
		}

		total += ractual;
		length -= ractual;
	}

	return total;
}

int64_t link_stream_from_fd(struct link * link, int fd, int64_t length, time_t stoptime)
{
	int64_t total = 0;

	while(length > 0) {
		char buffer[1<<16];
		size_t chunk = MIN(sizeof(buffer), (size_t)length);

		ssize_t ractual = full_read(fd, buffer, chunk);
		if(ractual <= 0)
			break;

		ssize_t wactual = link_write(link, buffer, ractual, stoptime);
		if(wactual != ractual) {
			total = -1;
			break;
		}

		total += ractual;
		length -= ractual;
	}

	return total;
}

int64_t link_stream_from_file(struct link * link, FILE * file, int64_t length, time_t stoptime)
{
	int64_t total = 0;

	while(1) {
		char buffer[1<<16];
		size_t chunk = MIN(sizeof(buffer), (size_t)length);

		ssize_t ractual = full_fread(file, buffer, chunk);
		if(ractual <= 0)
			break;

		ssize_t wactual = link_write(link, buffer, ractual, stoptime);
		if(wactual != ractual) {
			total = -1;
			break;
		}

		total += ractual;
		length -= ractual;
	}

	return total;
}

int64_t link_soak(struct link * link, int64_t length, time_t stoptime)
{
	int64_t total = 0;

	while(length > 0) {
		char buffer[1<<16];
		size_t chunk = MIN(sizeof(buffer), (size_t)length);

		ssize_t ractual = link_read(link, buffer, chunk, stoptime);
		if(ractual <= 0)
			break;

		total += ractual;
		length -= ractual;
	}

	return total;
}

int link_tune(struct link *link, link_tune_t mode)
{
	int onoff;
	int success;

	if(link->type == LINK_TYPE_FILE) {
		return 0;
	}

	switch (mode) {
	case LINK_TUNE_INTERACTIVE:
		onoff = 1;
		break;
	case LINK_TUNE_BULK:
		onoff = 0;
		break;
	default:
		return 0;
	}

	success = setsockopt(link->fd, IPPROTO_TCP, TCP_NODELAY, (void *) &onoff, sizeof(onoff));
	if(success != 0)
		return 0;

	return 1;
}

static int link_to_poll(int events)
{
	int r = 0;
	if(events & LINK_READ)
		r |= POLLIN | POLLHUP;
	if(events & LINK_WRITE)
		r |= POLLOUT;
	return r;
}

static int poll_to_link(int events)
{
	int r = 0;
	if(events & POLLIN)
		r |= LINK_READ;
	if(events & POLLHUP)
		r |= LINK_READ;
	if(events & POLLOUT)
		r |= LINK_WRITE;
	return r;
}

int link_poll(struct link_info *links, int nlinks, int msec)
{
	struct pollfd *fds = malloc(nlinks * sizeof(struct pollfd));
	int i;
	int result;

	memset(fds, 0, nlinks * sizeof(struct pollfd));

	for(i = 0; i < nlinks; i++) {
		fds[i].fd = links[i].link->fd;
		fds[i].events = link_to_poll(links[i].events);
		if(links[i].link->buffer_length) {  // If there's data already waiting, don't sit in the poll
			msec = 0;
		}
	}

	result = poll(fds, nlinks, msec);

	if(result >= 0) {
		for(i = 0; i < nlinks; i++) {
			links[i].revents = poll_to_link(fds[i].revents);
			if(links[i].link->buffer_length) {
				links[i].revents |= LINK_READ;
				result++;
			}
		}
	}

	free(fds);

	return result;
}

void link_stats (FILE *log)
{
	stats = log;
	setvbuf(stats, NULL, _IOLBF, 4096);
}

/* vim: set noexpandtab tabstop=4: */
