/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "datagram.h"
#include "debug.h"
#include "getaddrinfo_cache.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if ! (defined(__GLIBC__) || defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_AIX))
	typedef int socklen_t;
#endif

struct datagram {
	int fd;
};

struct datagram *datagram_create_address (const char *nodename, const char *servname)
{
	int rc;
	struct datagram *d = 0;
	struct addrinfo hints;
	struct addrinfo *addr = NULL, *addri;

	debug(D_DEBUG, "binding socket to %s:%s", nodename ? nodename : "*", servname ? servname : "*");

	d = malloc(sizeof(*d));
	if(!d)
		goto failure;

	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_family = AF_UNSPEC; /* IPv4 or IPv6 (RFC 3484 specifies IPv6 first) */
	hints.ai_flags = AI_PASSIVE;
	rc = getaddrinfo(nodename, servname ? servname : "0", &hints, &addr);
	if (rc) {
		debug(D_TCP, "getaddrinfo: %s", gai_strerror(rc));
		errno = EINVAL;
		goto failure;
	} else if (addr == NULL) {
		debug(D_TCP, "getaddrinfo: no results");
		errno = EINVAL;
		goto failure;
	}

	for (addri = addr; addri; addri = addri->ai_next) {
		d->fd = socket(addri->ai_family, addri->ai_socktype, addri->ai_protocol);
		if(d->fd == -1) {
			debug(D_DEBUG, "could not create socket for address family");
			continue;
		}

		rc = fcntl(d->fd, F_GETFD);
		if (rc == -1)
			abort();
		rc |= FD_CLOEXEC;
		if (fcntl(d->fd, F_SETFD, rc) == -1)
			abort();

		rc = 1;
		if (setsockopt(d->fd, SOL_SOCKET, SO_REUSEADDR, &rc, sizeof(rc)) == -1)
			debug(D_DEBUG, "could not setsockopt SO_REUSEADDR: %s", strerror(errno));
		rc = 1;
		if (setsockopt(d->fd, SOL_SOCKET, SO_BROADCAST, &rc, sizeof(rc)) == -1)
			debug(D_DEBUG, "could not setsockopt SO_BROADCAST: %s", strerror(errno));

		if (servname) {
			rc = bind(d->fd, addri->ai_addr, addri->ai_addrlen);
			if (rc == -1) {
				debug(D_DEBUG, "bind: %s", strerror(errno));
				goto failure;
			}
		}

		return d;
	}

failure:
	free(d);
	return 0;
}

struct datagram *datagram_create(int port)
{
	char servname[128];
	snprintf(servname, sizeof(servname), "%d", port);
	return datagram_create_address(NULL, servname);
}

void datagram_delete(struct datagram *d)
{
	if(d) {
		if(d->fd >= 0)
			close(d->fd);
		free(d);
	}
}

static int errno_is_temporary(int e)
{
	if(e == EINTR || e == EWOULDBLOCK || e == EAGAIN || e == EINPROGRESS || e == EALREADY || e == EISCONN) {
		return 1;
	} else {
		return 0;
	}
}

ssize_t datagram_recv(struct datagram *d, void *data, size_t length, struct sockaddr *sa, socklen_t *socklen, time_t timeout)
{
	int rc;
	ssize_t result;

	while(1) {
		fd_set fds;
		struct timeval tm;

		tm.tv_sec = timeout / 1000000;
		tm.tv_usec = timeout % 1000000;

		FD_ZERO(&fds);
		FD_SET(d->fd, &fds);

		rc = select(d->fd + 1, &fds, 0, 0, &tm);
		if(rc > 0) {
			if(FD_ISSET(d->fd, &fds))
				break;
		} else if(rc < 0 && errno_is_temporary(errno)) {
			continue;
		} else {
			return -1;
		}
	}

	result = recvfrom(d->fd, data, length, 0, sa, socklen);
	if (result == -1)
		return -1;

	return result;
}

ssize_t datagram_send (struct datagram *d, const void *data, size_t length, const char *nodename, const char *servname)
{
	int rc;
	ssize_t result;
	struct addrinfo hints;
	struct addrinfo *addr = NULL, *addri;

	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_family = AF_UNSPEC; /* IPv4 or IPv6 */
	rc = getaddrinfo_cache(nodename, servname, &hints, &addr);
	if (rc)
		return errno = EINVAL, -1;

	for (addri = addr; addri; addri = addri->ai_next) {
		result = sendto(d->fd, data, length, 0, addri->ai_addr, addri->ai_addrlen);
		if (result == -1) {
			debug(D_DEBUG, "sendto: %s", strerror(errno));
			continue; /* try another address */
		} else if ((size_t)result < length) {
			debug(D_DEBUG, "sendto: sent partial datagram (%zd/%zu)", result, length);
			break;
		} else if ((size_t)result == length) {
			break;
		} else assert(0);
	}

	return result;
}

int datagram_fd(struct datagram *d)
{
	return d->fd;
}

/* vim: set noexpandtab tabstop=4: */
