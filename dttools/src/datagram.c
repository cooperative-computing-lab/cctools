/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "datagram.h"
#include "stringtools.h"

#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <sys/un.h>

#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <netdb.h>
#include <errno.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

struct datagram {
	int fd;
};

struct datagram *datagram_create(int port)
{
	struct datagram *d = 0;
	struct sockaddr_in address;
	int success;
	int on = 1;

	d = malloc(sizeof(*d));
	if(!d)
		goto failure;

	d->fd = socket(PF_INET, SOCK_DGRAM, 0);
	if(d->fd < 0)
		goto failure;

	setsockopt(d->fd, SOL_SOCKET, SO_BROADCAST, &on, sizeof(on));

	if(port != DATAGRAM_PORT_ANY) {
		address.sin_family = AF_INET;
		address.sin_port = htons(port);
		address.sin_addr.s_addr = htonl(INADDR_ANY);

		success = bind(d->fd, (struct sockaddr *) &address, sizeof(address));
		if(success < 0)
			goto failure;
	}

	return d;

      failure:
	datagram_delete(d);
	return 0;
}

void datagram_delete(struct datagram *d)
{
	if(d) {
		if(d->fd >= 0)
			close(d->fd);
		free(d);
	}
}

static void addr_to_string(struct in_addr *addr, char *str)
{
	unsigned char *bytes;
	bytes = (unsigned char *) addr;

	sprintf(str, "%u.%u.%u.%u", (unsigned) bytes[0], (unsigned) bytes[1], (unsigned) bytes[2], (unsigned) bytes[3]);
}

static int errno_is_temporary(int e)
{
	if(e == EINTR || e == EWOULDBLOCK || e == EAGAIN || e == EINPROGRESS || e == EALREADY || e == EISCONN) {
		return 1;
	} else {
		return 0;
	}
}

#ifndef SOCKLEN_T
#if defined(__GLIBC__) || defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_AIX)
#define SOCKLEN_T socklen_t
#else
#define SOCKLEN_T int
#endif
#endif

int datagram_recv(struct datagram *d, char *data, int length, char *addr, int *port, int timeout)
{
	int result;
	struct sockaddr_in iaddr;
	SOCKLEN_T iaddr_length;
	fd_set fds;
	struct timeval tm;

	while(1) {

		tm.tv_sec = timeout / 1000000;
		tm.tv_usec = timeout % 1000000;

		FD_ZERO(&fds);
		FD_SET(d->fd, &fds);

		result = select(d->fd + 1, &fds, 0, 0, &tm);
		if(result > 0) {
			if(FD_ISSET(d->fd, &fds))
				break;
		} else if(result < 0 && errno_is_temporary(errno)) {
			continue;
		} else {
			return -1;
		}
	}

	iaddr_length = sizeof(iaddr);

	result = recvfrom(d->fd, data, length, 0, (struct sockaddr *) &iaddr, &iaddr_length);
	if(result < 0)
		return result;

	addr_to_string(&iaddr.sin_addr, addr);
	*port = ntohs(iaddr.sin_port);

	return result;
}

int datagram_send(struct datagram *d, const char *data, int length, const char *addr, int port)
{
	int result;
	struct sockaddr_in iaddr;
	SOCKLEN_T iaddr_length;

	iaddr_length = sizeof(iaddr);

	iaddr.sin_family = AF_INET;
	iaddr.sin_port = htons(port);
	if(!string_to_ip_address(addr, (unsigned char *) &iaddr.sin_addr))
		return -1;

	result = sendto(d->fd, data, length, 0, (const struct sockaddr *) &iaddr, iaddr_length);
	if(result < 0)
		return result;

	return result;
}

int datagram_fd(struct datagram *d)
{
	return d->fd;
}

/* vim: set noexpandtab tabstop=4: */
