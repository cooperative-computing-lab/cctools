/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
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

#include "address.h"

struct datagram {
	int fd;
};

struct datagram *datagram_create_address(const char *addr, int port)
{
	struct datagram *d = 0;
	struct sockaddr_storage address;
	SOCKLEN_T address_length;
	int success;
	int on = 1;

	if(port==DATAGRAM_PORT_ANY) port=0;

	address_to_sockaddr(addr,port,&address,&address_length);

	d = malloc(sizeof(*d));
	if(!d)
		goto failure;

	d->fd = socket(address.ss_family, SOCK_DGRAM, 0);
	if(d->fd < 0)
		goto failure;

	setsockopt(d->fd, SOL_SOCKET, SO_BROADCAST, &on, sizeof(on));

	success = bind(d->fd, (struct sockaddr *) &address, address_length );
	if(success < 0)
		goto failure;

	return d;
failure:
	datagram_delete(d);
	return 0;
}

struct datagram *datagram_create(int port)
{
	return datagram_create_address(NULL, port);
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

int datagram_recv(struct datagram *d, char *data, int length, char *addr, int *port, int timeout)
{
	int result;
	struct sockaddr_storage iaddr;
	SOCKLEN_T iaddr_length;
	SOCKLEN_T addr_length = DATAGRAM_ADDRESS_MAX;
	char port_string[16];
	SOCKLEN_T port_string_length = 16;
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

	getnameinfo((struct sockaddr *)&iaddr,iaddr_length,addr,addr_length,port_string,port_string_length,NI_NUMERICHOST|NI_NUMERICSERV);

	*port = atoi(port_string);

	return result;
}

int datagram_send(struct datagram *d, const char *data, int length, const char *addr, int port)
{
	int result;
	struct sockaddr_storage iaddr;
	SOCKLEN_T iaddr_length;

	result = address_to_sockaddr(addr,port,&iaddr,&iaddr_length);
	if(!result) {
		errno = EINVAL;
		return -1;
	}

	result = sendto(d->fd, data, length, 0, (const struct sockaddr *) &iaddr, iaddr_length);
	if(result < 0)
		return result;

	return result;
}

int datagram_fd(struct datagram *d)
{
	return d->fd;
}

/* vim: set noexpandtab tabstop=8: */
