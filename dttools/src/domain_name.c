/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "domain_name.h"
#include "link.h"
#include "stringtools.h"
#include "debug.h"
#include "address.h"

#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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

int domain_name_lookup_reverse(const char *addr, char *name)
{
	int err;
	struct sockaddr_storage saddr;
	SOCKLEN_T saddr_length;

	debug(D_DNS, "looking up addr %s", addr);

	if(!address_to_sockaddr(addr,0,&saddr,&saddr_length)) {
		debug(D_DNS, "%s is not a valid addr", addr);
		return 0;
	}

	if ((err = getnameinfo((struct sockaddr *) &saddr, sizeof(saddr), name, DOMAIN_NAME_MAX,0,0,0)) !=0 ) {
		debug(D_DNS, "couldn't look up %s: %s", addr, gai_strerror(err));
		return 0;
	}
	debug(D_DNS, "%s is %s", addr, name);

	return 1;
}

int domain_name_lookup(const char *name, char *addr)
{
	struct addrinfo hints;
	struct addrinfo *result;
	int err;

	debug(D_DNS, "looking up name %s", name);

	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	address_check_mode(&hints);

	if ((err = getaddrinfo(name, NULL, &hints, &result)) != 0) {
		debug(D_DNS, "couldn't look up %s: %s", name, gai_strerror(err));
		return 0;
	}

	int r = address_from_sockaddr(addr,result->ai_addr);

	if(r) {
		debug(D_DNS, "%s is %s", name, addr);
	} else {
		debug(D_DNS, "unable to translate result from getaddrinfo");
	}

	freeaddrinfo(result);

	return r;
}

/* vim: set noexpandtab tabstop=8: */
