/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "domain_name.h"
#include "link.h"
#include "stringtools.h"
#include "debug.h"

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
	struct in_addr iaddr;
	int err;

	struct sockaddr_in saddr; //replace with sockaddr_in6 for ipv6
	char host[DOMAIN_NAME_MAX];

	debug(D_DNS, "looking up addr %s", addr);

	if(!string_to_ip_address(addr, (unsigned char *) &iaddr)) {
		debug(D_DNS, "%s is not a valid addr", addr);
		return 0;
	}
	saddr.sin_addr = iaddr;
	saddr.sin_family = AF_INET;

	if ((err = getnameinfo((struct sockaddr *) &saddr, sizeof(saddr), host, sizeof(host), NULL, 0, 0)) != 0){
		debug(D_DNS, "couldn't look up %s: %s", addr, gai_strerror(err));
		return 0;
	}
	strcpy(name, host);
	debug(D_DNS, "%s is %s", addr, name);
	
	return 1;
}

int domain_name_lookup(const char *name, char *addr)
{
	struct addrinfo hints;
	struct addrinfo *result, *resultptr;
	char ipstr[LINK_ADDRESS_MAX];
	int err;
	
	debug(D_DNS, "looking up name %s", name);
	
	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_family = AF_UNSPEC;    /* Allow IPv4 or IPv6 */

	if ((err = getaddrinfo(name, NULL, &hints, &result)) != 0) {
		debug(D_DNS, "couldn't look up %s: %s", name, gai_strerror(err));
		return 0;
	}

	for (resultptr = result; resultptr != NULL; resultptr = resultptr->ai_next) {
		void *ipaddr;
		
		/* For ipv4 use struct sockaddr_in and sin_addr field;
		   for ipv6 use struct sockaddr_in6 and sin6_addr field. */
		// But right now, only find ipv4 address.
		if (resultptr->ai_family == AF_INET) { 
			struct sockaddr_in *addr_ipv4 = (struct sockaddr_in *)resultptr->ai_addr;
			ipaddr = &(addr_ipv4->sin_addr);
			inet_ntop(resultptr->ai_family, ipaddr, ipstr, sizeof(ipstr));
			debug(D_DNS, "%s is %s", name, ipstr);
			break;	
		}
	}
	strcpy(addr, ipstr);
	freeaddrinfo(result);
	
	return 1;
}
