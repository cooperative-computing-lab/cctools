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

#include <assert.h>
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
	int tries;
	struct addrinfo hints;

	debug(D_DNS, "looking up name %s", name);


	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_family = AF_INET; /* Currently only lookup IPv4 addresses */

	for (tries = 0; tries < 10; tries++) {
		struct addrinfo *result;
		if (tries) {
			sleep(1<<(tries-1));
		}
		int rc = getaddrinfo(name, NULL, &hints, &result);
		if (rc == 0) {
			struct addrinfo *iresult;
			/* N.B. this for loop is not really necessary since we always use the first result. */
			for (iresult = result; iresult; iresult = iresult->ai_next) {
				char ipstr[INET6_ADDRSTRLEN] = "";
				switch (iresult->ai_family) {
					case AF_INET:
						inet_ntop(AF_INET, &((struct sockaddr_in *)iresult->ai_addr)->sin_addr, ipstr, sizeof(ipstr));
						break;
					case AF_INET6:
						inet_ntop(AF_INET6, &((struct sockaddr_in6 *)iresult->ai_addr)->sin6_addr, ipstr, sizeof(ipstr));
						break;
					default: assert(0);
				}
				debug(D_DNS, "%s is %s", name, ipstr);
				strcpy(addr, ipstr);
				break;
			}
			freeaddrinfo(result);
			return 1;
		} else {
			debug(D_DNS, "couldn't look up %s: %s", name, gai_strerror(rc));

			/* glibc returns EAI_NONAME for many transient network failures.
			 * Unfortunately this gives us no information on how to proceed. If
			 * the network interface is temporarily down or if a packet get
			 * dropped, we should retry...
			 *
			 * This deficiency has been brought up in [1] but my (batrick) own
			 * experiments show that this continues to exist in glibc 2.20 for
			 * all settings of hints.ai_family.
			 *
			 * [1] https://bugzilla.redhat.com/show_bug.cgi?id=1044628
			 */

			if (!(rc == EAI_AGAIN || rc == EAI_NODATA || rc == EAI_NONAME || rc == EAI_SYSTEM))
				return 0; /* permanent failure */
		}
	}
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
