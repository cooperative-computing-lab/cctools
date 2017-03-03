/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "interfaces_address.h"

#ifndef HAS_IFADDRS

struct jx *interfaces_of_host() {
	return NULL;
}

#else

#include <errno.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <string.h>

#include <sys/socket.h>
#include <sys/types.h>

#include "debug.h"
#include "link.h"
#include "stringtools.h"

struct jx *interfaces_of_host() {
	struct ifaddrs *ifa, *head_if;

	char host[NI_MAXHOST];

	if(getifaddrs(&head_if) == -1 ) {
		warn(D_NOTICE, "Could not get network interfaces information: %s", strerror(errno));
		return NULL;
	}

	struct jx *interfaces = NULL;

	for(ifa = head_if; ifa != NULL; ifa = ifa->ifa_next) {
		if(ifa->ifa_addr == NULL) {
			continue;
		}

		int family = ifa->ifa_addr->sa_family;

		if(family != AF_INET && family != AF_INET6) {
			continue;
		}

		if(string_prefix_is(ifa->ifa_name, "lo")) {
			continue;
		}

		int size   = (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);
		int result = getnameinfo(ifa->ifa_addr, size, host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);

		if(result) {
			warn(D_NOTICE, "Could not determine address of interface '%s': %s", ifa->ifa_name, gai_strerror(result));
			continue;
		}

		if(!interfaces) {
			interfaces = jx_array(0);
		}

		struct jx *jf = jx_object(0);

		jx_insert_string(jf, "interface", ifa->ifa_name);
		jx_insert_string(jf, "host",      host);

		switch(family) {
			case AF_INET:
				jx_insert_string(jf, "family", "AF_INET");
				break;
			case AF_INET6:
				jx_insert_string(jf, "family", "AF_INET6");
				break;
			default:
				break;
		}

		jx_array_append(interfaces, jf);
	}

	freeifaddrs(head_if);

	return interfaces;
}

#endif
