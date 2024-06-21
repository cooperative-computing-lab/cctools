/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "interfaces_address.h"

#ifndef HAS_IFADDRS

struct jx *interfaces_of_host()
{
	return NULL;
}

#else

#include <errno.h>
#include <ifaddrs.h>
#include <string.h>

#include <sys/types.h>

#include "address.h"
#include "debug.h"
#include "stringtools.h"

struct jx *interfaces_of_host()
{
	struct ifaddrs *ifa, *head_if;

	char address[IP_ADDRESS_MAX];

	if (getifaddrs(&head_if) == -1) {
		warn(D_NOTICE, "Could not get network interfaces information: %s", strerror(errno));
		return NULL;
	}

	struct addrinfo hints;
	address_check_mode(&hints);

	struct jx *interfaces = NULL;
	for (ifa = head_if; ifa != NULL; ifa = ifa->ifa_next) {
		if (ifa->ifa_addr == NULL) {
			continue;
		}

		int family = ifa->ifa_addr->sa_family;
		if (hints.ai_family != AF_UNSPEC && hints.ai_family != family) {
			continue;
		}

		if (string_prefix_is(ifa->ifa_name, "lo")) {
			continue;
		}

		int result = address_from_sockaddr(address, ifa->ifa_addr);
		if (!result) {
			warn(D_NOTICE, "Could not determine address of interface '%s': %s", ifa->ifa_name, gai_strerror(result));
			continue;
		}

		if (!interfaces) {
			interfaces = jx_array(0);
		}

		struct jx *jf = jx_object(0);

		jx_insert_string(jf, "interface", ifa->ifa_name);
		jx_insert_string(jf, "address", address);

		switch (family) {
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
