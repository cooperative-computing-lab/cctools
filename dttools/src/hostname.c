/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "getaddrinfo_cache.h"
#include "hostname.h"
#include "macros.h"

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/utsname.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int getcanonical (const char *nodename, char *canonical, size_t l)
{
	int rc;
	struct addrinfo *addr;
	struct addrinfo hints;

	memset(&hints, 0, sizeof(hints));
	hints.ai_flags = AI_CANONNAME;
	rc = getaddrinfo_cache(nodename, NULL, &hints, &addr);
	if (rc) {
		debug(D_DNS, "getaddrinfo: %s", gai_strerror(rc));
		return (errno = EINVAL, -1);
	}

	assert(addr && addr->ai_canonname);
	rc = snprintf(canonical, l, "%s", addr->ai_canonname);
	debug(D_DNS, "node '%s' canonical hostname is '%s'", nodename, canonical);

	freeaddrinfo(addr);
	return rc;
}

int getcanonicalhostname (char *canonical, size_t l)
{
	struct utsname n;

	if (uname(&n) == -1) {
		debug(D_DNS, "uname: %s", strerror(errno));
		return -1;
	}

	return getcanonical(n.nodename, canonical, l);
}

int getshortname (char *shortname, size_t l)
{
	struct utsname n;

	if (uname(&n) == -1) {
		debug(D_DNS, "uname: %s", strerror(errno));
		return -1;
	}

	memset(shortname, 0, l);
	memcpy(shortname, n.nodename, MIN(l-1, strcspn(n.nodename, ".")));

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
