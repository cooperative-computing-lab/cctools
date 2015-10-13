/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "getaddrinfo_cache.h"

#include "debug.h"
#include "hash_cache.h"

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>

#include <limits.h>
#include <stdio.h>

static struct hash_cache *H;
#define TTL (5*60)

static int init (void)
{
	if (!H) {
		H = hash_cache_create(128, hash_string, (hash_cache_cleanup_t)freeaddrinfo);
		if (!H)
			return 0;
	}
	return 1;
}

int getaddrinfo_cache (const char *node, const char *service, const struct addrinfo *hints, struct addrinfo **res)
{
	char key[HOST_NAME_MAX + 256 /* |port|family|socktype|protocol */];
	struct addrinfo cachehints;

	if (!init()) return EAI_SYSTEM;

	cachehints = *hints;
	cachehints.ai_flags |= AI_CANONNAME; /* get the canonical name */
	snprintf(key, sizeof(key), "%s|%s|%d|%d|%d", node, service, cachehints.ai_family, cachehints.ai_socktype, cachehints.ai_protocol);
	*res = hash_cache_lookup(H, key);
	if (*res) {
		debug(D_DNS, "getaddrinfo cache hit for (%s, %s)", node, service);
		return 0;
	} else {
		debug(D_DNS, "getaddrinfo cache miss for (%s, %s)", node, service);
		int rc = getaddrinfo(node, service, &cachehints, res);
		if (rc == 0) {
			hash_cache_insert(H, key, *res, TTL); /* ignore failures */
			return 0;
		} else {
			debug(D_DNS, "getaddrinfo: %s", gai_strerror(rc));
			return rc;
		}
	}
}

/* vim: set noexpandtab tabstop=4: */
