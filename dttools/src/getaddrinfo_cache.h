/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#ifndef GETADDRINFO_CACHE_H
#define GETADDRINFO_CACHE_H

#include <netdb.h>

int getaddrinfo_cache (const char *node, const char *service, const struct addrinfo *hints, struct addrinfo **res);

#endif

/* vim: set noexpandtab tabstop=4: */
