/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DOMAIN_NAME_CACHE_H
#define DOMAIN_NAME_CACHE_H

#include "domain_name.h"

/** @file domain_name_cache.h
Look up domain names and addresses quickly.
These routines resolve domain names using an internal cache,
allowing for much faster response time than the plain @ref domain_name.h routines.
*/

/** Determine the caller's primary domain name.
This function uses a variety of sources, including uname, the local hosts file,
and the domain name system, to determine the caller's primary domain name.
If this function returns an unexpected name, try running with the @ref debug flags
set to @ref D_DNS to observe exactly who th ename was determined.
@param name Pointer to buffer where name will be stored.
@returns One on success, zero on failure.
*/

int domain_name_cache_guess( char *name );

/** Determine the caller's local machine name.
This function uses the built in facility to determine the
local host name of the machine, without involving DNS.
If the local name has been configured to look like a domain name,
only the first segment of the name will be returned.
@param name Pointer to buffer where name will be stored.
@returns One on success, zero on failure.
*/

int domain_name_cache_guess_short( char *name );

/** Resolve a domain name to an IP address with caching.
@param name A string containing a domain name like "www.google.com".
@param addr A string where the IP address will be written.
@return One on success, zero on failure.
*/

int domain_name_cache_lookup( const char *name, char *addr );

/** Resolve an IP address to a domain name with caching.
@param addr A string containing an IP address like "202.5.129.1"
@param name A string where the domain name will be written.
@return One on success, zero on failure.
*/

int domain_name_cache_lookup_reverse( const char *addr, char *name );

/** Find the canonical name of a host.
@param name_or_addr A string containing a domain name or ip address.
@param cname A string where the canonical domain name will be written.
@return One on success, zero on failure.
*/

int domain_name_cache_canonical( const char *name_or_addr, char *cname );

#endif
