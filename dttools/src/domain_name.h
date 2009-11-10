/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DOMAIN_NAME_H
#define DOMAIN_NAME_H

/** @file domain_name.h
Look up domain names and addresses directly.
Most applications should use @ref domain_name_cache.h,
which uses an internal cache to perform lookups quickly.
*/

/** Maximum number of characters in a domain name or address */
#define DOMAIN_NAME_MAX 256

/** Resolve a domain name to an IP address.
@param name A string containing a domain name like "www.google.com".
@param addr A string where the IP address will be written.
@return One on success, zero on failure.
*/

int domain_name_lookup( const char *name, char *addr );

/** Resolve an IP address to a domain name with caching.
@param addr A string containing an IP address like "202.5.129.1"
@param name A string where the domain name will be written.
@return One on success, zero on failure.
*/

int domain_name_lookup_reverse( const char *addr, char *name );

#endif
