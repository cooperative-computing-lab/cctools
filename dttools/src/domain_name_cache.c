/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "domain_name_cache.h"
#include "hash_cache.h"
#include "debug.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/utsname.h>

/* cache domain names for up to five minutes */
#define DOMAIN_NAME_CACHE_LIFETIME 300

static struct hash_cache *name_to_addr = 0;
static struct hash_cache *addr_to_name = 0;

static int domain_name_cache_init()
{
	if(!name_to_addr) {
		name_to_addr = hash_cache_create(127, hash_string, free);
		if(!name_to_addr) {
			return 0;
		}
	}

	if(!addr_to_name) {
		addr_to_name = hash_cache_create(127, hash_string, free);
		if(!addr_to_name) {
			return 0;
		}
	}

	return 1;
}

int domain_name_cache_canonical(const char *name_or_addr, char *cname)
{
	char addr[DOMAIN_NAME_MAX];
	return domain_name_cache_lookup(name_or_addr, addr) && domain_name_cache_lookup_reverse(addr, cname);
}

int domain_name_cache_lookup(const char *name, char *addr)
{
	char *found, *copy;
	int success;

	if(!domain_name_cache_init())
		return 0;

	found = hash_cache_lookup(name_to_addr, name);
	if(found) {
		strcpy(addr, found);
		return 1;
	}

	success = domain_name_lookup(name, addr);
	if(!success)
		return 0;

	copy = strdup(addr);
	if(!copy)
		return 1;

	success = hash_cache_insert(name_to_addr, name, copy, DOMAIN_NAME_CACHE_LIFETIME);

	return 1;
}

int domain_name_cache_lookup_reverse(const char *addr, char *name)
{
	char *found, *copy;
	int success;

	if(!domain_name_cache_init())
		return 0;

	found = hash_cache_lookup(addr_to_name, addr);
	if(found) {
		strcpy(name, found);
		return 1;
	}

	success = domain_name_lookup_reverse(addr, name);
	if(!success)
		return 0;

	copy = strdup(name);
	if(!copy)
		return 1;

	success = hash_cache_insert(addr_to_name, addr, copy, DOMAIN_NAME_CACHE_LIFETIME);

	return 1;
}

static int guess_dns_domain(char *domain)
{
	char line[DOMAIN_NAME_MAX];
	FILE *file;

	file = fopen("/etc/resolv.conf", "r");
	if(!file)
		return 0;

	while(fgets(line, sizeof(line), file)) {
		if(sscanf(line, "search %[^ \t\n]", domain) == 1) {
			fclose(file);
			return 1;
		}
		if(sscanf(line, "domain %[^ \t\n]", domain) == 1) {
			fclose(file);
			return 1;
		}
	}

	fclose(file);
	return 0;
}

int domain_name_cache_guess(char *name)
{
	struct utsname n;
	char addr[DOMAIN_NAME_MAX];
	char domain[DOMAIN_NAME_MAX];

	if(uname(&n) < 0)
		return 0;

	if(!domain_name_cache_lookup(n.nodename, addr))
		return 0;
	if(!domain_name_cache_lookup_reverse(addr, name))
		return 0;

	debug(D_DNS, "finding my hostname: uname = %s, address = %s, hostname = %s", n.nodename, addr, name);

	if(!strncmp(name, "localhost", 9) || !strcmp(addr, "127.0.0.1")) {
		debug(D_DNS, "local address of '%s' (%s) is not very useful.", name, addr);
		if(guess_dns_domain(domain)) {
			sprintf(name, "%s.%s", n.nodename, domain);
			debug(D_DNS, "but /etc/resolv.conf says domain = %s so hostname = %s", domain, name);
			if(!domain_name_cache_lookup(name, addr)) {
				debug(D_DNS, "unfortunately %s is meaningless, so going back to %s", name, n.nodename);
				sprintf(name, "%s", n.nodename);
			}
		} else {
			strcpy(name, n.nodename);
			debug(D_DNS, "cannot find any more info, so use hostname = %s", n.nodename);
		}
	}

	return 1;
}

static char shortname[DOMAIN_NAME_MAX];
static int got_shortname = 0;

int domain_name_cache_guess_short(char *name)
{
	struct utsname n;

	if(got_shortname) {
		strcpy(name, shortname);
		return 1;
	}

	if(uname(&n) < 0)
		return 0;

	strcpy(shortname, n.nodename);

	char *p = strchr(shortname, '.');
	if(p)
		*p = 0;

	strcpy(name, shortname);
	got_shortname = 1;

	return 1;
}

/* vim: set noexpandtab tabstop=8: */
