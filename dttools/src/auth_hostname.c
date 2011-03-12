/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth.h"
#include "debug.h"
#include "domain_name_cache.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

static int auth_hostname_assert( struct link *link, struct hash_table *t, time_t stoptime )
{
	char line[AUTH_LINE_MAX];

	if(!link_readline(link,line,sizeof(line),stoptime)) {
		debug(D_AUTH,"hostname: lost connection");
		return 0;
	}

	if(!strcmp(line,"yes")) {
		debug(D_AUTH,"hostname: accepted");
		return 1;
	}

	return 0;
}

static int auth_hostname_accept( struct link *link, char **subject, struct hash_table *t, time_t stoptime )
{
	char addr[LINK_ADDRESS_MAX];
	char name[DOMAIN_NAME_MAX];
	int port;

	if(!link_address_remote(link,addr,&port)) {
		debug(D_AUTH,"hostname: couldn't get address of link");
		goto reject;
	}

	if(!domain_name_cache_lookup_reverse(addr,name)) {
		debug(D_AUTH,"hostname: couldn't look up name of %s",name);
		goto reject;
	}

	*subject = strdup(name);
	if(!*subject) {
		debug(D_AUTH,"hostname: out of memory");
		goto reject;
	}

	link_putliteral(link,"yes\n",stoptime);
	return 1;

	reject:
	link_putliteral(link,"no\n",stoptime);
	return 0;
}

int auth_hostname_register()
{
	debug(D_AUTH,"hostname: registered");
	return auth_register( "hostname", auth_hostname_assert, auth_hostname_accept );
}

