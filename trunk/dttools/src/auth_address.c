/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
/*
Copyright (C) 2003-2004 Douglas Thain
Copyright (C) 2005- The University of Notre Dame
This work is made available under the GNU General Public License.
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

static int auth_address_assert(struct link *link, time_t stoptime)
{
	char line[AUTH_LINE_MAX];

	if(!link_readline(link, line, sizeof(line), stoptime)) {
		debug(D_AUTH, "address: lost connection");
		return 0;
	}

	if(!strcmp(line, "yes")) {
		debug(D_AUTH, "address: accepted");
		return 1;
	}

	return 0;
}

static int auth_address_accept(struct link *link, char **subject, time_t stoptime)
{
	char addr[LINK_ADDRESS_MAX];
	int port;

	if(!link_address_remote(link, addr, &port)) {
		debug(D_AUTH, "address: couldn't get address of link");
		goto reject;
	}

	*subject = strdup(addr);
	if(!*subject) {
		debug(D_AUTH, "address: out of memory");
		goto reject;
	}

	link_putliteral(link, "yes\n", stoptime);
	return 1;

      reject:
	link_putliteral(link, "no\n", stoptime);
	return 0;
}

int auth_address_register(void)
{
	debug(D_AUTH, "address: registered");
	return auth_register("address", auth_address_assert, auth_address_accept);
}
