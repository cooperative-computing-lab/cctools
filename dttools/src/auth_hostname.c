/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

static int auth_hostname_assert(struct link *link, time_t stoptime)
{
	char line[AUTH_LINE_MAX];

	if(!link_readline(link, line, sizeof(line), stoptime)) {
		debug(D_AUTH, "hostname: lost connection");
		return 0;
	}

	if(!strcmp(line, "yes")) {
		debug(D_AUTH, "hostname: accepted");
		return 1;
	}

	return 0;
}

static int auth_hostname_accept(struct link *link, char **subject, time_t stoptime)
{
	char node[HOST_NAME_MAX];
	char serv[128];

	if(!link_getpeername(link, node, sizeof(node), serv, sizeof(serv), NI_NUMERICSERV)) {
		debug(D_AUTH, "hostname: couldn't get peer name");
		goto reject;
	}

	*subject = xxstrdup(node);

	link_putliteral(link, "yes\n", stoptime);
	return 1;
reject:
	link_putliteral(link, "no\n", stoptime);
	return 0;
}

int auth_hostname_register(void)
{
	debug(D_AUTH, "hostname: registered");
	return auth_register("hostname", auth_hostname_assert, auth_hostname_accept);
}

/* vim: set noexpandtab tabstop=4: */
