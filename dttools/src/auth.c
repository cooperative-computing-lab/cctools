/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>

struct auth_ops {
	char *type;
	auth_assert_t assert;
	auth_accept_t accept;
	struct auth_ops *next;
};

struct auth_state {
	struct auth_ops *ops;
};

static struct auth_state state;

static struct auth_ops *type_lookup(char *type)
{
	struct auth_ops *a;

	for(a = state.ops; a; a = a->next) {
		if(!strcmp(a->type, type))
			return a;
	}

	return 0;
}

/*
Regardless of what the individual authentication modules do,
we need to have sanitized subject names that don't have spaces,
newlines, and other crazy stuff like that.
*/

static void auth_sanitize(char *s)
{
	while(*s) {
		if(isspace((int) (*s)) || !isprint((int) (*s))) {
			*s = '_';
		}
		s++;
	}

}

int auth_assert(struct link *link, char **type, char **subject, time_t stoptime)
{
	char line[AUTH_LINE_MAX];
	struct auth_ops *a;

	for(a = state.ops; a; a = a->next) {
		debug(D_AUTH, "requesting '%s' authentication", a->type);

		if (link_putfstring(link, "%s\n", stoptime, a->type) <= 0)
			return 0;

		if(!link_readline(link, line, AUTH_LINE_MAX, stoptime))
			return 0;
		if(!strcmp(line, "yes")) {
			debug(D_AUTH, "server agrees to try '%s'", a->type);
			if(a->assert(link, stoptime)) {
				debug(D_AUTH, "successfully authenticated");
				if(!link_readline(link, line, AUTH_LINE_MAX, stoptime))
					return 0;
				if(!strcmp(line, "yes")) {
					debug(D_AUTH, "reading back auth info from server");
					if(!link_readline(link, line, sizeof(line), stoptime))
						return 0;
					*type = xxstrdup(line);
					if(!link_readline(link, line, sizeof(line), stoptime))
						return 0;
					*subject = xxstrdup(line);
					auth_sanitize(*subject);
					debug(D_AUTH, "server thinks I am %s:%s", *type, *subject);
					return 1;
				} else {
					debug(D_AUTH, "but not authorized to continue");
				}
			} else {
				debug(D_AUTH, "failed to authenticate");
			}
		} else {
			debug(D_AUTH, "server refuses to try '%s'", a->type);
		}
		debug(D_AUTH, "still trying...");
	}

	debug(D_AUTH, "ran out of authenticators");
	errno = EACCES;

	return 0;
}

int auth_accept(struct link *link, char **typeout, char **subject, time_t stoptime)
{
	struct auth_ops *a;
	char type[AUTH_TYPE_MAX];
	char node[HOST_NAME_MAX];
	char serv[128];

	*subject = 0;

	if(!link_getpeername(link, node, sizeof(node), serv, sizeof(serv), NI_NUMERICHOST|NI_NUMERICSERV)) {
		debug(D_AUTH, "couldn't get peer name");
		return 0;
	}

	while(link_readline(link, type, AUTH_TYPE_MAX, stoptime)) {
		string_chomp(type);

		debug(D_AUTH, "%s:%s requests '%s' authentication", node, serv, type);

		a = type_lookup(type);
		if(a) {
			debug(D_AUTH, "I agree to try '%s' ", type);
			if (link_putliteral(link, "yes\n", stoptime) <= 0)
				return 0;
		} else {
			debug(D_AUTH, "I do not agree to '%s' ", type);
			if (link_putliteral(link, "no\n", stoptime) <= 0)
				return 0;
			continue;
		}

		if(a->accept(link, subject, stoptime)) {
			auth_sanitize(*subject);
			debug(D_AUTH, "'%s' authentication succeeded", type);
			debug(D_AUTH, "%s:%s is %s:%s\n", node, serv, type, *subject);
			if (link_putfstring(link, "yes\n%s\n%s\n", stoptime, type, *subject) <= 0)
				return 0;
			*typeout = xxstrdup(type);
			return 1;
		} else {
			debug(D_AUTH, "%s:%s could not authenticate using '%s'", node, serv, type);
		}
		debug(D_AUTH, "still trying");
	}

	debug(D_AUTH, "%s:%s disconnected", node, serv);

	return 0;
}


int auth_barrier(struct link *link, const char *response, time_t stoptime)
{
	char line[AUTH_LINE_MAX];

	if (link_putstring(link, response, stoptime) <= 0)
		return 0;

	if(link_readline(link, line, sizeof(line), stoptime)) {
		if(!strcmp(line, "yes")) {
			return 1;
		}
	}

	return 0;
}

int auth_register(char *type, auth_assert_t assert, auth_accept_t accept)
{
	struct auth_ops *a = (struct auth_ops *) malloc(sizeof(struct auth_ops));
	if(!a)
		return 0;

	a->type = type;
	a->assert = assert;
	a->accept = accept;
	a->next = 0;

	if(!state.ops) {
		state.ops = a;
	} else {
		/* inserts go at the tail of the list */
		struct auth_ops *l = state.ops;
		while(l->next) {
			l = l->next;
		}
		l->next = a;
	}

	return 1;
}

void auth_clear()
{
	while(state.ops) {
		struct auth_ops *n = state.ops->next;
		free(state.ops);
		state.ops = n;
	}
}

struct auth_state *auth_clone (void)
{
	struct auth_state *clone = xxmalloc(sizeof(struct auth_state));
	struct auth_ops **opsp;
	*clone = state;
	for (opsp = &clone->ops; *opsp; opsp = &(*opsp)->next) {
		struct auth_ops *copy = xxmalloc(sizeof(struct auth_ops));
		*copy = **opsp;
		*opsp = copy;
	}
	return clone;
}

void auth_replace (struct auth_state *new)
{
	auth_clear();
	state = *new;
}

/* vim: set noexpandtab tabstop=4: */
