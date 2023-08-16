/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the password COPYING for details.
*/

#include "password_cache.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

struct password_cache *password_cache_init(const char *uname, const char *pwd)
{
	struct password_cache *p = malloc(sizeof(*p));
	if(!p)
		return 0;

	p->username = strdup(uname);
	p->password = strdup(pwd);
	return p;
}

void password_cache_delete(struct password_cache *p)
{
	if(p) {
		password_cache_cleanup(p);
		free(p);
	}
}

void password_cache_cleanup(struct password_cache *p)
{
	int len;

	len = strlen(p->username);
	memset(p->username, 0, len);
	free(p->username);
	p->username = NULL;

	len = strlen(p->password);
	memset(p->password, 0, len);
	free(p->password);
	p->password = NULL;
}

int password_cache_register(struct password_cache *p, const char *uname, const char *pwd)
{
	if(!p)
		return -1;
	password_cache_cleanup(p);
	p->username = strdup(uname);
	p->password = strdup(pwd);
	return 0;
}

int password_cache_full(struct password_cache *c)
{
	if(c->username && c->password)
		return 1;
	else
		return 0;
}

/* vim: set noexpandtab tabstop=8: */
