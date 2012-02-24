/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the password COPYING for details.
*/

#ifndef PASSWORD_CACHE_H
#define PASSWORD_CACHE_H

#include <sys/types.h>

#include "int_sizes.h"

struct password_cache {
	char *username;
	char *password;
};

struct password_cache *password_cache_init(const char *uname, const char *pwd);
void password_cache_delete(struct password_cache *c);
void password_cache_cleanup(struct password_cache *c);
int password_cache_register(struct password_cache *p, const char *uname, const char *pwd);
int password_cache_full(struct password_cache *c);

#endif
