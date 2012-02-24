/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef AUTH_H
#define AUTH_H

#include "link.h"

#define AUTH_SUBJECT_MAX 1024
#define AUTH_TYPE_MAX 1024
#define AUTH_LINE_MAX (AUTH_SUBJECT_MAX+AUTH_TYPE_MAX)

typedef int (*auth_assert_t) (struct link * l, time_t stoptime);
typedef int (*auth_accept_t) (struct link * l, char **subject, time_t stoptime);

int auth_assert(struct link *l, char **type, char **subject, time_t stoptime);
int auth_accept(struct link *l, char **type, char **subject, time_t stoptime);

int auth_barrier(struct link *l, const char *response, time_t stoptime);
int auth_register(char *type, auth_assert_t assert, auth_accept_t accept);

void auth_clear(void);

#endif
