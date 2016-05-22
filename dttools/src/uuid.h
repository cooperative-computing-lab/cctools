/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef UUID_H
#define UUID_H

/* RFC 4122 V4 */
#define UUID_LEN (sizeof "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx" - 1)
typedef struct {
	char str[UUID_LEN + 1 /* for NUL */];
} uuid;

void uuid_create (uuid *uuid);
void uuid_loadhex (uuid *uuid, const char *hex);

#endif
