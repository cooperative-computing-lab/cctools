/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_UUID_H
#define CCTOOLS_UUID_H

/* RFC 4122 V4 */
#define UUID_LEN (sizeof "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx" - 1)
typedef struct {
	char str[UUID_LEN + 1 /* for NUL */];
} cctools_uuid_t;

void cctools_uuid_create (cctools_uuid_t *uuid);
void cctools_uuid_loadhex (cctools_uuid_t *uuid, const char *hex);

#endif
