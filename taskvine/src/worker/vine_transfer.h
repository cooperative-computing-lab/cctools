/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TRANSFER_H
#define VINE_TRANSFER_H

#include "vine_cache.h"
#include "link.h"

typedef enum {
	VINE_TRANSFER_MODE_ANY,
	VINE_TRANSFER_MODE_FILE_ONLY
} vine_transfer_mode_t;

/* Send any cached file/dir along the connection to a remote party. */

int vine_transfer_put_any( struct link *lnk, struct vine_cache *cache, const char *filename, vine_transfer_mode_t mode, time_t stoptime );

/* Receive a named file/dir from the connection to a local transfer path. */

int vine_transfer_get_any(struct link *lnk, const char *dirname, int64_t *totalsize, int *mode, int *mtime, time_t stoptime, char **error_message);

/* Request an item by name, and then receive it in the same way as vine_transfer_get_any. */

int vine_transfer_request_any(struct link *lnk, const char *request_name, const char *dirname, int64_t *totalsize, int *mode, int *mtime, time_t stoptime, char **error_message);

#endif
