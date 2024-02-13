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

/** Put any named filesystem item (file, directory, symlink) using the recursive transfer protocol.
@param lnk The network link to use.
@param cache The cache object where the file is located.
@param filename The name of the file, relative to the cache object.
@param mode Controls whether any item will be sent, or just a file.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int vine_transfer_put_any( struct link *lnk, struct vine_cache *cache, const char *filename, vine_transfer_mode_t mode, time_t stoptime );

/** Receive any named filesystem item (file, directory, symlink) using the recursive transfer protocol.
@param lnk The network link to use.
@param dirname The directory in which to place the object.
@param totalsize To be filled in with the total size of the transfer.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int vine_transfer_get_any(struct link *lnk, const char *dirname, int64_t *totalsize, time_t stoptime);

/** Fetch any named item by requesting its name and then reading it back on the socket using the recursive transfer protocol.
@param lnk The network link to use.
@param dirname The directory in which to place the object.
@param totalsize To be filled in with the total size of the transfer.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int vine_transfer_request_any(struct link *lnk, const char *request_name, const char *dirname, int64_t *totalsize, time_t stoptime);

#endif
