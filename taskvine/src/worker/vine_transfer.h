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

/** Get any named filesystem item (file, directory, symlink) using the recursive transfer protocol.
@param lnk The network link to use.
@param cache The cache object where the file is located.
@param source_path The name of the file to fetch from the remote host.
@param cachename The name of the entry in the local cache.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int vine_transfer_get_any( struct link *lnk, struct vine_cache *cache, const char *source_path, const char *cachename, time_t stoptime );

/** Get a directory using the recursive transfer protocol.
This presumes that the directory header message has already
been read off the wire by the caller.
@param lnk The network link to use.
@param cache The cache object where the file is located.
@param dirname The name of the directory, relative to the cache object.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int vine_transfer_get_dir( struct link *lnk, struct vine_cache *cache, const char *dirname, time_t stoptime );

/** Get a single file using the recursive transfer protocol.
This presumes that the file header message has already
been read off the wire by the caller.
@param lnk The network link to use.
@param cache The cache object where the file is located.
@param filename The name of the filename, relative to the cache object.
@param length The length of the file in bytes.
@param mode The Unix mode bits of the file.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int vine_transfer_get_file( struct link *lnk, struct vine_cache *cache, const char *filename, int64_t length, int mode, time_t stoptime );

#endif
