#ifndef DS_TRANSFER_H
#define DS_TRANSFER_H

#include "ds_cache.h"
#include "link.h"

/*
Put any named filesystem item (file, directory, symlink) using the recursive transfer protocol.
@param lnk The network link to use.
@param cache The cache object where the file is located.
@param filename The name of the file, relative to the cache object.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int ds_transfer_put_any( struct link *lnk, struct ds_cache *cache, const char *filename, time_t stoptime );

/*
Get any named filesystem item (file, directory, symlink) using the recursive transfer protocol.
@param lnk The network link to use.
@param cache The cache object where the file is located.
@param filename The name of the file, relative to the cache object.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int ds_transfer_get_any( struct link *lnk, struct ds_cache *cache, const char *filename, time_t stoptime );

/*
Get a directory using the recursive transfer protocol.
This presumes that the directory header message has already
been read off the wire by the caller.
@param lnk The network link to use.
@param cache The cache object where the file is located.
@param dirname The name of the directory, relative to the cache object.
@param stoptime The absolute Unix time at which to stop and accept failure.
@return Non-zero on success, zero on failure.
*/

int ds_transfer_get_dir( struct link *lnk, struct ds_cache *cache, const char *dirname, time_t stoptime );

/*
Get a single file using the recursive transfer protocol.
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

int ds_transfer_get_file( struct link *lnk, struct ds_cache *cache, const char *filename, int64_t length, int mode, time_t stoptime );

#endif
