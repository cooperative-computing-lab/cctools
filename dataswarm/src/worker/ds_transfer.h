#ifndef DS_TRANSFER_H
#define DS_TRANSFER_H

#include "ds_cache.h"
#include "link.h"

int ds_transfer_put_any( struct link *lnk, struct ds_cache *cache, const char *filename, time_t stoptime );

int ds_transfer_get_dir( struct link *lnk, struct ds_cache *cache, const char *dirname, time_t stoptime );
int ds_transfer_get_file( struct link *lnk, struct ds_cache *cache, const char *filename, int64_t length, int mode, time_t stoptime );
int ds_transfer_get_any( struct link *lnk, struct ds_cache *cache, const char *dirname, const char *filename, time_t stoptime );

#endif
