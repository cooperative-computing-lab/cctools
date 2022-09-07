#ifndef DS_TRANSFER_H
#define DS_TRANSFER_H

#include "link.h"

int ds_transfer_put_any( struct link *lnk, const char *filename );

int ds_transfer_get_dir( struct link *lnk, const char *dirname, int64_t *totalsize );
int ds_transfer_get_file( struct link *lnk, const char *filename, int64_t length, int mode );
int ds_transfer_get_any( struct link *lnk, const char *dirname, const char *filename );

#endif
