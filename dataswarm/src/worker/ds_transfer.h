#ifndef DS_TRANSFER_H
#define DS_TRANSFER_H

#include "link.h"

int ds_transfer_send_any( struct link *lnk, const char *filename );

int ds_transfer_recv_dir( struct link *lnk, char *dirname );
int ds_transfer_recv_file( struct link *lnk, char *filename, int64_t length, int mode );

int ds_transfer_get_any( struct link *lnk );

#endif
