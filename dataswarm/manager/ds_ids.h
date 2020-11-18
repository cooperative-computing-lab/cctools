#ifndef DS_IDS_H
#define DS_IDS_H

#include "ds_manager.h"

char *ds_create_taskid( struct ds_manager *m );
char *ds_create_fileid( struct ds_manager *m );
char *ds_create_blobid( struct ds_manager *m );
char *ds_create_serviceid( struct ds_manager *m );

#endif
