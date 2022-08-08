#ifndef DS_SANDBOX_H
#define DS_SANDBOX_H

#include "ds_process.h"
#include "ds_cache.h"
#include "link.h"

int ds_sandbox_stagein( struct ds_process *p, struct ds_cache *c, struct link *manager );
int ds_sandbox_stageout( struct ds_process *p, struct ds_cache *c );

#endif
