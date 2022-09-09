/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_SANDBOX_H
#define DS_SANDBOX_H

#include "ds_process.h"
#include "ds_cache.h"
#include "link.h"

int ds_sandbox_stagein( struct ds_process *p, struct ds_cache *c, struct link *manager );
int ds_sandbox_stageout( struct ds_process *p, struct ds_cache *c );

#endif
