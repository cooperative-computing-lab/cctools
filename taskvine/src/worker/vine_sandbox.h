/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_SANDBOX_H
#define VINE_SANDBOX_H

#include "vine_process.h"
#include "vine_cache.h"
#include "link.h"

vine_cache_status_t vine_sandbox_ensure( struct vine_process *p, struct vine_cache *c, struct link *manager );
int vine_sandbox_stagein( struct vine_process *p, struct vine_cache *c);
int vine_sandbox_stageout( struct vine_process *p, struct vine_cache *c, struct link *manager );

#endif
