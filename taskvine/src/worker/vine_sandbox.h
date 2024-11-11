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

char *vine_sandbox_full_path(struct vine_process *p, const char *sandbox_name);

vine_cache_status_t vine_sandbox_ensure( struct vine_process *p, struct vine_cache *c, struct link *manager, struct itable  *procs_table );

int vine_sandbox_stagein( struct vine_process *p, struct vine_cache *c);

/* void because stageout always succeeds. Let manager figure out missing outputs. Call only on reap_process! */
void vine_sandbox_stageout( struct vine_process *p, struct vine_cache *c, struct link *manager );

#endif
