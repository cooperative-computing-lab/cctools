/*
Copyright (C) 2024- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MANAGER_FACTORY_H
#define VINE_MANAGER_FACTORY_H

#include "vine_manager.h"
#include "vine_worker_info.h"
#include "vine_factory_info.h"

#include <time.h>

/*
This module handles workers that report themselves
as coming from a particular factory, then allowing
the manager to query the catalog for the factory
status and remove idle workers that overflow the user's
desired limit.
*/

int  vine_manager_factory_worker_arrive( struct vine_manager *q, struct vine_worker_info *w, const char *factory_name );
void vine_manager_factory_worker_leave( struct vine_manager *q, struct vine_worker_info *w );
int  vine_manager_factory_worker_prune( struct vine_manager *q, struct vine_worker_info *w );
void vine_manager_factory_update_all(struct vine_manager *q, time_t stoptime);

#endif
