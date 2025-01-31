/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TRANSFER_SERVER_H
#define VINE_TRANSFER_SERVER_H

#include "vine_cache.h"
#include "link.h"

/* This number defines the maximum allowable concurrent forking processes for file transfers. However, it is the manager's 
 * responsibility to allocate transfer tasks efficiently among workers, to ensure that no worker excessively forks processes 
 * to complete the job. In this case, this value serves more as a theoretical safety threshold and should never be reached under 
 * normal conditions. If a worker reaches this limit, it indicates a bug on the manager's side. */
#define VINE_TRANSFER_PROC_MAX_CHILD 128

void vine_transfer_server_start( struct vine_cache *cache, int port_min, int port_max );
void vine_transfer_server_stop();
void vine_transfer_server_address( char *addr, int *port );

#endif
