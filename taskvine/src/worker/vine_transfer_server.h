/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TRANSFER_SERVER_H
#define VINE_TRANSFER_SERVER_H

#include "vine_cache.h"
#include "link.h"

#define VINE_TRANSFER_PROC_MAX_CHILD 8

void vine_transfer_server_start( struct vine_cache *cache );
void vine_transfer_server_stop();
void vine_transfer_server_address( char *addr, int *port );

#endif
