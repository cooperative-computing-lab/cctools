/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_TRANSFER_SERVER_H
#define DS_TRANSFER_SERVER_H

#include "ds_cache.h"
#include "link.h"

void ds_transfer_server_start( struct ds_cache *cache );
void ds_transfer_server_stop();
void ds_transfer_server_address( char *addr, int *port );

#endif
