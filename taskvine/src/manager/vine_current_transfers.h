/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_worker_info.h"
#include "uuid.h"

#define VINE_FILE_SOURCE_MAX_TRANSFERS 1
#define VINE_WORKER_SOURCE_MAX_TRANSFERS 3 // static 1 until if/when multiple transfer ports are opened up on worker transfer server

char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *to, const char *source);

int vine_current_transfers_remove(struct vine_manager *q, const char *id);

int vine_current_transfers_source_in_use(struct vine_manager *q,const char *source);

int vine_current_transfers_worker_in_use(struct vine_manager *q,const char *peer_addr);

int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w);

void vine_current_transfers_print_table(struct vine_manager *q);

void vine_current_transfers_clear( struct vine_manager *q );
