/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_worker_info.h"
#include "uuid.h"

#define VINE_FILE_SOURCE_MAX_TRANSFERS 1

struct vine_transfer_pair {
    struct vine_worker_info *to;
    char   *source;
};

char *vine_current_transfers_add(struct vine_manager *q,struct vine_worker_info *to, char *source);

int vine_current_transfers_remove(struct vine_manager *q,char *id);

int vine_current_transfers_source_in_use(struct vine_manager *q,char *source);

int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w);

void vine_current_transfers_print_table(struct vine_manager *q);

void vine_current_transfers_delete( struct vine_transfer_pair *p );
