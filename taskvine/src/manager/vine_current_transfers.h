/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_worker_info.h"
#include "uuid.h"

#define VINE_FILE_SOURCE_MAX_TRANSFERS 1
#define VINE_WORKER_SOURCE_MAX_TRANSFERS 10

char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *to, struct vine_worker_info *source_worker, const char *source_url);

int vine_current_transfers_remove(struct vine_manager *q, const char *id);

int vine_current_transfers_set_failure(struct vine_manager *q, char *id, const char *cachename);

void vine_current_transfers_set_success(struct vine_manager *q, char *id);

int vine_current_transfers_url_in_use(struct vine_manager *q, const char *source);

int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w);

void vine_current_transfers_print_table(struct vine_manager *q);

void vine_current_transfers_clear( struct vine_manager *q );

int vine_current_transfers_get_table_size(struct vine_manager *q);
