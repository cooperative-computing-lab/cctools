/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#ifndef VINE_BLOCKLIST_H
#define VINE_BLOCKLIST_H

#include "vine_manager.h"

struct vine_blocklist_info {
	int    blocked;
	int    times_blocked;
	time_t release_at;
};

/* Operations on individual blocklist entries. */

struct vine_blocklist_info * vine_blocklist_info_create();
void vine_blocklist_info_delete( struct vine_blocklist_info *b );

/* Operations on the blocklist as a whole. */

void       vine_blocklist_block( struct vine_manager *q, const char *hostname, time_t timeout );
void       vine_blocklist_unblock_all_by_time(struct vine_manager *q, time_t deadline);
void       vine_blocklist_unblock( struct vine_manager *q, const char *host );
int        vine_blocklist_is_blocked( struct vine_manager *q, const char *host );
struct jx *vine_blocklist_to_jx( struct vine_manager *q );

#endif
