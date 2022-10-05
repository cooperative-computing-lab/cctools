/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#ifndef DS_BLOCKLIST_H
#define DS_BLOCKLIST_H

#include "ds_manager.h"

struct ds_blocklist_info {
	int    blocked;
	int    times_blocked;
	time_t release_at;
};

/* Operations on individual blocklist entries. */

struct ds_blocklist_info * ds_blocklist_info_create();
void ds_blocklist_info_delete( struct ds_blocklist_info *b );

/* Operations on the blocklist as a whole. */

void       ds_blocklist_block( struct ds_manager *q, const char *hostname, time_t timeout );
void       ds_blocklist_unblock_all_by_time(struct ds_manager *q, time_t deadline);
void       ds_blocklist_unblock( struct ds_manager *q, const char *host );
int        ds_blocklist_is_blocked( struct ds_manager *q, const char *host );
struct jx *ds_blocklist_to_jx( struct ds_manager *q );

#endif
