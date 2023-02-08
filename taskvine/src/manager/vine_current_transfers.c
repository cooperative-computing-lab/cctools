/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager.h"
#include "vine_current_transfers.h"

#include "debug.h"

static struct vine_transfer_pair *vine_transfer_pair_create(struct vine_worker_info *to, const char *source)
{
    struct vine_transfer_pair *t = malloc(sizeof(struct vine_transfer_pair));
    t->to = to;
    t->source = strdup(source);
    return t;
}

// add a current transaction to the transfer table
char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *to, const char *source)
{
    cctools_uuid_t uuid;
    cctools_uuid_create(&uuid);

    char *transfer_id = strdup(uuid.str);  
    struct vine_transfer_pair *t = vine_transfer_pair_create(to, source);

    hash_table_insert(q->current_transfer_table, transfer_id, t);
    return transfer_id;
}

// remove a completed transaction from the transfer table - i.e. open the source to an additional transfer
int vine_current_transfers_remove(struct vine_manager *q, const char *id)
{
    if(hash_table_remove(q->current_transfer_table, id)) return 1;
    return 0;
}


// count the number transfers coming from a specific source
int vine_current_transfers_source_in_use(struct vine_manager *q, const char *source)
{
    char *id;
    struct vine_transfer_pair *t;
    int c = 0;
    HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
    {
    	if(strcmp(source, t->source) == 0) c++;
    }
    return c;
}

// remove all transactions involving a worker from the transfer table - if a worker failed or is being deleted intentionally
int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w)
{
    char *id;
    struct vine_transfer_pair *t;
    debug(D_VINE, "Removing instances of worker from transfer table");
    HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
    {
    	if(t->to == w)
	{
		vine_current_transfers_remove(q, id);
	}
    }
    return 1;
}

void vine_current_transfers_print_table(struct vine_manager *q)
{ 
    char *id;
    struct vine_transfer_pair *t;
    debug(D_VINE, "-----------------TRANSFER-TABLE--------------------");
    HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
    {	    	
    	debug(D_VINE, "%s : source=%s", id, t->source);
    }
    debug(D_VINE, "-----------------END-------------------------------");
}

// for use in hash_table_delete
void vine_current_transfers_delete( struct vine_transfer_pair *p )
{
	if (p) {
		free(p->source);
	}
}



