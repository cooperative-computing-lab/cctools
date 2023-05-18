#ifndef VINE_WORKER_H
#define VINE_WORKER_H

/* Public interface to various items in vine_worker.c */

#include "timestamp.h"
#include "link.h"

void vine_worker_send_cache_update( struct link *manager, const char *cachename, int64_t size, timestamp_t transfer_time, timestamp_t transfer_start );
void vine_worker_send_cache_invalid( struct link *manager, const char *cachename, const char *message );

extern int vine_worker_symlinks_enabled;
extern char *vine_worker_password;

#endif
