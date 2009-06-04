#ifndef CHIRP_STATS_H
#define CHIRP_STATS_H

#include "int_sizes.h"
#include "link.h"

struct chirp_stats {
	char     addr[LINK_ADDRESS_MAX];
	UINT32_T active_clients;
	UINT32_T total_connections;
	UINT32_T total_ops;
	UINT32_T pad;
	UINT64_T bytes_read;
	UINT64_T bytes_written;
};


void                 chirp_stats_init();
struct chirp_stats * chirp_stats_global();
struct chirp_stats * chirp_stats_local_begin( const char *address );
void		     chirp_stats_local_end( struct chirp_stats *s );
void                 chirp_stats_cleanup();
void                 chirp_stats_summary( char *buf, int length );
void                 chirp_stats_sync();

#endif
