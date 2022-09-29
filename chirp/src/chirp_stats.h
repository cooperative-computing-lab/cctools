/**
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_STATS_H
#define CHIRP_STATS_H

#include "jx.h"

void chirp_stats_collect( const char *addr, const char *subject, uint64_t ops, uint64_t bytes_read, uint64_t bytes_written );
void chirp_stats_summary( struct jx *j );
void chirp_stats_cleanup();

void chirp_stats_update( uint64_t ops, uint64_t bytes_read, uint64_t bytes_written );
void chirp_stats_report( int pipefd, const char *addr, const char *subject, int interval );

#endif

/* vim: set noexpandtab tabstop=4: */
