/**
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_STATS_H
#define CHIRP_STATS_H

#include "jx.h"
#include "int_sizes.h"

void chirp_stats_collect( const char *addr, const char *subject, UINT64_T ops, UINT64_T bytes_read, UINT64_T bytes_written );
void chirp_stats_summary( struct jx *j );
void chirp_stats_cleanup();

void chirp_stats_update( UINT64_T ops, UINT64_T bytes_read, UINT64_T bytes_written );
void chirp_stats_report( int pipefd, const char *addr, const char *subject, int interval );

#endif

/* vim: set noexpandtab tabstop=8: */
