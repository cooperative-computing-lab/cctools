/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_XPUS_H
#define VINE_XPUS_H

struct vine_xpus * vine_xpus_create( const char *name, int count );
void vine_xpus_debug( struct vine_xpus *x );
void vine_xpus_alloc( struct vine_xpus *x, int taskid, int n );
void vine_xpus_free( struct vine_xpus *x, int taskid );
char * vine_xpus_to_string( struct vine_xpus *x, int taskid );

#endif
