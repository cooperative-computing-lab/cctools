/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_GPUS_H
#define VINE_GPUS_H

void vine_gpus_init( int ngpus );
void vine_gpus_debug();
void vine_gpus_free( int task_id );
void vine_gpus_allocate( int n, int task );
char *vine_gpus_to_string( int task_id );

#endif
