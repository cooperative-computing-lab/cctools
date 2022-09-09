/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DS_GPUS_H
#define DS_GPUS_H

void ds_gpus_init( int ngpus );
void ds_gpus_debug();
void ds_gpus_free( int taskid );
void ds_gpus_allocate( int n, int task );
char *ds_gpus_to_string( int taskid );

#endif
