/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BUFFER_H
#define BUFFER_H

int buffer_open_input( const char *name );
int buffer_open_output( const char *name );
int buffer_open_append( const char *name );

char * buffer_load( const char *name );
int    buffer_save( const char *name, const char *data );
int    buffer_delete( const char *name );

#endif
