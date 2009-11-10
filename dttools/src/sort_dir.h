/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SORTDIR_H
#define SORTDIR_H

int sort_dir( const char *dirname, char ***list, int (*sort) ( const char *a, const char *b ) );
void sort_dir_free( char **list );

#endif
