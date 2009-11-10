/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <sys/types.h>

#ifndef XMALLOC_H
#define XMALLOC_H

void * xxmalloc( size_t nbytes );
char * xstrdup( const char *str );

#endif
