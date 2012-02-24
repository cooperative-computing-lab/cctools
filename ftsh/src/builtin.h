/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BUILTIN_H
#define BUIlTIN_H

#include <time.h>

typedef int (*builtin_func_t) ( int line, int argc, char **argv, time_t stoptime );

builtin_func_t builtin_lookup( const char *name );

#endif
