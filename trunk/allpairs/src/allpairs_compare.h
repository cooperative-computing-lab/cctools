/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef ALLPAIRS_COMPARE_H
#define ALLPAIRS_COMPARE_H

typedef void (*allpairs_compare_t) ( const char *name1, const char *data1, int size1, const char *name2, const char *data2, int size2 );

allpairs_compare_t allpairs_compare_function_get( const char *name );

#endif
