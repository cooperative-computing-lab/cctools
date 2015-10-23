/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_PARSE_H
#define JX_PARSE_H

#include "jx.h"
#include <stdio.h>

struct jx * jx_parse_string( const char *str );
struct jx * jx_parse_file( FILE *file );

#endif
