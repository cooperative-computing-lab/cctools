#ifndef JX_PARSE_H
#define JX_PARSE_H

#include "jx.h"
#include <stdio.h>

struct jx * jx_parse_string( const char *str );
struct jx * jx_parse_file( FILE *file );

#endif
