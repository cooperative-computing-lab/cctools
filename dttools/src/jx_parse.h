#ifndef JX_PARSE_H
#define JX_PARSE_H

#include "jx.h"
#include <stdio.h>

jx_t * jx_parse_string( const char *str );
jx_t * jx_parse_file( FILE *file );

#endif
