#ifndef JX_BINARY_H
#define JX_BINARY_H

#include <stdio.h>
#include "jx.h"

int jx_binary_write( FILE *stream, struct jx *j );
struct jx * jx_binary_read( FILE *stream );

#endif
