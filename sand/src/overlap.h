#ifndef OVERLAP_H
#define OVERLAP_H

#include <stdio.h>

#include "align.h"

void overlap_write_begin( FILE * file );
void overlap_write( FILE *file, struct alignment *a, const char *name1, const char *name2 );
void overlap_write_end( FILE * file );

#endif
