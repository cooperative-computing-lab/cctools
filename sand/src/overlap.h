/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef OVERLAP_H
#define OVERLAP_H

#include <stdio.h>

#include "align.h"

void celera_ovl_write(FILE * file, struct alignment *aln, const char * id1, const char * id2);
void overlap_write_begin( FILE * file );
void celera_ovlmsg_write( FILE *file, struct alignment *a, const char *name1, const char *name2 );
void overlap_write_end( FILE * file );

#endif
