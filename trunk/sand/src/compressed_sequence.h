/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef COMPRESSED_SEQUENCE_H
#define COMPRESSED_SEQUENCE_H

#include <string.h>
#include <math.h>
#include <stdio.h>

#include "sequence.h"

struct cseq
{
	char * name;
	char * metadata;
	short * data;
	int num_bases;
};

struct cseq * cseq_create( const char *name, int num_bases, short *data, const char *metadata);
struct cseq * cseq_copy(struct cseq *s);
struct cseq * seq_compress( struct seq *s );
struct seq  * cseq_uncompress( struct cseq * c );
void          cseq_free( struct cseq * c );
void          cseq_write( FILE *file, struct cseq *c );
struct cseq * cseq_read( FILE *file );
void          cseq_file_reset();
size_t        cseq_size( struct cseq *c );
int           cseq_sprint( char *buf, struct cseq *c, const char *extra_data );

void translate_to_str(int mer, char * str, int length);
int base_to_num(char base );
char num_to_base( int num );

#endif
