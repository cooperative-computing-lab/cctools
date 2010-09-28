/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <math.h>
#include <stdio.h>

#include "align.h"
#include "sequence.h"

#ifndef __SEQUENCE_COMPRESSION_H_
#define __SEQUENCE_COMPRESSION_H_

struct cseq
{
	char * name;
	char * metadata;
	short * data;
	int num_bases;
	int num_bytes;
};

struct cseq * cseq_create( const char *name, int num_bases, int num_bytes, short *data, const char *metadata);
struct cseq * cseq_copy(struct cseq *s);
struct cseq * seq_compress( struct seq *s );
struct seq  * cseq_uncompress( struct cseq * c );
void          cseq_free( struct cseq * c );
void          cseq_print( FILE *file, struct cseq *c );
struct cseq * cseq_read( FILE *file );
void          cseq_file_reset();
size_t        cseq_size( struct cseq *c );
size_t        cseq_sprint( char * buf, struct cseq *c );

void translate_to_str(int mer, char * str, int length);
int base_to_num(char base );
char num_to_base( int num );

#endif
