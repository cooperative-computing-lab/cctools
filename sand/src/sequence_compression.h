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

struct s_cseq
{
	char * ext_id;
	int id;
	char * metadata;
	short * mers;
	int length;
	int mercount;
};

typedef struct s_cseq cseq;

cseq compress_seq(seq s);
seq uncompress_seq(cseq m);
void free_cseq(cseq m);
void print_cseq(FILE * file, cseq c);
cseq get_next_cseq(FILE * file);
seq  get_next_seq(FILE *file);
void cseq_file_reset();
size_t cseq_size(cseq c);
size_t sprint_cseq(char * buf, cseq c);

void translate_to_str(int mer, char * str, int length);
int base_to_num(char base );
char num_to_base( int num );

#endif // __SEQUENCE_COMPRESSION_H_
