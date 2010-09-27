#ifndef SEQUENCE_H
#define SEQUENCE_H

#include <stdio.h>

struct s_seq
{
	char * id;
	char * seq;
	char * metadata;
	int length;
};

typedef struct s_seq seq;

void   seq_reverse_complement( seq * s);
void   seq_print( FILE * file, seq s);
seq    seq_read( FILE * file );
void   seq_free( seq s);
size_t seq_sprint( char * buf, seq s);
int    sequence_count( FILE * file);

#endif
