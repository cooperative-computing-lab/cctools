#ifndef SEQUENCE_H
#define SEQUENCE_H

#include <stdio.h>

struct seq
{
	char *name;
	char *data;
	char *metadata;
	int   num_bases;
};

struct seq * seq_create( const char *name, const char *data, const char *metadata );
struct seq * seq_read( FILE *file );
void         seq_print( FILE * file, struct seq *s );
int          seq_sprint( char *buf, struct seq *s );
void         seq_reverse_complement( struct seq *s );
void         seq_free( struct seq *s );

int    sequence_count( FILE *file );

#define SEQUENCE_FILE_LINE_MAX 1024

#endif
