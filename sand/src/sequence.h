#ifndef SEQUENCE_H
#define SEQUENCE_H

/*
This is code in transition from a horrible mess to a
slightly less horrible mess.  We had two implementations
of sequence objects, all mixed up.  For clarity, they
have been brought together here in a single module with
two namespaces: sequence_*() for the binary representation
and seq_*() for the text representation.  A future version
will attempt to unify these and adjust the corresponding code.
*/

#include <stdio.h>

struct sequence {
	char *name;
	int num_bases;
	int num_bytes;
	unsigned char *data;
	char *metadata;
};

struct sequence * sequence_create( const char *name, int num_bases, int num_bytes, unsigned char *data, const char *metadata );
struct sequence * sequence_copy( struct sequence *s );
struct sequence * sequence_read_binary( FILE *file );
void              sequence_delete( struct sequence *s );

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
