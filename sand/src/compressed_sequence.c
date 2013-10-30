/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdlib.h>

#include "compressed_sequence.h"
#include "full_io.h"
#include "debug.h"

static int get_num_bytes(int num_bases);
static int get_malloc_bytes(int num_bases);

/*
This module has a lot of manifest constants that are related to the size
of the integer type (short) used for binary encoding the data.  It's not
clear to me that short is a good choice here, but to change it, we first
must track down all of the constants that are connected to it.
*/

/*
Special Note: num_bytes refers to the number of bytes used to store
the compressed representation on disk.  When allocating memory, we
must round that number up to the nearest integer-aligned size, in
order to avoid running off the end while doing arithmetic.  Hence,
you see get_mallc_bytes used to round it up.
*/

struct cseq * cseq_create( const char *name, int num_bases, short *mers, const char *metadata)
{
	struct cseq *c = malloc(sizeof(*c));
	c->name = strdup(name);
	c->num_bases = num_bases;
	int malloc_bytes = get_malloc_bytes(c->num_bases);
	c->data = malloc(malloc_bytes);
	memcpy(c->data,mers,malloc_bytes);
	c->metadata = strdup(metadata);
	return c;
}

struct cseq *cseq_copy( struct cseq *c )
{
	return cseq_create(c->name,c->num_bases,c->data,c->metadata);
}

struct cseq * seq_compress( struct seq *s )
{
	struct cseq  *c;

	c = malloc(sizeof(*c));
	if(!c) return 0;

	c->num_bases = s->num_bases;
	int malloc_bytes = get_malloc_bytes(s->num_bases);
	c->data = malloc(malloc_bytes);
	c->name = strdup(s->name);
	c->metadata = strdup(s->metadata);

	memset(c->data,0,malloc_bytes);

	int i=0, j=0, shift=14;

	while(i<s->num_bases) {
		c->data[j] |= (base_to_num(s->data[i]) << shift);
		i++;
		shift-=2;
		if(shift<0) {
			shift=14;
			j++;
		}
	}

	return c;
}

static int get_num_bytes( int num_bases )
{
	int num_bytes = num_bases/4;
	if (num_bases%4 > 0) { num_bytes++; }
	return num_bytes;
}

static int get_malloc_bytes( int num_bases )
{
	int num_bytes = get_num_bytes(num_bases);
	int remainder = num_bytes % sizeof(short);
	if(remainder!=0) {
		num_bytes += sizeof(short) - remainder;
	}
	return num_bytes;
}

int base_to_num(char base)
{
	switch(base)
	{
		case 'C':
		case 'c':
			return 0;
		case 'A':
		case 'a':
			return 1;
		case 'T':
		case 't':
			return 2;
		case 'G':
		case 'g':
		default:
			return 3;
	}
}

char num_to_base(int num)
{
	switch(num)
	{
		case 0: return 'C';
		case 1: return 'A';
		case 2: return 'T';
		case 3: return 'G';
		default: return 'N';
	}
}

struct seq * cseq_uncompress( struct cseq *c )
{
	struct seq *s = malloc(sizeof(*s));

	s->name = strdup(c->name);
	s->metadata = strdup(c->metadata);
	s->data = malloc(c->num_bases+1);
	s->num_bases = c->num_bases;

	int i=0, j=0, shift=14;

	while(i<s->num_bases) {
		s->data[i] = num_to_base( (c->data[j] >> shift) & 3);
		i++;
		shift-=2;
		if(shift<0) {
			shift=14;
			j++;
		}
	}

	s->data[s->num_bases] = 0;

	return s;
}

void cseq_free( struct cseq *c )
{
	if(c) {
		if (c->data)     free(c->data);
		if (c->name)     free(c->name);
		if (c->metadata) free(c->metadata);
		free(c);
	}
}

size_t cseq_size( struct cseq *c)
{
	return get_num_bytes(c->num_bases) + 100;
}

void cseq_write( FILE * file, struct cseq *c )
{
	if(!c) {
		// special case: null pointer indicates the end of a list.
		fprintf(file,">>\n");
	} else {
		int num_bytes = get_num_bytes(c->num_bases);
		fprintf(file, ">%s %d %d %s\n", c->name, c->num_bases, num_bytes, c->metadata);
		fwrite(c->data,1,num_bytes,file);
		fputc('\n',file);
	}
}

int cseq_sprint( char *buf, struct cseq *c, const char *extra_data )
{
	int total = 0;

	if(!c) {
		// special case: null pointer indicates the end of a list.
		total += sprintf(buf,">>\n");
	} else {
		int num_bytes = get_num_bytes(c->num_bases);

		total += sprintf(buf, ">%s %d %d %s %s\n", c->name, c->num_bases, num_bytes, c->metadata, extra_data);
		buf += total;

		memcpy(buf,c->data,num_bytes);
		buf += num_bytes;
		total += num_bytes;

		strcpy(buf,"\n");
		buf += 1;
		total += 1;
	}

	return total;
}

struct cseq * cseq_read( FILE *file )
{
	char line[SEQUENCE_FILE_LINE_MAX];
	char metadata[SEQUENCE_FILE_LINE_MAX];
	char name[SEQUENCE_FILE_LINE_MAX];
	int  nbases, nbytes;

	if(!fgets(line,sizeof(line),file)) return 0;

	// special case: two arrows indicates the end of a list,
	// but not the end of a file.

	if(line[0] == '>' && line[1] == '>') return 0;

	metadata[0] = 0;

	int n = sscanf(line, ">%s %d %d %[^\n]",name,&nbases,&nbytes,metadata);
	if(n<3) fatal("syntax error near %s\n",line);

	// allocate memory, rounding up to the next word size
	int malloc_bytes = get_malloc_bytes(nbases);
	short *data = malloc(malloc_bytes);

	// set the last full word to zero, to avoid uninitialized data.
	data[(malloc_bytes/sizeof(short))-1] = 0;

	n = full_fread(file,data,nbytes);
	if(n!=nbytes) fatal("sequence file is corrupted.");

	fgetc(file);

	struct cseq *c = cseq_create(name,nbases,data,metadata);

	free(data);

	return c;
}


/* vim: set noexpandtab tabstop=4: */
