/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdlib.h>

#include "sequence_compression.h"
#include "full_io.h"
#include "debug.h"

#define MAX_ID SEQUENCE_ID_MAX
#define MAX_METADATA SEQUENCE_METADATA_MAX 

static short mer_add_base(short mer, char base);
static short translate_8mer(const char * str, int start);
static int get_mercount(int length);

struct cseq * cseq_create( const char *name, int length, int mercount, short *mers, const char *metadata)
{
	struct cseq *c = malloc(sizeof(*c));
	c->name = strdup(name);
	c->length = length;
	c->mercount = mercount;
	c->mers = malloc(c->mercount);
	memcpy(c->mers,mers,c->mercount);
	c->metadata = strdup(metadata);
	return c;
	
}

struct cseq *cseq_copy( struct cseq *c )
{
	return cseq_create(c->name,c->length,c->mercount,c->mers,c->metadata);
}

struct cseq * seq_compress(seq s)
{
	struct cseq  *c;
	int len = s.length;
	int mercount = get_mercount(len);
	int curr_8mer;

	c = malloc(sizeof(*c));
	if(!c) return 0;

	c->length = len;
	c->mercount = mercount;
	c->mers = malloc(mercount*sizeof(short));
	c->name = 0;
	c->metadata = 0;
	
	if(s.id)       c->name = strdup(s.id);
	if(s.metadata) c->metadata = strdup(s.metadata);

	if (s.seq != 0)
	{
		for (curr_8mer=0; curr_8mer < mercount; curr_8mer++)
		{
			c->mers[curr_8mer] = translate_8mer(s.seq, curr_8mer*8);
		}
	} else {
		c->mers = 0;
	}

	return c;
}

static int get_mercount(int length)
{
	int mercount = length/8;
	if (length%8 > 0) { mercount++; }

	return mercount;
}

static short translate_8mer(const char * str, int start)
{
	int i;
	short mer = 0;

	for(i=start; i<start+8; i++)
	{
		if (str[i] == '\0') { return mer; }
		mer = mer_add_base(mer, str[i]);
	}
	return mer;
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

short mer_add_base(short mer, char base)
{
	return (mer << 2) + base_to_num(base);
}

seq cseq_uncompress( struct cseq *c )
{
	seq s;

	memset(&s,0,sizeof(s));

	if(c->name)   s.id = strdup(c->name);
	if(c->metadata) s.metadata = strdup(c->metadata);

	s.length = c->length;

	int mercount = c->length/8;

	if (c->mers != 0)
	{
		s.seq = malloc((c->length+1)*sizeof(char));
		s.seq[0] = '\0';
		int i;
		char tempstr[9];
		for (i=0; i<mercount; i++)
		{
			translate_to_str(c->mers[i], tempstr, 8);
			strcat(s.seq, tempstr);
		}
		if (c->length % 8 > 0)
		{
			// handle the overflow if it doesn't divide by 8 exactly.
			translate_to_str(c->mers[i], tempstr, c->length%8);
			strcat(s.seq, tempstr);
		} 
		s.seq[s.length] = '\0';
	} else {
		s.seq = 0;
	}

	return s;
}

void translate_to_str(int mer, char * str, int length)
{
	int i;

	// 2 bits represent each base. So, to get the first base, we the
	// two most significant bits. To get the second base, the two second
	// most significant bits, etc. In other, we need to bit shift all but
	// 2 (aka bitshift 14 to the right) when i = 0, bitshift 12 when i=1,
	// etc.
	// We mask by 3 to make sure we only have the two numbers and nothing
	// but 0's in the rest.
	for (i=0; i<length; i++)
	{
		str[i] = num_to_base((mer >> ((length-1)-i)*2) & 3);
	}
	str[length] = '\0';
}


void cseq_free( struct cseq *c )
{
	if(c) {
		if (c->mers)     free(c->mers);
		if (c->name)     free(c->name);
		if (c->metadata) free(c->metadata);
		free(c);
	}
}

size_t cseq_size( struct cseq *c)
{
	return c->mercount*sizeof(short) + 100;
}

void cseq_print( FILE * file, struct cseq *c )
{
	if(!c) {
		// special case: null pointer indicates the end of a list.
		fprintf(file,">>\n");
	} else {
		fprintf(file, ">%s %d %d %s\n", c->name, c->length, (int)(c->mercount*sizeof(short)), c->metadata);
		fwrite(c->mers, sizeof(short), c->mercount, file);
		fputc('\n',file);
	}
}

#define SEQUENCE_FILE_LINE_MAX 1024

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

	int n = sscanf(line, ">%s %d %d %[^\n]",name,&nbases,&nbytes,metadata);
	if(n<3) fatal("syntax error near %s\n",line);

	short *data = malloc(nbytes);

	n = full_fread(file,data,nbytes);
	if(n!=nbytes) fatal("sequence file is corrupted.");

	fgetc(file);

	struct cseq *c = cseq_create(name,nbases,nbytes,data,metadata);

	free(data);

	return c;
}

