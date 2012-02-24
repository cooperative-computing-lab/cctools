/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "stringtools.h"
#include "debug.h"
#include "full_io.h"

#include "sequence.h"

struct seq * seq_create( const char *name, const char *data, const char *metadata )
{
	struct seq *s = malloc(sizeof(*s));

	s->name = strdup(name);
	s->data = strdup(data);
	s->metadata = strdup(metadata);
	s->num_bases = strlen(data);

	return s;
}

static char base_complement(char c)
{
	switch(c)
	{
		case 'A':
		case 'a':
			return 'T';
		case 'T':
		case 't':
			return 'A';
		case 'G':
		case 'g':
			return 'C';
		case 'C':
		case 'c':
			return 'G';
		default:
			return 'N';
	}
}

void seq_reverse_complement( struct seq *s )
{
	char * str = s->data;
	int num_bases = s->num_bases;
	char c_i, c_j;
	int i, j;

	for(i=0, j=num_bases-1; i <= j; i++, j--)
	{
		c_i = str[i];
		c_j = str[j];
		str[j] = base_complement(c_i);
		str[i] = base_complement(c_j);
	}
}

void seq_free( struct seq *s )
{
	if(s) {
	      free(s->name);
	      free(s->data);
	      free(s->metadata);
	      free(s);
	}
}

int seq_sprint(char * buf, struct seq *s )
{
	return sprintf(buf,">%s %s\n%s\n",s->name,s->metadata,s->data);
}

void seq_print( FILE *file, struct seq *s )
{
	fprintf(file,">%s %s\n%s\n",s->name,s->metadata,s->data);
}

struct seq * seq_read( FILE *file )
{
	static char *buffer = 0;
	static int  buffer_size = SEQUENCE_FILE_LINE_MAX;

	char line[SEQUENCE_FILE_LINE_MAX];
	char name[SEQUENCE_FILE_LINE_MAX];
	char metadata[SEQUENCE_FILE_LINE_MAX];

	if(!fgets(line,sizeof(line),file)) return 0;

	// special case: >> indicates the end of a list
	if(line[0]=='>' && line[1]=='>') return 0;

	metadata[0] = 0;

	int n = sscanf(line, ">%s %[^\n]\n",name,metadata);
	if(n<1) fatal("syntax error near: %s\n",line);

	if(!buffer) buffer = malloc(buffer_size);

	int num_bases = 0;

	while(1) {
		int c = getc_unlocked(file);
		if(isspace(c)) continue;
		if(c==EOF) break;
		if(c=='>') {
			ungetc(c,file);
			break;
		}
		buffer[num_bases++] = toupper(c);
		if(num_bases>(buffer_size-2)) {
			buffer_size *= 2;
			buffer = realloc(buffer,buffer_size);
		}
	}

	buffer[num_bases] = 0;

	return seq_create(name,buffer,metadata);
}

int sequence_count(FILE * file)
{
	int count = 0;
	char line[SEQUENCE_FILE_LINE_MAX];
	long int start_pos = ftell(file);

	while(fgets(line,sizeof(line),file)) {
		if(line[0] == '>') count++;
	}

	fseek(file, start_pos, SEEK_SET);
	return count;
}
