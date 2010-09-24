#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "stringtools.h"
#include "debug.h"
#include "full_io.h"

#include "sequence.h"

#define SEQUENCE_FILE_LINE_MAX 4096

struct sequence * sequence_create( const char *name, int num_bases, int num_bytes, unsigned char *data, const char *metadata)
{
	struct sequence *s = malloc(sizeof(*s));
	s->name = strdup(name);
	s->num_bases = num_bases;
	s->num_bytes = num_bytes;
	s->data = malloc(s->num_bytes);
	memcpy(s->data,data,s->num_bytes);
	s->metadata = strdup(metadata);
	return s;
	
}

struct sequence *sequence_copy(struct sequence *s)
{
  return sequence_create(s->name,s->num_bases,s->num_bytes,s->data,s->metadata);
}

void sequence_delete( struct sequence *s )
{
	if(s) {
		free(s->name);
		free(s->data);
		free(s->metadata);
		free(s);
	}
}

struct sequence * sequence_read_binary( FILE *file )
{
	char line[SEQUENCE_FILE_LINE_MAX];
	char metadata[SEQUENCE_FILE_LINE_MAX];
	char name[SEQUENCE_FILE_LINE_MAX];
	int  nbases, nbytes;

	if(!fgets(line,sizeof(line),file)) return 0;

	int n = sscanf(line, ">%s %d %d%[^\n]%*1[\n]",name,&nbases,&nbytes,metadata);
	if(n!=4) fatal("syntax error near %s\n",line);

	unsigned char *data = malloc(nbytes);

	n = full_fread(file,data,nbytes);
	if(n!=nbytes) fatal("sequence file is corrupted.");

	fgetc(file);

	struct sequence *s = sequence_create(name,nbases,nbytes,data,metadata);

	free(data);
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

void seq_reverse_complement(seq * s)
{
	char * str = s->seq;
	int length;
	char c_i, c_j;
	int i, j;
	length = strlen(str);

	for(i=0, j=length-1; i <= j; i++, j--)
	{
		c_i = str[i];
		c_j = str[j];
		str[j] = base_complement(c_i);
		str[i] = base_complement(c_j);
	}
}

void seq_free(seq s)
{
	if (s.id) { free(s.id); s.id = 0; }
	if (s.seq) { free(s.seq); s.seq = 0; }
	if (s.metadata) { free(s.metadata); s.metadata = 0; }
}

size_t seq_sprint(char * buf, seq s)
{
	int res = 0;
	size_t size = 0;
	char * ins = buf;
	res = sprintf(ins, ">%s %d %d", s.id, s.length, s.length);
	ins += res;
	size += res;

	if (strlen(s.metadata) > 0)
	{
		res = sprintf(ins, " ");
		ins += res;
		size += res;
	}

	res = sprintf(ins, "%s\n%s\n", s.metadata, s.seq);
	ins += res;
	size += res;

	return size;
}

void seq_print(FILE * file, seq s)
{
	fprintf(file, ">%s %d %d", s.id, s.length, s.length);
	if (strlen(s.metadata) > 0) { fprintf(file, " "); }
	fprintf(file, "%s\n%s\n", s.metadata, s.seq);
}


static void seq_cat( seq * sequence, char * new_str)
{
	while (*new_str != '\0')
	{
		sequence->seq[sequence->length] = *new_str;
		sequence->length++;
		new_str++;
	}
	sequence->seq[sequence->length] = '\0';
}

static void seq_normalize( char *str )
{
	while (*str != '\0')
	{
		*str = toupper((int)*str);
		str++;
	}
	str--;
	if (*str == '\n') {
		*str = '\0';
	}
}

#define MAX_ID 100
#define MAX_METADATA 100
#define MAX_STRING 102048

seq seq_read(FILE * file)
{
	static char line[MAX_STRING] = "";
	static int count = 0;

	seq sequence;

	// Get the first line of the file, compile the regexp.
	if (count == 0)
	{
		fgets(line, MAX_STRING, file);
		count = 1;
	}

	sequence.seq = 0;
	sequence.id = 0;
	sequence.metadata = 0;
	sequence.length = 0;

	if (line[0] == '>' && line[1] == '>')
	{

		// Get the next line in the file for the next iteration to start with.
		fgets(line, MAX_STRING, file);
		return sequence;
	}else{//we need to allocate memory properly 
		sequence.seq = malloc(MAX_STRING*sizeof(char));
		sequence.id = malloc(MAX_ID*sizeof(char));
		sequence.metadata = malloc(MAX_METADATA*sizeof(char));
		sequence.length = 0;
	}
	
	strcpy(sequence.metadata, "");

	int bases;
	int bytes;

	sscanf(line, ">%s %d %d %[^\n]\n", sequence.id, &bases, &bytes, sequence.metadata);

	while (1)
	{
		fgets(line, MAX_STRING, file);
		if (line[0] == '>') { break; }
		if (feof(file)) { break; }

		seq_normalize(line);
		seq_cat(&sequence, line);
	}

	return sequence;
}

int sequence_count(FILE * file)
{
	int count = 0;
	char line[MAX_STRING];
	char id[MAX_STRING];
	int length, bytes;
	long int start_pos = ftell(file);

	while (!feof(file))
	{
		if (fgets(line, MAX_STRING, file) == 0) break;
		if ((line[0] == '>') && (line[1] != '>'))
		{
			count++;
			sscanf(line, ">%s %d %d", id, &length, &bytes);
			fseek(file, bytes+1, SEEK_CUR);
		}
	}
	fseek(file, start_pos, SEEK_SET);
	return count;
}
