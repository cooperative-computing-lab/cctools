#include <string.h>
#include <stdlib.h>
#include "compress.h"
#include "align_macros.h"


#define MAX_ID 100
#define MAX_METADATA 100

static short mer_add_base(short mer, char base);
static short translate_8mer(const char * str, int start);
static void print_mer(FILE * file, int mer);
static int get_mercount(int length);

cseq compress_seq(seq s)
{
	static int id = 1;
	cseq m;
	char * str = s.seq;
	int len = s.length;
	int mercount = get_mercount(len);
	int curr_8mer;

	m.length = len;
	m.mercount = mercount;
	m.mers = malloc(mercount*sizeof(short));
	m.id = id++;
	
	if (s.id != 0)
	{
		m.ext_id = malloc((strlen(s.id)+1)*sizeof(char));
		strcpy(m.ext_id, s.id);
	}
	else { m.ext_id = 0; }

	if (s.metadata != 0)
	{
		m.metadata = malloc((strlen(s.metadata)+1)*sizeof(char));
		strcpy(m.metadata, s.metadata);
	}
	else { m.metadata = 0; }

	if (s.seq != 0)
	{
		for (curr_8mer=0; curr_8mer < mercount; curr_8mer++)
		{
			m.mers[curr_8mer] = translate_8mer(s.seq, curr_8mer*8);
		}
	}
	else
	{
		m.mers = 0;
	}

	return m;
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
	//print_mer(stderr, mer);
	return mer;
}

static short mer_add_base(short mer, char base)
{
	return (mer << 2) + base_to_num(base);
}
#ifndef UMD_COMPRESSION
int base_to_num(char base)
{
	switch(base)
	{
		case 'A':
		case 'a':
			return 0;
		case 'C':
		case 'c':
			return 1;
		case 'G':
		case 'g':
			return 2;
		case 'T':
		case 't':
			return 3;
	}
}
char num_to_base(int num)
{
	switch(num)
	{
		case 0: return 'A';
		case 1: return 'C';
		case 2: return 'G';
		case 3: return 'T';
		default: return 'N';
	}
}
#else
static int base_to_num(char base)
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
#endif

seq uncompress_seq(cseq m)
{
	seq s;

	if (m.ext_id != 0)
	{
		s.id = malloc((strlen(m.ext_id)+1)*sizeof(char));
		strcpy(s.id, m.ext_id);
	}
	else { s.id = 0; }

	if (m.metadata != 0)
	{
		s.metadata = malloc((strlen(m.metadata)+1)*sizeof(char));
		strcpy(s.metadata, m.metadata);
	}
	else { s.metadata = 0; }

	s.length = m.length;

	int mercount = m.length/8;

	if (m.mers != 0)
	{
		s.seq = malloc((m.length+1)*sizeof(char));
		s.seq[0] = '\0';
		int i;
		char tempstr[9];
		for (i=0; i<mercount; i++)
		{
			translate_to_str(m.mers[i], tempstr, 8);
			//fprintf(stderr, "m.mers[%d]: %d, tempstr: %s\n", i, m.mers[i], tempstr);
			strcat(s.seq, tempstr);
		}
		if (m.length % 8 > 0)
		{
			// handle the overflow if it doesn't divide by 8 exactly.
			translate_to_str(m.mers[i], tempstr, m.length%8);
			//fprintf(stderr, "m.mers[%d]: %d, tempstr: %s\n", i, m.mers[i], tempstr);
			strcat(s.seq, tempstr);
		} 
		s.seq[s.length] = '\0';
	}
	else { s.seq = 0; }

	return s;
}

void translate_to_str(int mer, char * str, int length)
{
	//print_mer(stderr, mer);
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
		//fprintf(stderr, "i:%d, mer >> %i & 3: %d\n", i, (length-1)-i, (mer >> (length-1)-i) & 3);
	}
	str[length] = '\0';
}


static void print_mer(FILE * file, int mer)
{
	fprintf(file, "%d\n", mer);
}

void free_cseq(cseq m)
{
	if (m.mers) { free(m.mers); m.mers = 0; }
	if (m.ext_id) { free(m.ext_id); m.ext_id = 0; }
	if (m.metadata) { free(m.metadata); m.metadata = 0; }
}

void print_cseq(FILE * file, cseq c)
{
	fprintf(file, ">%s %d %d %s\n", c.ext_id, c.length, c.mercount*sizeof(short), c.metadata);

	fwrite(c.mers, sizeof(short), c.mercount, file);
	fputc('\n',file);
}

static char line[MAX_STRING] = "";
static int count = 0;
void cseq_file_reset()
{
	count = 0;
	strcpy(line, "");
}

cseq get_next_cseq(FILE * file)
{

	cseq sequence;

	// Read the first line.
	if (count == 0)
	{
		fgets(line, MAX_STRING, file);
		count = 1;
	}

	if (line[0] == '>' && line[1] == '>')
	{
		sequence.mers = 0;
		sequence.ext_id = 0;
		sequence.metadata = 0;
		sequence.length = 0;

		// Read ahead the next line.
		fgets(line, MAX_STRING, file);

		// Get the next line in the file for the next iteration to start with.
		return sequence;
	}
	else if (line[0] == '>')
	{
		sequence.ext_id = malloc(MAX_ID*sizeof(char));
		sequence.metadata = malloc(MAX_METADATA*sizeof(char));
		strcpy(sequence.metadata, "");
		sequence.length = 0;
		sequence.mercount = 0;
	}
	else
	{
		fprintf(stderr, "Error parsing compressed sequences. Expected '>' and received %c (ASCII %d)\n", line[0], (int) line[0]);
		exit(1);
	}

	int bytes;
	//parse_header(&sequence, line);
	sscanf(line, ">%s %d %d %s\n", sequence.ext_id, &sequence.length, &bytes,sequence.metadata);
	sequence.mercount = get_mercount(sequence.length);
	sequence.mers = malloc(sequence.mercount*sizeof(short));

	char last;
	fread(sequence.mers, sizeof(short), sequence.mercount, file);
	fread(&last, sizeof(char), 1, file);  // Get the newline at the end of each sequence.

	// Read ahead the next line.
	fgets(line, MAX_STRING, file);
	count++;

	return sequence;
}






