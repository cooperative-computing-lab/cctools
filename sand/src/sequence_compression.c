/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdlib.h>

#include "sequence_compression.h"
#include "sand_align.h"

#define MAX_ID SEQUENCE_ID_MAX
#define MAX_METADATA SEQUENCE_METADATA_MAX 

static short mer_add_base(short mer, char base);
static short translate_8mer(const char * str, int start);
static int get_mercount(int length);

cseq seq_compress(seq s)
{
	cseq m;
	int len = s.length;
	int mercount = get_mercount(len);
	int curr_8mer;

	m.length = len;
	m.mercount = mercount;
	m.mers = malloc(mercount*sizeof(short));
	m.name = 0;
	m.metadata = 0;
	
	if(s.id)       m.name = strdup(s.id);
	if(s.metadata) m.metadata = strdup(s.metadata);

	if (s.seq != 0)
	{
		for (curr_8mer=0; curr_8mer < mercount; curr_8mer++)
		{
			m.mers[curr_8mer] = translate_8mer(s.seq, curr_8mer*8);
		}
	} else {
		m.mers = 0;
	}

	return m;
}

int get_mercount(int length)
{
	int mercount = length/8;
	if (length%8 > 0) { mercount++; }

	return mercount;
}

short translate_8mer(const char * str, int start)
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

seq cseq_uncompress(cseq m)
{
	seq s;

	memset(&s,0,sizeof(s));

	if(m.name)   s.id = strdup(m.name);
	if(m.metadata) s.metadata = strdup(m.metadata);

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
			strcat(s.seq, tempstr);
		}
		if (m.length % 8 > 0)
		{
			// handle the overflow if it doesn't divide by 8 exactly.
			translate_to_str(m.mers[i], tempstr, m.length%8);
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


void cseq_free(cseq m)
{
	if (m.mers) { free(m.mers); m.mers = 0; }
	if (m.name) { free(m.name); m.name = 0; }
	if (m.metadata) { free(m.metadata); m.metadata = 0; }
}

size_t cseq_size(cseq c)
{
	char tmp[255];
	size_t size = 0;

	// Get the size of the header line.
	size += sprintf(tmp, ">%s %d %d %s\n", c.name, c.length, (int)(c.mercount*sizeof(short)), c.metadata);
	/*
	size += strlen(c.name);
	size += floor(log(c.length)+1);
	size += floor(log(c.mercount*sizeof(short)));
	size += strlen(c.metadata);
	size += 5;  // The '>', spaces and newline.
	*/
	// Add the number of mers (each a short)
	size += c.mercount*sizeof(short);
	// Add one more for the newline at the end.
	size++;

	return size;
}

size_t cseq_sprint(char * buf, cseq c)
{
	char * ins = buf;
	int res;
	size_t size = 0;
	res = sprintf(ins, ">%s %d %d %s\n", c.name, c.length, (int)(c.mercount*sizeof(short)), c.metadata);
	ins += res;
	size += res;
	memcpy(ins, c.mers, c.mercount*sizeof(short));
	ins += c.mercount*sizeof(short);
	size += c.mercount*sizeof(short);
	sprintf(ins, "\n");
	size++;

	return size;
}

void cseq_print(FILE * file, cseq c)
{
	fprintf(file, ">%s %d %d %s\n", c.name, c.length, (int)(c.mercount*sizeof(short)), c.metadata);

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

cseq cseq_read(FILE * file)
{
	int total;
	cseq sequence;
	char tmp_id[MAX_ID];
	char tmp_metadata[MAX_METADATA];

	// Read the first line.
	if (count == 0)
	{
		fgets(line, MAX_STRING, file);
		count = 1;
	}

	if (line[0] == '>' && line[1] == '>')
	{
		sequence.mers = 0;
		sequence.name = 0;
		sequence.metadata = 0;
		sequence.length = 0;

		// Read ahead the next line.
		fgets(line, MAX_STRING, file);

		// Get the next line in the file for the next iteration to start with.
		return sequence;
	}
	else if (line[0] == '>')
	{
		sequence.name = 0;
		sequence.metadata = 0;
		sequence.length = 0;
		sequence.mercount = 0;
	}
	else
	{
		fprintf(stderr, "Error parsing compressed sequences. Expected '>' and received %c (ASCII %d)\n", line[0], (int) line[0]);
		exit(1);
	}

	int bytes;

	tmp_metadata[0] = '\0';
	sscanf(line, ">%s %d %d %[^\n]\n", tmp_id, &sequence.length, &bytes,tmp_metadata);
	sequence.name = strdup(tmp_id);
	sequence.metadata = strdup(tmp_metadata);
	sequence.mercount = get_mercount(sequence.length);
	sequence.mers = malloc(sequence.mercount*sizeof(short));

	char last;
	total = fread(sequence.mers, 1, bytes, file);
	if (total != bytes)
	{
		fprintf(stderr, "ERROR: Did not read as much as was expected (expected %d, read %d, line: %s).\n", bytes, total, line);
		exit(1);
	}
	fread(&last, sizeof(char), 1, file);  // Get the newline at the end of each sequence.

	// Read ahead the next line.
	fgets(line, MAX_STRING, file);
	count++;

	return sequence;
}






