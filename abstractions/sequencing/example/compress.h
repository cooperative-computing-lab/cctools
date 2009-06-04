#include <string.h>
#include <math.h>
#include <stdio.h>
#include "alignment.h"

#ifndef __COMPRESS_H_
#define __COMPRESS_H_

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

struct s_kmer
{
	cseq *m;
	int mer;
	short loc;
};

typedef struct s_kmer kmer;

struct s_merlist
{
	cseq *m;
	struct s_merlist * next;
};

typedef struct s_merlist merlist;

cseq compress_seq(seq s);
seq uncompress_seq(cseq m);
void free_cseq(cseq m);
void print_cseq(FILE * file, cseq c);
cseq get_next_cseq(FILE * file);
void cseq_file_reset();
void translate_to_str(int mer, char * str, int length);
char num_to_base(int num);
int base_to_num(char);


#endif // __COMPRESS_H_
