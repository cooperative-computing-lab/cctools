/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#include "allpairs_compare.h"

#include "../../sand/src/matrix.h"
#include "../../sand/src/align.h"

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

/*
If you have a custom comparison function, implement it in allpairs_compare_CUSTOM.
Arguments data1 and data2 point to the objects to be compared.
Arguments size1 and size2 are the length of each object in bytes.
When the comparison is complete, output name1, name2, and the result of the comparison.
The result may take any form that you find useful.
Note that pthread_mutex_lock/unlock is needed around the printf for safety in a multithreaded program.
*/

static void allpairs_compare_CUSTOM( const char *name1, const char *data1, int size1, const char *name2, const char *data2, int size2 )
{
	int result = 5;

	pthread_mutex_lock(&mutex);
	printf("%s\t%s\t%d\n",name1,name2,result);
	pthread_mutex_unlock(&mutex);
}

/*
This is a simple bitwise comparison function that just counts up
the number of bytes in each object that are different.
*/

static void allpairs_compare_BITWISE( const char *name1, const char *data1, int size1, const char *name2, const char *data2, int size2 )
{
	unsigned int i, count = 0;
	unsigned int minsize;

	minsize = size1 - size2 ? size2 : size1;

	count = 0;
	for(i = 0; i < minsize; i++) {
		if(data1[i] != data2[i]) {
			count++;
		}
	}

	pthread_mutex_lock(&mutex);
	printf("%s\t%s\t%d\n",name1,name2,count);
	pthread_mutex_unlock(&mutex);
}

/*
This function aligns two DNA sequences using the Smith-Waterman algorithm,
and if the quality is sufficiently good, displays the alignment.
*/

static void allpairs_compare_SWALIGN( const char *name1, const char *data1, int size1, const char *name2, const char *data2, int size2 )
{
	char *stra = strdup(data1);
	char *strb = strdup(data2);

	stra[size1-1] = 0;
	strb[size2-1] = 0;

	struct matrix *m = matrix_create(size1-1,size2-1);
	struct alignment *aln = align_smith_waterman(m,stra,strb);

	pthread_mutex_lock(&mutex);
	printf("> %s %s\n",name1,name2);
	alignment_print(stdout,stra,strb,aln);
	pthread_mutex_unlock(&mutex);

	free(stra);
	free(strb);
	matrix_delete(m);
	alignment_delete(aln);
}

/*
This function compares two iris templates in the binary format used by the Computer Vision Research Lab at the University of Notre Dame.
*/

static void allpairs_compare_IRIS( const char *name1, const char *data1, int size1, const char *name2, const char *data2, int size2 )
{
	int i, j, count = 0;
	int size, band, inner, outer, quality;
	const int power2[8] = { 1, 2, 4, 8, 16, 32, 64, 128 };

	// Start processing data in mmap1       
	sscanf(data1, "%d %d %d %d %d", &size, &band, &inner, &outer, &quality);
	data1 = strchr(data1, '\n');

	// Let data1 points to the start of code data
	data1++;

	if(size1 - (data1 - (char *) data1) != size / 8 * 2) {
		fprintf(stderr, "allpairs_multicore: Image 1 data size error!\n");
		exit(1);
	}

	int code1[size];
	int mask1[size];

	count = 0;
	for(i = 0; i < size / 8; i++) {
		for(j = 0; j < 8; j++) {
			if(data1[i] & power2[j]) {
				code1[count] = 1;
				count++;
			} else {
				code1[count] = 0;
				count++;
			}
		}
	}

	// Let data1 now points to the start of mask data
	data1 += i;

	count = 0;
	for(i = 0; i < size / 8; i++) {
		for(j = 0; j < 8; j++) {
			if(data1[i] & power2[j]) {
				mask1[count] = 1;
				count++;
			} else {
				mask1[count] = 0;
				count++;
			}
		}
	}

	sscanf(data2, "%d %d %d %d %d", &size, &band, &inner, &outer, &quality);
	data2 = strchr(data2, '\n');

	// Let data2 points to the start of code data
	data2++;

	if(size2 - (data2 - (char *) data2) != size / 8 * 2) {
		fprintf(stderr, "allpairs_multicore: Image 2 data size error!\n");
		exit(1);
	}

	int code2[size];
	int mask2[size];

	count = 0;
	for(i = 0; i < size / 8; i++) {
		for(j = 0; j < 8; j++) {
			if(data2[i] & power2[j]) {
				code2[count] = 1;
				count++;
			} else {
				code2[count] = 0;
				count++;
			}
		}
	}

	// Let data1 now points to the start of mask data
	data2 += i;

	count = 0;
	for(i = 0; i < size / 8; i++) {
		for(j = 0; j < 8; j++) {
			if(data2[i] & power2[j]) {
				mask2[count] = 1;
				count++;
			} else {
				mask2[count] = 0;
				count++;
			}
		}
	}

	int codes[size];
	int masks[size];
	int results[size];
	int distance = 0;
	int total = 0;
	for(i = 0; i < size; i++) {
		codes[i] = code1[i] ^ code2[i];
		masks[i] = mask1[i] & mask2[i];
		results[i] = codes[i] & masks[i];
		distance = distance + results[i];
		total = total + masks[i];
	}

	pthread_mutex_lock(&mutex);
	printf("%s\t%s\t%lf\n",name1,name2,distance/(double)total);
	pthread_mutex_unlock(&mutex);
}

allpairs_compare_t allpairs_compare_function_get( const char *name )
{
	if(!strcmp(name,"CUSTOM")) {
		return allpairs_compare_CUSTOM;
	} else if(!strcmp(name,"BITWISE")) {
		return allpairs_compare_BITWISE;
	} else if(!strcmp(name,"SWALIGN")) {
		return allpairs_compare_SWALIGN;
	} else if(!strcmp(name,"IRIS")) {
		return allpairs_compare_IRIS;
	} else {
		return 0;
	}
}

