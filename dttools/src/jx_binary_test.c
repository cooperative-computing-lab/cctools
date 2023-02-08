/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_binary.h"
#include "jx_parse.h"
#include "jx_print.h"

#include "timestamp.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>

#define TIMEIT( name, xxx )\
{ \
timestamp_t start = timestamp_get(); \
xxx \
timestamp_t end = timestamp_get(); \
printf( "%s %lu us\n",name,(unsigned long)(end-start));	\
}


int main( int argc, char *argv[] )
{
	if(argc!=4) {
		fprintf(stderr,"use: %s <source-text> <binary-file> <text-out>\n",argv[0]);
		return 1;
	}

	FILE *textfile = fopen(argv[1],"r");
	if(!textfile) {
		fprintf(stderr,"couldn't open %s: %s\n",argv[1],strerror(errno));
		return 1;
	}

	FILE *binaryfile = fopen(argv[2],"w");
	if(!binaryfile) {
		fprintf(stderr,"couldn't open %s: %s\n",argv[2],strerror(errno));
		return 1;
	}

	FILE *textout = fopen(argv[3],"w");
	if(!textout) {
		fprintf(stderr,"couldn't open %s: %s\n",argv[3],strerror(errno));
		return 1;
	}


	struct jx *j;
	TIMEIT( "text    read", j = jx_parse_stream(textfile); fclose(textfile); )

	TIMEIT( "binary write", jx_binary_write(binaryfile,j); fclose(binaryfile); )

	jx_delete(j);

	binaryfile = fopen(argv[2],"r");
	if(!binaryfile) {
		fprintf(stderr,"couldn't open %s: %s\n",argv[2],strerror(errno));
		return 1;
	}

	TIMEIT( "binary read", j = jx_binary_read(binaryfile); fclose(binaryfile); )

	TIMEIT( "text  write", jx_print_stream(j,textout); fclose(textout); )
	return 0;
}
