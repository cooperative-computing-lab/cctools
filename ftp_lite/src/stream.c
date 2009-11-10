/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ftp_lite.h"

#include <stdio.h>
#include <stdlib.h>

#define BUFFER_SIZE 32768

ftp_lite_size_t ftp_lite_stream_to_stream( FILE *input, FILE *output )
{
	char buffer[BUFFER_SIZE];
	int actual_read=0, actual_write=0;
	ftp_lite_size_t total=0;

	while(1) {
		actual_read = fread(buffer,1,BUFFER_SIZE,input);
		if(actual_read<=0) break;

		actual_write = fwrite(buffer,1,actual_read,output);
		if(actual_write!=actual_read) break;

		total+=actual_write;
	}

	if( ( (actual_read<0) || (actual_write<0) ) && total==0 ) {
		return -1;
	} else {
		return total;
	}
}

ftp_lite_size_t ftp_lite_stream_to_buffer( FILE *input, char **buffer )
{
	int buffer_size = 8192;
        int actual;
	ftp_lite_size_t total=0;
	char *newbuffer;

	*buffer = malloc(buffer_size);
	if(!*buffer) return -1;

	while(1) {
		actual = fread(&(*buffer)[total],1,buffer_size-total,input);
		if(actual<=0) break;

		total += actual;

		if( (buffer_size-total)<1 ) {
			buffer_size *= 2;
			newbuffer = realloc(*buffer,buffer_size);
			if(!newbuffer) {
				free(*buffer);
				return -1;
			}
			*buffer = newbuffer;
		}
	}

	return total;
}
