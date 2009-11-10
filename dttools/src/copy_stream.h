/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef COPY_STREAM_H
#define COPY_STREAM_H

#include <stdio.h>

int copy_stream_to_stream( FILE *input, FILE *output );
int copy_stream_to_buffer( FILE *input, char **buffer );
int copy_stream_to_fd( FILE *input, int fd );
int copy_fd_to_stream( int fd, FILE *output );
void copy_fd_pair( int leftin, int leftout, int rightin, int rightout );

#endif
