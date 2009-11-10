/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "copy_stream.h"
#include "full_io.h"

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <signal.h>

#define COPY_BUFFER_SIZE 65536

int copy_stream_to_stream( FILE *input, FILE *output )
{
	char buffer[COPY_BUFFER_SIZE];
	int actual_read=0, actual_write=0;
	int total=0;

	while(1) {
		actual_read = full_fread(input,buffer,COPY_BUFFER_SIZE);
		if(actual_read<=0) break;

		actual_write = full_fwrite(output,buffer,actual_read);
		if(actual_write!=actual_read) {
			total = -1;
			break;
		}

		total+=actual_write;
	}

	if( ( (actual_read<0) || (actual_write<0) ) && total==0 ) {
		return -1;
	} else {
		return total;
	}
}

int copy_stream_to_buffer( FILE *input, char **buffer )
{
	int buffer_size = 8192;
	int total=0;
	int actual;
	char *newbuffer;

	*buffer = malloc(buffer_size);
	if(!*buffer) return -1;

	while(1) {
		actual = full_fread(input,&(*buffer)[total],buffer_size-total);
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

	(*buffer)[total] = 0;

	return total;
}

int copy_stream_to_fd( FILE *input, int fd )
{
	char buffer[COPY_BUFFER_SIZE];
	int actual_read=0, actual_write=0;
	int total=0;

	while(1) {
		actual_read = full_fread(input,buffer,COPY_BUFFER_SIZE);
		if(actual_read<=0) break;

		actual_write = full_write(fd,buffer,actual_read);
		if(actual_write!=actual_read) {
			total = -1;
			break;
		}

		total+=actual_write;
	}

	if( ( (actual_read<0) || (actual_write<0) ) && total==0 ) {
		return -1;
	} else {
		return total;
	}
}

int copy_fd_to_stream( int fd, FILE *output )
{
	char buffer[COPY_BUFFER_SIZE];
	int actual_read=0, actual_write=0;
	int total=0;

	while(1) {
		actual_read = full_read(fd,buffer,COPY_BUFFER_SIZE);
		if(actual_read<=0) break;

		actual_write = full_fwrite(output,buffer,actual_read);
		if(actual_write!=actual_read) {
			total = -1;
			break;
		}

		total+=actual_write;
	}

	if( ( (actual_read<0) || (actual_write<0) ) && total==0 ) {
		return -1;
	} else {
		return total;
	}
}

static int keepgoing = 0;

static void stop_working( int sig )
{
	keepgoing = 0;
}

static void * install_handler( int sig, void (*handler)(int sig))
{
	struct sigaction s,olds;
	s.sa_handler = handler;
	sigfillset(&s.sa_mask);
	s.sa_flags = 0; 
	sigaction(sig,&s,&olds);
	return olds.sa_handler;
}

void copy_fd_pair( int leftin, int leftout, int rightin, int rightout )
{
	char buffer[COPY_BUFFER_SIZE];
	int actual, result;
	pid_t pid;
	void (*old_sigchld)(int);
	void (*old_sigterm)(int);

	keepgoing = 1;

	old_sigchld = install_handler(SIGCHLD,stop_working);
	old_sigterm = install_handler(SIGTERM,stop_working);

	pid = fork();
	if(pid==0) {
		while(keepgoing) {
			result = read(leftin,buffer,sizeof(buffer));
			if(result>0) {
				actual = full_write(rightout,buffer,result);
				if(actual!=result) break;
			} else if(result==0) {
				break;				
			}
		}
		kill(getppid(),SIGTERM);
		_exit(0);
	} else {
		while(keepgoing) {
			result = read(rightin,buffer,sizeof(buffer));
			if(result>0) {
				actual = full_write(leftout,buffer,result);
				if(actual!=result) break;
			} else if(result==0) {
				break;				
			}
		}
		kill(pid,SIGTERM);
	}


	install_handler(SIGTERM,old_sigterm);
	install_handler(SIGTERM,old_sigchld);
}

