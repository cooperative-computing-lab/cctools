/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "hash_table.h"
#include "copy_stream.h"
#include "ftsh_error.h"
#include "full_io.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <limits.h>

static struct hash_table *table=0;

static int buffer_init()
{
	if(!table) {
		table = hash_table_create( 127, hash_string );
		if(!table) return 0;
	}
	return 1;
}

int buffer_open_input( const char *tag )
{
	int fd;

	if(!buffer_init()) return 0;

	fd = (PTRINT_T) hash_table_lookup(table,tag);
	if(fd==0) {
		errno = ENOENT;
		return -1;
	}

	lseek(fd,0,SEEK_SET);

	return fd;
}

static int buffer_open( const char *tag, int do_truncate )
{
	char path[PATH_MAX];
	long fd;
	int old=0;

	if(!buffer_init()) return 0;

	if(!do_truncate) {
		fd = (long) hash_table_lookup(table,tag);
		if(fd!=0) {
			lseek(fd,0,SEEK_END);
			return fd;
		}
	}

	strcpy(path,"/tmp/ftsh.XXXXXX");

	fd = mkstemp(path);
	if(fd<0) return -1;

	unlink(path);

	old = (PTRINT_T) hash_table_remove(table,tag);
	if(old) close(old);

	if(!hash_table_insert(table,tag,(void*)fd)) {
		ftsh_fatal(0,"out of memory");
	}


	return fd;
}


int buffer_open_output( const char *tag )
{
	return buffer_open(tag,1);
}

int buffer_open_append( const char *tag )
{
	return buffer_open(tag,0);
}

char * buffer_load( const char *tag )
{
	FILE *stream=0;
	char *buffer;
	int nfd=-1, fd=-1;
	int save_errno;

	fd = buffer_open_input(tag);
	if(fd<0) goto failure;

	nfd = dup(fd);
	if(nfd<0) goto failure;

	stream = fdopen(nfd,"r");
	if(!stream) goto failure;
	if(!copy_stream_to_buffer(stream,&buffer)) goto failure;
	fclose(stream);

	return buffer;

	failure:
	save_errno = errno;
	if(stream) {
		fclose(stream);
	} else {
		if(nfd>=0) close(nfd);
	}
	errno = save_errno;
	return 0;
}

int buffer_save( const char *tag, const char *data )
{
	int fd;

	fd = buffer_open_output(tag);
	if(fd<0) return 0;

	if(full_write(fd,data,strlen(data))==strlen(data)) {
		return 1;
	} else {
		return 0;
	}
}

int buffer_delete( const char *tag )
{
	long fd;

	if(!buffer_init()) return 0;

	fd = (long) hash_table_remove(table,tag);
	if(fd!=0) close(fd);

	return 1;
}



/* vim: set noexpandtab tabstop=4: */
