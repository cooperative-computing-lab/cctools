/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_file.h"
#include "pfs_service.h"
#include "pfs_name.h"
#include "pfs_file_gzip.h"

extern "C" {
#include "debug.h"
}

#include <errno.h>
#include <stdio.h>
#include <sys/mman.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <zlib.h>

#define BUFFER_SIZE 65536

#define STATE_RESET 0
#define STATE_READY 1
#define STATE_BROKEN 2

#define GZIP_MAGIC1 0x1f
#define GZIP_MAGIC2 0x8b

#define GZIP_DEFLATE 8

#define GZIP_FLAG_FTEXT   1
#define GZIP_FLAG_FHCRC   2
#define GZIP_FLAG_FNAME   4
#define GZIP_FLAG_COMMENT 8

class pfs_file_gzip : public pfs_file
{
private:
	pfs_file *file;
	z_streamp zstream;
	unsigned char buffer[BUFFER_SIZE];
	pfs_off_t compressed_file_pointer;
	int state;

	unsigned char read_byte() {
		char c=0;
		file->read(&c,1,compressed_file_pointer++);
		return c;
	}

	int read_gzip_header() {
		int i;
		int length;
		unsigned char flags;

		if(read_byte()!=GZIP_MAGIC1) goto broken;
		if(read_byte()!=GZIP_MAGIC2) goto broken;
		if(read_byte()!=GZIP_DEFLATE) goto broken;

		flags = read_byte();

		for(i=0;i<6;i++) read_byte();

		if(flags&GZIP_FLAG_FTEXT) {
			length = (read_byte() | (((unsigned int)read_byte())<8));
			for(i=0;i<length;i++) read_byte();
		}

		if(flags&GZIP_FLAG_FNAME) {
			while(read_byte()!=0) {}
		}

		if(flags&GZIP_FLAG_COMMENT) {
			while(read_byte()!=0) {}
		}
		
		if(flags&GZIP_FLAG_FHCRC) {
			read_byte();
			read_byte();
		}

		inflateInit2(zstream,-15);

		state = STATE_READY;
		return 1;

		broken:
		state = STATE_BROKEN;
		return 0;
	}

public:
	
	pfs_file_gzip( pfs_file *f ) : pfs_file(f->get_name()) {
		file = f;
		zstream = (z_streamp) malloc(sizeof(*zstream));
		memset(zstream,0,sizeof(*zstream));
		state = STATE_RESET;
	}
	
	~pfs_file_gzip() {
		delete file;
		inflateEnd(zstream);
		free(zstream);
	}

	int close() {
		file->close();
		return 0;
	}

	pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset )
	{
		unsigned char *cdata = (unsigned char*)data;
		pfs_ssize_t total = 0;
		pfs_ssize_t actual = 0;
		int zresult;

		if(state==STATE_RESET) {
			read_gzip_header();
		}
		
		if(state==STATE_BROKEN) {
			errno=EIO;
			return -1;
		}

		while(length>0) {
			if(zstream->avail_in==0) {
				actual = file->read(buffer,BUFFER_SIZE,compressed_file_pointer);
				if(actual<=0) break;

				compressed_file_pointer += actual;
				zstream->next_in = buffer;
				zstream->avail_in = actual;
			}

			zstream->next_out = cdata;
			zstream->avail_out = length;

			zresult = inflate(zstream,Z_SYNC_FLUSH);
			if(zresult==Z_OK || zresult==Z_STREAM_END) {
				actual = length-zstream->avail_out;
				if(actual>0) {
					cdata  += actual;
					total  += actual;
					length -= actual;
					continue;
				} else {
					break;
				}
			} else {
				debug(D_NOTICE,"decompression error %d on file %s",zresult,get_name()->logical_name);
				errno = EIO;
				return -1;
				break;
			}
		}

		if(total>0) {
			return total;
		} else {
			if(actual<0) {
				return -1;
			} else {
				return 0;
			}
		}
	}

	pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset )
	{
		errno = EROFS;
		return -1;
	}

	int fstat( struct pfs_stat *buf )
	{
		return file->fstat(buf);
	}

	int fstatfs( struct pfs_statfs *buf )
	{
		return file->fstatfs(buf);
	}

	int ftruncate( pfs_size_t length )
	{
		return file->ftruncate(length);
	}

	int fsync()
	{
		return file->fsync();
	}

	int fcntl( int cmd, void *arg )
	{
		return file->fcntl(cmd,arg);
	}

	int ioctl( int cmd, void *arg )
	{
		return file->ioctl(cmd,arg);
	}

	int fchmod( mode_t mode )
	{
		return file->fchmod(mode);
	}

	int fchown( uid_t uid, gid_t gid )
	{
		return file->fchown(uid,gid);
	}

	int flock( int op )
	{
		return file->flock(op);
	}

	void * mmap( void *start, pfs_size_t length, int prot, int flags, off_t offset )
	{
		return file->mmap(start,length,prot,flags,offset);
	}

	struct dirent * fdreaddir( pfs_off_t offset, pfs_off_t *next_offset )
	{
		return file->fdreaddir(offset,next_offset);
	}

	pfs_name * get_name()
	{
		return file->get_name();
	}

	pfs_ssize_t get_size()
	{
		return file->get_size();
	}

	int get_real_fd()
	{
		return file->get_real_fd();
	}

	int get_local_name( char *n )
	{
		return file->get_local_name(n);
	}

	int is_seekable()
	{
		return 0;
	}
};

pfs_file * pfs_gzip_open( pfs_file *file, int flags, int mode )
{
	return new pfs_file_gzip(file);
}
