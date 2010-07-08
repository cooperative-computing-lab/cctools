/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_local.h"
#include "chirp_protocol.h"

#include "create_dir.h"
#include "hash_table.h"
#include "xmalloc.h"
#include "int_sizes.h"
#include "stringtools.h"
#include "full_io.h"
#include "delete_dir.h"
#include "md5.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>
#include <utime.h>
#include <sys/file.h>
#include <sys/stat.h>
#ifdef HAS_SYS_STATFS_H
#include <sys/statfs.h>
#endif
#ifdef HAS_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif
#include <sys/param.h>
#include <sys/mount.h>

/* Cygwin does not have 64-bit I/O, while Darwin has it by default. */

#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#define ftruncate64 ftruncate
#define truncate64 truncate
#define statfs64 statfs
#define fstatfs64 fstatfs
#endif

#if CCTOOLS_OPSYS_DARWIN
#define lchown chown
#endif

/* Solaris has statfs, but it doesn't work! Use statvfs instead. */

#if CCTOOLS_OPSYS_SUNOS
#define statfs statvfs
#define fstatfs fstatvfs
#define statfs64 statvfs64
#define fstatfs64 fstatvfs64
#endif

#ifndef COPY_CSTAT
#define COPY_CSTAT( a, b )\
	memset(&(b),0,sizeof(b));\
	(b).cst_dev = (a).st_dev;\
	(b).cst_ino = (a).st_ino;\
	(b).cst_mode = (a).st_mode;\
	(b).cst_nlink = (a).st_nlink;\
	(b).cst_uid = (a).st_uid;\
	(b).cst_gid = (a).st_gid;\
	(b).cst_rdev = (a).st_rdev;\
	(b).cst_size = (a).st_size;\
	(b).cst_blksize = (a).st_blksize;\
	(b).cst_blocks = (a).st_blocks;\
	(b).cst_atime = (a).st_atime;\
	(b).cst_mtime = (a).st_mtime;\
	(b).cst_ctime = (a).st_ctime;
#endif

INT64_T chirp_local_file_size( const char *path )
{
	struct chirp_stat info;
	if(chirp_local_stat(path,&info)==0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

INT64_T chirp_local_fd_size( int fd )
{
	struct chirp_stat info;
	if(chirp_local_fstat(fd,&info)==0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

INT64_T chirp_local_open( const char *path, INT64_T flags, INT64_T mode )
{
	mode = 0600 | (mode&0100);
	return open64(path,flags,(int)mode);
}

INT64_T chirp_local_close( int fd )
{
	return close(fd);
}

INT64_T chirp_local_pread( int fd, void *buffer, INT64_T length, INT64_T offset )
{
	INT64_T result;
	result = full_pread64(fd,buffer,length,offset);
	if(result<0 && errno==ESPIPE) {
		/* if this is a pipe, return whatever amount is available */
		result = read(fd,buffer,length);
	}
	return result;
}

INT64_T chirp_local_sread( int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset )
{
	INT64_T total = 0;
	INT64_T actual = 0;
	char *buffer = vbuffer;

	if(stride_length<0 || stride_skip<0 || offset<0) {
		errno = EINVAL;
		return -1;
	}

	while(length>=stride_length) {
		actual = chirp_local_pread(fd,&buffer[total],stride_length,offset);
		if(actual>0) {
			length -= actual;
			total += actual;
			offset += stride_skip;
			if(actual==stride_length) {
				continue;
			} else {
				break;
			}
		} else {
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

INT64_T chirp_local_pwrite( int fd, const void *buffer, INT64_T length, INT64_T offset )
{
	INT64_T result;
	result = full_pwrite64(fd,buffer,length,offset);
	if(result<0 && errno==ESPIPE) {
		/* if this is a pipe, then just write without the offset. */
		result = full_write(fd,buffer,length);
	}
	return result;
}

INT64_T chirp_local_swrite( int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset )
{
	INT64_T total = 0;
	INT64_T actual = 0;
	const char *buffer = vbuffer;

	if(stride_length<0 || stride_skip<0 || offset<0) {
		errno = EINVAL;
		return -1;
	}

	while(length>=stride_length) {
		actual = chirp_local_pwrite(fd,&buffer[total],stride_length,offset);
		if(actual>0) {
			length -= actual;
			total += actual;
			offset += stride_skip;
			if(actual==stride_length) {
				continue;
			} else {
				break;
			}
		} else {
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

INT64_T chirp_local_fstat( int fd, struct chirp_stat *buf )
{
	struct stat64 info;
	int result;
	result = fstat64(fd,&info);
	if(result==0) COPY_CSTAT(info,*buf);
	buf->cst_mode = buf->cst_mode & (~0077);
	return result;	
}

INT64_T chirp_local_fstatfs( int fd, struct chirp_statfs *buf )
{
	struct statfs64 info;
	int result;
	result = fstatfs64(fd,&info);
	if(result==0) {
		memset(buf,0,sizeof(*buf));
#ifdef CCTOOLS_OPSYS_SUNOS
		buf->f_type = info.f_fsid;
		buf->f_bsize = info.f_frsize;
#else
		buf->f_type = info.f_type;
		buf->f_bsize = info.f_bsize;
#endif
		buf->f_blocks = info.f_blocks;
		buf->f_bavail = info.f_bavail;
		buf->f_bfree = info.f_bfree;
		buf->f_files = info.f_files;
		buf->f_ffree = info.f_ffree;
	}
	return result;
}

INT64_T chirp_local_fchown( int fd, INT64_T uid, INT64_T gid )
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

INT64_T chirp_local_fchmod( int fd, INT64_T mode )
{
	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.
	mode = 0600 | (mode&0177);
	return fchmod(fd,mode);
}

INT64_T chirp_local_ftruncate( int fd, INT64_T length )
{
	return ftruncate64(fd,length);
}

INT64_T chirp_local_fsync( int fd )
{
	return fsync(fd);
}

void *  chirp_local_opendir( const char *path )
{
	return opendir(path);
}

char *  chirp_local_readdir( void *dir )
{
	struct dirent *d;
	d = readdir(dir);
	if(d) {
		return d->d_name;
	} else {
		return 0;
	}
}

void    chirp_local_closedir( void *dir )
{
	closedir(dir);
}

INT64_T chirp_local_getfile( const char *path, struct link *link, time_t stoptime )
{
	int fd;
	INT64_T result;
	char line[CHIRP_LINE_MAX];
	struct chirp_stat info;
	
	result = chirp_local_stat(path,&info);
	if(result<0) return result;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	if(S_ISFIFO(info.cst_mode)) {
		errno = ESPIPE;
		return -1;
	}

	fd = chirp_local_open(path,O_RDONLY,0);
	if(fd>=0) {
		INT64_T length = info.cst_size;
		sprintf(line,"%lld\n",length);
		link_write(link,line,strlen(line),stoptime);
		result = link_stream_from_fd(link,fd,length,stoptime);
		chirp_local_close(fd);
	} else {
		result = -1;
	}

	return result;
}

INT64_T chirp_local_putfile( const char *path, struct link *link, INT64_T length, INT64_T mode, time_t stoptime )
{
	int fd;
	INT64_T result;
	char line[CHIRP_LINE_MAX];

	mode = 0600 | (mode&0100);

	fd = chirp_local_open(path,O_WRONLY|O_CREAT|O_TRUNC,(int)mode);
	if(fd>=0) {
		sprintf(line,"0\n");
		link_write(link,line,strlen(line),stoptime);
		result = link_stream_to_fd(link,fd,length,stoptime);
		if(result!=length) {
			if(result>=0) link_soak(link,length-result,stoptime);
			result = -1;
		}
		chirp_local_close(fd);
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_local_mkfifo( const char *path )
{
	return mknod(path,0700|S_IFIFO,0);
}

INT64_T chirp_local_unlink( const char *path )
{
	int result = unlink(path);

	/*
	On Solaris, an unlink on a directory
	returns EPERM when it should return EISDIR.
	Check for this cast, and then fix it.
	*/

	if(result<0 && errno==EPERM) {
		struct stat64 info;
		result = stat64(path,&info);
		if(result==0 && S_ISDIR(info.st_mode)) {
			result = -1;
			errno = EISDIR;
		} else {
			result = -1;
			errno = EPERM;
		}
	}

	return result;
}

INT64_T chirp_local_rename( const char *path, const char *newpath )
{
	return rename(path,newpath);
}

INT64_T chirp_local_link( const char *path, const char *newpath )
{
	return link(path,newpath);
}

INT64_T chirp_local_symlink( const char *path, const char *newpath )
{
	return symlink(path,newpath);
}

INT64_T chirp_local_readlink( const char *path, char *buf, INT64_T length )
{
	return readlink(path,buf,length);
}

INT64_T chirp_local_chdir( const char *path)
{
  return chdir(path);
}

INT64_T chirp_local_mkdir( const char *path, INT64_T mode )
{
	return mkdir(path,0700);
}

/*
rmdir is a little unusual.
An 'empty' directory may contain some administrative
files such as an ACL and an allocation state.
Only delete the directory if it contains only those files.
*/

INT64_T chirp_local_rmdir( const char *path )
{
	void *dir;
	char *d;
	int empty = 1;

	dir = chirp_local_opendir(path);
	if(dir) {
		while((d=chirp_local_readdir(dir))) {
			if(!strcmp(d,".")) continue;
			if(!strcmp(d,"..")) continue;
			if(!strncmp(d,".__",3)) continue;
			empty = 0;
			break;
		}
		chirp_local_closedir(dir);

		if(empty) {
			if(delete_dir(path)) {
				return 0;
			} else {
				return -1;
			}
		} else {
			errno = ENOTEMPTY;
			return -1;
		}		
	} else {
		return -1;
	}
}

INT64_T chirp_local_stat( const char *path, struct chirp_stat *buf )
{
	struct stat64 info;
	int result;
	result = stat64(path,&info);
	if(result==0) COPY_CSTAT(info,*buf);
	return result;
}

INT64_T chirp_local_lstat( const char *path, struct chirp_stat *buf )
{
	struct stat64 info;
	int result;
	result = lstat64(path,&info);
	if(result==0) COPY_CSTAT(info,*buf);
	return result;
}

INT64_T chirp_local_statfs( const char *path, struct chirp_statfs *buf )
{
	struct statfs64 info;
	int result;
	result = statfs64(path,&info);
	if(result==0) {
		memset(buf,0,sizeof(*buf));
#ifdef CCTOOLS_OPSYS_SUNOS
		buf->f_type = info.f_fsid;
		buf->f_bsize = info.f_frsize;
#else
		buf->f_type = info.f_type;
		buf->f_bsize = info.f_bsize;
#endif
		buf->f_blocks = info.f_blocks;
		buf->f_bavail = info.f_bavail;
		buf->f_bfree = info.f_bfree;
		buf->f_files = info.f_files;
		buf->f_ffree = info.f_ffree;
	}
	return result;
}

INT64_T chirp_local_access( const char *path, INT64_T mode )
{
	return access(path,mode);
}

INT64_T chirp_local_chmod( const char *path, INT64_T mode )
{
	struct chirp_stat info;

	int result = chirp_local_stat(path,&info);
	if(result<0) return result;

	// A remote user can change some of the permissions bits,
	// which only affect local users, but we don't let them
	// take away the owner bits, which would affect the Chirp server.

	if(S_ISDIR(info.cst_mode)) {
		// On a directory, the user cannot set the execute bit.
		mode = 0700 | (mode&0077);
	} else {
		// On a file, the user can set the execute bit.
		mode = 0600 | (mode&0177);
	}

	return chmod(path,mode);
}

INT64_T chirp_local_chown( const char *path, INT64_T uid, INT64_T gid )
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

INT64_T chirp_local_lchown( const char *path, INT64_T uid, INT64_T gid )
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	return 0;
}

INT64_T chirp_local_truncate( const char *path, INT64_T length )
{
	return truncate64(path,length);
}

INT64_T chirp_local_utime( const char *path, time_t actime, time_t modtime )
{
	struct utimbuf ut;
	ut.actime = actime;
	ut.modtime = modtime;
	return utime(path,&ut);
}

INT64_T chirp_local_md5( const char *path, unsigned char digest[16] )
{
	return md5_file(path,digest);
}

INT64_T chirp_local_init (const char *path)
{
  return !create_dir(path, 0711);
}

INT64_T chirp_local_destroy (void)
{
  return 0;
}

struct chirp_filesystem chirp_local_fs = {
	chirp_local_init,
    chirp_local_destroy,

	chirp_local_open,
	chirp_local_close,
	chirp_local_pread,
	chirp_local_pwrite,
	chirp_local_sread,
	chirp_local_swrite,
	chirp_local_fstat,
	chirp_local_fstatfs,
	chirp_local_fchown,
	chirp_local_fchmod,
	chirp_local_ftruncate,
	chirp_local_fsync,

	chirp_local_opendir,
	chirp_local_readdir,
	chirp_local_closedir,

	chirp_local_getfile,
	chirp_local_putfile,

	chirp_local_mkfifo,
	chirp_local_unlink,
	chirp_local_rename,
	chirp_local_link,
	chirp_local_symlink,
	chirp_local_readlink,
	chirp_local_chdir,
	chirp_local_mkdir,
	chirp_local_rmdir,
	chirp_local_stat,
	chirp_local_lstat,
	chirp_local_statfs,
	chirp_local_access,
	chirp_local_chmod,
	chirp_local_chown,
	chirp_local_lchown,
	chirp_local_truncate,
	chirp_local_utime,
	chirp_local_md5,

	chirp_local_file_size,
	chirp_local_fd_size,
};
