/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_HDFS

#include "chirp_filesystem.h"
#include "chirp_hdfs.h"
#include "chirp_protocol.h"

#include "macros.h"
#include "xmalloc.h"
#include "hash_table.h"
#include "debug.h"
#include "md5.h"

#include "hdfs.h"

#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <utime.h>
#include <dlfcn.h>
#include <pwd.h>
#include <grp.h>
#include <sys/statfs.h>

char *chirp_hdfs_hostname = NULL;
UINT16_T chirp_hdfs_port = 0;

#define HDFS_LOAD_FUNC(func, func_name, func_sig) \
	do { \
		char *errmsg = dlerror(); \
		debug(D_HDFS, "loading function %s", func_name); \
		hdfs->func = (func_sig)dlsym(LIBHDFS_HANDLE, func_name); \
		if (hdfs->func == NULL && (errmsg = dlerror()) != NULL) { \
			debug(D_HDFS, "= %s", errmsg); \
			return -1; \
		} \
		debug(D_HDFS, "= %p", hdfs->func); \
	} while (0);

static struct hdfs_services_ {
	hdfsFS          (*connect)      (const char *, tPort, const char *, const char *[], int);
	int             (*disconnect)   (hdfsFS);
	hdfsFileInfo*   (*listdir)      (hdfsFS, const char *, int *);
	hdfsFile	(*open)		(hdfsFS, const char *, int, int, short, tSize);
	int		(*close)	(hdfsFS, hdfsFile);
	int		(*flush)	(hdfsFS, hdfsFile);
	tSize		(*read)		(hdfsFS, hdfsFile, void *, tSize);
	tSize		(*pread)		(hdfsFS, hdfsFile, tOffset, void *, tSize);
	tSize		(*write)	(hdfsFS, hdfsFile, const void *, tSize);
	int		(*exists)	(hdfsFS, const char *);
	int		(*mkdir)	(hdfsFS, const char *);
	int		(*unlink)	(hdfsFS, const char *);
	int		(*rename)	(hdfsFS, const char *, const char *);
	hdfsFileInfo*	(*stat)		(hdfsFS, const char *);
	void		(*free_stat)	(hdfsFileInfo *, int);
	char***		(*get_hosts)	(hdfsFS, const char *, tOffset, tOffset);
	void		(*free_hosts)	(char ***);
	tOffset		(*get_default_block_size) (hdfsFS);
	tOffset		(*get_capacity) (hdfsFS);
	tOffset		(*get_used) (hdfsFS);
	int			(*chmod) (hdfsFS, const char *, short);
	int			(*utime) (hdfsFS, const char *, tTime, tTime);
	int			(*chdir) (hdfsFS, const char *);
} hdfs_services;
static int hdfs_services_loaded = 0;
static void *LIBJVM_HANDLE = NULL;
static void *LIBHDFS_HANDLE = NULL;

/* TODO: Clean up handles? */
static int load_hdfs_services (struct hdfs_services_ *hdfs)
{
    assert(LIBJVM_HANDLE == NULL && LIBHDFS_HANDLE == NULL);

	if(!getenv("JAVA_HOME") || !getenv("HADOOP_HOME") || !getenv("CLASSPATH") || !getenv("LIBHDFS_PATH") || !getenv("LIBJVM_PATH")) {
		static int did_hdfs_warning=0;
		if(!did_hdfs_warning) {
			fprintf(stderr,"Sorry, to use Parrot with HDFS, you need to set up Java and Hadoop first.\n");
			fprintf(stderr,"Set JAVA_HOME, HADOOP_HOME, CLASSPATH, LIBHDFS_PATH, and LIBJVM_PATH appropriately.\n");
			fprintf(stderr,"Or just run parrot_hdfs, which sets up everything for you.\n");
			did_hdfs_warning = 1;
		}
		errno = ENOSYS;		
		return -1;
	}
	
	LIBJVM_HANDLE = dlopen(getenv("LIBJVM_PATH"), RTLD_LAZY);
	if (!LIBJVM_HANDLE) {
		debug(D_NOTICE|D_HDFS, "%s", dlerror());
		return -1;
	}

	LIBHDFS_HANDLE = dlopen(getenv("LIBHDFS_PATH"), RTLD_LAZY);
	if (!LIBHDFS_HANDLE) {
		debug(D_NOTICE|D_HDFS, "%s", dlerror());
		return -1;
	}

	HDFS_LOAD_FUNC(connect,    "hdfsConnectAsUser",       void*(*)(const char*, tPort, const char *, const char *[], int))
	HDFS_LOAD_FUNC(disconnect, "hdfsDisconnect",    int(*)(void*)) 
	HDFS_LOAD_FUNC(listdir,    "hdfsListDirectory", hdfsFileInfo *(*)(void*, const char*, int*))
	HDFS_LOAD_FUNC(open,       "hdfsOpenFile",	hdfsFile(*)(hdfsFS, const char *, int, int, short, tSize))
	HDFS_LOAD_FUNC(close,      "hdfsCloseFile",	int(*)(hdfsFS, hdfsFile))
	HDFS_LOAD_FUNC(flush,      "hdfsFlush",		int(*)(hdfsFS, hdfsFile))
	HDFS_LOAD_FUNC(read,       "hdfsRead",		tSize(*)(hdfsFS, hdfsFile, void *, tSize))
	HDFS_LOAD_FUNC(pread,       "hdfsPread",		tSize(*)(hdfsFS, hdfsFile, tOffset, void *, tSize))
	HDFS_LOAD_FUNC(write,      "hdfsWrite",		tSize(*)(hdfsFS, hdfsFile, const void *, tSize))
	HDFS_LOAD_FUNC(exists,     "hdfsExists",	int(*)(hdfsFS, const char *))
	HDFS_LOAD_FUNC(mkdir,      "hdfsCreateDirectory", int(*)(hdfsFS, const char *))
	HDFS_LOAD_FUNC(unlink,     "hdfsDelete",	int(*)(hdfsFS, const char *))
	HDFS_LOAD_FUNC(rename,     "hdfsRename",	int(*)(hdfsFS, const char *, const char *))
	HDFS_LOAD_FUNC(stat,	   "hdfsGetPathInfo",	hdfsFileInfo *(*)(hdfsFS, const char *))
	HDFS_LOAD_FUNC(free_stat,  "hdfsFreeFileInfo",	void(*)(hdfsFileInfo *, int))
	HDFS_LOAD_FUNC(get_hosts,  "hdfsGetHosts",	char ***(*)(hdfsFS, const char *, tOffset, tOffset))
	HDFS_LOAD_FUNC(free_hosts, "hdfsFreeHosts",	void(*)(char ***))
	HDFS_LOAD_FUNC(get_default_block_size, "hdfsGetDefaultBlockSize",	tOffset(*)(hdfsFS))
	HDFS_LOAD_FUNC(get_capacity, "hdfsGetCapacity",	tOffset(*)(hdfsFS))
	HDFS_LOAD_FUNC(get_used, "hdfsGetUsed",	tOffset(*)(hdfsFS))
	HDFS_LOAD_FUNC(chmod, "hdfsChmod",	int(*)(hdfsFS, const char*, short))
	HDFS_LOAD_FUNC(utime, "hdfsUtime",	int(*)(hdfsFS, const char*, tTime, tTime))
	HDFS_LOAD_FUNC(chdir, "hdfsSetWorkingDirectory",	int(*)(hdfsFS, const char*))


    hdfs_services_loaded = 1;
	return 0;
}

/* FIXME temporary static variables? */
/* static struct hdfs_services *hdfs;*/
static hdfsFS fs = NULL;

/* Array of open HDFS Files */
#define BASE_SIZE 1024
static struct chirp_hdfs_file {
	char *name;
	hdfsFile file;
} open_files[BASE_SIZE]; // = NULL;

INT64_T chirp_hdfs_init (const char *path)
{
  static const char *groups[] = {"supergroup"};

  int i;
  char name[LOGIN_NAME_MAX+2];

  i = getlogin_r(name, sizeof(name)); 
  assert(i == 0);

  if (chirp_hdfs_hostname == NULL)
    fatal("hostname and port must be specified, use -x option");

  debug(D_HDFS, "initializing", chirp_hdfs_hostname, chirp_hdfs_port);

  assert(fs == NULL);

  for (i = 0; i < BASE_SIZE; i++)
    open_files[i].name = NULL;

  if (!hdfs_services_loaded && load_hdfs_services(&hdfs_services) == -1)
    return -1; /* errno is set */
  debug(D_HDFS, "connecting to %s:%u as '%s'\n", chirp_hdfs_hostname, chirp_hdfs_port, name);
  fs = hdfs_services.connect(chirp_hdfs_hostname, chirp_hdfs_port, name, groups, 1);

  if (fs == NULL)
	return (errno = ENOSYS, -1);
  else
	return chirp_hdfs_chdir(path);
}

INT64_T chirp_hdfs_destroy (void)
{
  int ret;
  if (fs == NULL)
    return 0;
  debug(D_HDFS, "destroying hdfs connection", chirp_hdfs_hostname, chirp_hdfs_port);
  ret = hdfs_services.disconnect(fs);
  if (ret == -1) return ret;
  fs = NULL;
  if (dlclose(LIBHDFS_HANDLE) != 0)
    fatal("could not close LIBHDFS handle: %s", dlerror());
  if (dlclose(LIBJVM_HANDLE) != 0)
    fatal("could not close LIBJVM handle: %s", dlerror());
  LIBHDFS_HANDLE = LIBJVM_HANDLE = NULL;
  hdfs_services_loaded = 0;
  return 0;
}

/* modified version from pfs_service.cc */
void service_emulate_stat (const char *name, struct chirp_stat *buf)
{
  static time_t start_time = 0;
  memset(buf,0,sizeof(*buf));
  buf->cst_dev = (dev_t) -1; /* HDFS has no concept of a device number */
  if (name) {
    buf->cst_ino = hash_string(name); /* HDFS has no concept of an inode number */
  } else {
    buf->cst_ino = 0;
  }
  buf->cst_mode = S_IFREG | S_IRWXU | S_IRWXG | S_IRWXO ;
  buf->cst_uid = getuid();
  buf->cst_gid = getgid();
  buf->cst_nlink = 1;
  buf->cst_size = 0;
  if(start_time==0) start_time = time(0);
  buf->cst_ctime = buf->cst_atime = buf->cst_mtime = start_time;
}

static void copystat (struct chirp_stat *cs, hdfsFileInfo *hs)
{
  service_emulate_stat(hs->mName, cs);
  cs->cst_mode = hs->mKind == kObjectKindDirectory ? S_IFDIR : S_IFREG;
  /* HDFS does not have execute bit, lie and set it for all files */
  cs->cst_mode |= hs->mPermissions | S_IXUSR|S_IXGRP;
  /* What do we do with char * mOwner and char * mGroup?? */
  cs->cst_uid = 0; /* i guess it belongs to root */
  cs->cst_gid = 0;
  cs->cst_rdev = -2;
  cs->cst_size = hs->mSize;
  cs->cst_blksize = hs->mBlockSize;
  cs->cst_blocks = 1; /* FIXME */
  cs->cst_atime = (INT64_T) hs->mLastAccess;
  cs->cst_mtime = (INT64_T) hs->mLastMod;
  cs->cst_ctime = -1; /* FIXME */
}

INT64_T chirp_hdfs_fstat (int fd, struct chirp_stat *buf)
{
  return chirp_hdfs_stat(open_files[fd].name, buf);
}

INT64_T chirp_hdfs_stat (const char *path, struct chirp_stat *buf)
{
  hdfsFileInfo *file_info;

  debug(D_HDFS, "stat %s", path);

  file_info = hdfs_services.stat(fs, path);
  if (file_info == NULL) return (errno = ENOENT, -1);
  copystat(buf, file_info);
  hdfs_services.free_stat(file_info, 1);

  return 0;
}

struct chirp_hdfs_dir {
  int i;
  int n;
  hdfsFileInfo *info;
  char *path;
};

void *chirp_hdfs_opendir (const char *path)
{
  struct chirp_hdfs_dir *d;

  debug(D_HDFS, "opendir %s", path);

  d = xxmalloc(sizeof(struct chirp_hdfs_dir));
  d->info = hdfs_services.listdir(fs, path, &d->n);
  d->i = 0;
  d->path = xstrdup(path);

  if (d->info == NULL) return (free(d), errno = ENOENT, NULL);

  return d;
}

char *chirp_hdfs_readdir (void *dir)
{
  struct chirp_hdfs_dir *d = (struct chirp_hdfs_dir *) dir;
  debug(D_HDFS, "readdir %s", d->path);
  if (d->i < d->n)
  {
	/* mName is of the form hdfs:/hostname:port/path/to/file */
	char *entry = d->info[d->i++].mName;
	entry += strlen(entry); /* now points to nul byte */
	while (entry[-1] != '/') entry--;
	return entry;
  }
  else
	return NULL;
}

void chirp_hdfs_closedir (void *dir)
{
  struct chirp_hdfs_dir *d = (struct chirp_hdfs_dir *) dir;
  debug(D_HDFS, "closedir", d->path);
  hdfs_services.free_stat(d->info, d->n);
  free(d->path);
  free(d);
}

INT64_T chirp_hdfs_file_size( const char *path )
{
	struct chirp_stat info;
	if(chirp_hdfs_stat(path,&info)==0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

INT64_T chirp_hdfs_fd_size( int fd )
{
	struct chirp_stat info;
	debug(D_HDFS, "fstat on file descriptor %d, path = %s", fd, open_files[fd].name);
	if(chirp_hdfs_fstat(fd,&info)==0) {
		return info.cst_size;
	} else {
		return -1;
	}
}

static INT64_T get_fd (void)
{
	INT64_T fd;
	/* find an unused file descriptor */
	for (fd = 0; fd < BASE_SIZE; fd++)
		if (open_files[fd].name == NULL)
			return fd;
	debug(D_HDFS, "too many files open");
	errno = EMFILE;
	return -1;
}

static char *read_buffer (const char *path, int entire_file, INT64_T *size)
{
	hdfsFile file;
	char *buffer;
	INT64_T current = 0;

	if (entire_file) /* read entire file? */
	{
		struct chirp_stat info;
		if (chirp_hdfs_stat(path, &info) == -1)
			return NULL;
		*size = info.cst_size;
	}

	file = hdfs_services.open(fs, path, O_RDONLY, 0, 0, 0);
	if (file == NULL) return NULL;

	buffer = xxmalloc(sizeof(char)*(*size));
	memset(buffer, 0, sizeof(char)*(*size));

	while (current < *size) {
		INT64_T ractual = hdfs_services.read(fs, file, buffer+current, *size-current);
  		if (ractual <= 0) break;
  		current += ractual;
	}
	hdfs_services.close(fs, file);
	return buffer;
}

static INT64_T write_buffer (const char *path, const char *buffer, size_t size)
{
	hdfsFile file;
	INT64_T current = 0;
	INT64_T fd;

	fd = get_fd();
	if (fd == -1) return -1;

	file = hdfs_services.open(fs, path, O_WRONLY, 0, 0, 0);
	if (file == NULL) return -1; /* errno is set */

	while (current < size) {
		INT64_T wactual = hdfs_services.write(fs, file, buffer, size-current);
		if (wactual == -1) return -1;
		current += wactual;
	}
	open_files[fd].file = file;
	open_files[fd].name = xstrdup(path);
	return fd;

}

INT64_T chirp_hdfs_open( const char *path, INT64_T flags, INT64_T mode )
{
	INT64_T fd, stat_result;
	struct chirp_stat info;
	stat_result = chirp_hdfs_stat(path,&info);

	fd = get_fd();
	if (fd == -1) return -1;

	mode = 0600 | (mode&0100);
	switch (flags & O_ACCMODE)
	{
		case O_RDONLY:
			debug(D_HDFS, "opening file %s (flags: %o) for reading; mode: %o", path, flags, mode);
			if (stat_result == -1)
			  return (errno = ENOENT, -1); /* HDFS screws this up */
            break;
		case O_WRONLY:
			debug(D_HDFS, "opening file %s (flags: %o) for writing; mode: %o", path, flags, mode);
            /* Check if file exists already */
			if (stat_result<0) {
				flags = O_WRONLY;
				break; /* probably doesn't exist, continue.... */
			}
			else if (S_ISDIR(info.cst_mode))
				return (errno = EISDIR, -1);
			else if (O_TRUNC & flags) {
				/* delete file, then open again */
				INT64_T result = hdfs_services.unlink(fs, path);
				if (result == -1) return (errno = EIO, -1);
				flags ^= O_TRUNC;
				break;
			}
			else if (!(O_APPEND & flags)) {
				debug(D_HDFS, "file does not have append flag set, setting it anyway");
				/* return (errno = ENOTSUP, -1); */
                flags |= O_APPEND;
			}
            INT64_T size;
			char *buffer = read_buffer(path, 1, &size);
			if (buffer == NULL) return -1;
			INT64_T fd = write_buffer(path, buffer, size);
			free(buffer);
			return fd;
		default:
			debug(D_HDFS, "invalid file open flag %o", flags&O_ACCMODE);
			return (errno = EINVAL, -1);
	}

	open_files[fd].file = hdfs_services.open(fs, path, flags, 0, 0, 0);
	if (open_files[fd].file == NULL)
	{
		debug(D_HDFS, "could not open file %s", path);
		return -1;
	}
	else
	{
		open_files[fd].name = xstrdup(path);
		return fd;
	}
}

INT64_T chirp_hdfs_close( int fd )
{
	debug(D_HDFS, "closing file %s", open_files[fd].name);
	free(open_files[fd].name);
	open_files[fd].name = NULL;
	return hdfs_services.close(fs, open_files[fd].file);
}

INT64_T chirp_hdfs_pread( int fd, void *buffer, INT64_T length, INT64_T offset )
{
	debug(D_HDFS, "pread %s", open_files[fd].name);
	return hdfs_services.pread(fs, open_files[fd].file, offset, buffer, length);
}

INT64_T chirp_hdfs_sread( int fd, void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset )
{
	INT64_T total = 0;
	INT64_T actual = 0;
	char *buffer = vbuffer;

	if(stride_length<0 || stride_skip<0 || offset<0) {
		errno = EINVAL;
		return -1;
	}

	while(length>=stride_length) {
		actual = chirp_hdfs_pread(fd,&buffer[total],stride_length,offset);
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

INT64_T chirp_hdfs_pwrite( int fd, const void *buffer, INT64_T length, INT64_T offset )
{
	/* FIXME deal with non-appends gracefully using an error if not costly */
	debug(D_HDFS, "pwrite %s", open_files[fd].name);
	return hdfs_services.write(fs, open_files[fd].file, buffer, length);
}

INT64_T chirp_hdfs_swrite( int fd, const void *vbuffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset )
{
	INT64_T total = 0;
	INT64_T actual = 0;
	const char *buffer = vbuffer;

	if(stride_length<0 || stride_skip<0 || offset<0) {
		errno = EINVAL;
		return -1;
	}

	while(length>=stride_length) {
		actual = chirp_hdfs_pwrite(fd,&buffer[total],stride_length,offset);
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

INT64_T chirp_hdfs_fchown( int fd, INT64_T uid, INT64_T gid )
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "fchown %s %ld %ld", open_files[fd].name, (long) uid, (long) gid);
	return 0;
}

INT64_T chirp_hdfs_fchmod( int fd, INT64_T mode )
{
	// The owner may only add or remove the execute bit,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "fchmod %s %lo", open_files[fd].name, (long) mode);
	mode = 0600 | (mode&0100);
	return hdfs_services.chmod(fs, open_files[fd].name, mode);
}

INT64_T chirp_hdfs_ftruncate( int fd, INT64_T length )
{
	debug(D_HDFS, "ftruncate %s %ld", open_files[fd].name, (long) length);
    INT64_T size = length;
	char *buffer = read_buffer(open_files[fd].name, 0, &size);
	if (buffer == NULL) return -1;
	/* simulate truncate */
	if (hdfs_services.close(fs, open_files[fd].file) == -1)
		return (free(buffer), -1);
	INT64_T fd2 = write_buffer(open_files[fd].name, buffer, size);
	open_files[fd].file = open_files[fd2].file; /* copy over new file */
	free(open_files[fd2].name); /* close new fd */
	open_files[fd2].name = NULL;
	return 0;
}

INT64_T chirp_hdfs_fsync( int fd )
{
	debug(D_HDFS, "fsync %s", open_files[fd].name);
	return hdfs_services.flush(fs, open_files[fd].file);
}

INT64_T chirp_hdfs_getfile( const char *path, struct link *link, time_t stoptime )
{
	int fd;
	INT64_T result;
	char line[CHIRP_LINE_MAX];
	struct chirp_stat info;

	debug(D_HDFS, "getfile %s", path);
	
	result = chirp_hdfs_stat(path,&info);
	if(result<0) return result;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	fd = chirp_hdfs_open(path,O_RDONLY,0);
	if(fd>=0) {
		char buffer[65536];
		INT64_T total=0;
		INT64_T ractual, wactual;
		INT64_T length = info.cst_size;

		sprintf(line,"%lld\n",length);
		link_write(link,line,strlen(line),stoptime);

		// Copy Pasta from link.c
		
		while(length>0) {
		  INT64_T chunk = MIN(sizeof(buffer),length);
		  
		  ractual = hdfs_services.read(fs, open_files[fd].file, buffer, chunk);
		  if(ractual<=0) break;
		  
		  wactual = link_write(link,buffer,ractual,stoptime);
		  if(wactual!=ractual) {
			total = -1;
			break;
		  }
		  
		  total += ractual;
		  length -= ractual;
		}
		result = total;
		chirp_hdfs_close(fd);
	} else {
		result = -1;
	}

	return result;
}

INT64_T chirp_hdfs_putfile( const char *path, struct link *link, INT64_T length, INT64_T mode, time_t stoptime )
{
	int fd;
	INT64_T result;
	char line[CHIRP_LINE_MAX];

	debug(D_HDFS, "putfile %s", path);

	mode = 0600 | (mode&0100);

	fd = chirp_hdfs_open(path,O_WRONLY|O_CREAT|O_TRUNC,(int)mode);
	if(fd>=0) {
		char buffer[65536];
		INT64_T total=0;

		sprintf(line,"0\n");
		link_write(link,line,strlen(line),stoptime);

		// Copy Pasta from link.c
		
		while(length>0) {
		  INT64_T ractual, wactual;
		  INT64_T chunk = MIN(sizeof(buffer),length);
		  
		  ractual = link_read(link,buffer,chunk,stoptime);
		  if(ractual<=0) break;
		  
		  wactual = hdfs_services.write(fs, open_files[fd].file, buffer, ractual);
		  if(wactual!=ractual) {
			total = -1;
			break;
		  }
		  
		  total += ractual;
		  length -= ractual;
		}

		result = total;

		if(length!=0) {
			if(result>=0) link_soak(link,length-result,stoptime);
			result = -1;
		}
		chirp_hdfs_close(fd);
	} else {
		result = -1;
	}
	return result;
}

INT64_T chirp_hdfs_mkfifo( const char *path )
{
	debug(D_HDFS, "mkfifo %s", path);
	return (errno = ENOTSUP, -1);
}

INT64_T chirp_hdfs_unlink( const char *path )
{
	debug(D_HDFS, "unlink %s", path);
    /* FIXME unlink does not set errno properly on failure! */
	int ret = hdfs_services.unlink(fs, path);
    if (ret == -1) errno = EEXIST; /* FIXME bad fix to above problem */
    return 0;
}

INT64_T chirp_hdfs_rename( const char *path, const char *newpath )
{
	debug(D_HDFS, "rename %s -> %s", path, newpath);
	hdfs_services.unlink(fs, newpath);
	return hdfs_services.rename(fs, path, newpath);
}

INT64_T chirp_hdfs_link( const char *path, const char *newpath )
{
	debug(D_HDFS, "link %s -> %s", path, newpath);
	return (errno = ENOTSUP, -1);
}

INT64_T chirp_hdfs_symlink( const char *path, const char *newpath )
{
	debug(D_HDFS, "symlink %s -> %s", path, newpath);
	return (errno = ENOTSUP, -1);
}

INT64_T chirp_hdfs_readlink( const char *path, char *buf, INT64_T length )
{
	debug(D_HDFS, "readlink %s", path);
	return (errno = EINVAL, -1);
}

INT64_T chirp_hdfs_mkdir( const char *path, INT64_T mode )
{
	debug(D_HDFS, "mkdir %s", path);
	return hdfs_services.mkdir(fs, path);
}

/*
rmdir is a little unusual.
An 'empty' directory may contain some administrative
files such as an ACL and an allocation state.
Only delete the directory if it contains only those files.
*/

INT64_T chirp_hdfs_rmdir( const char *path )
{
	void *dir;
	char *d;
	int empty = 1;

	debug(D_HDFS, "rmdir %s", path);

	dir = chirp_hdfs_opendir(path);
	if(dir) {
		while((d=chirp_hdfs_readdir(dir))) {
			if(!strcmp(d,".")) continue;
			if(!strcmp(d,"..")) continue;
			if(!strncmp(d,".__",3)) continue;
			empty = 0;
			break;
		}
		chirp_hdfs_closedir(dir);

		if(empty) {
			return hdfs_services.unlink(fs, path);
		} else {
			errno = ENOTEMPTY;
			return -1;
		}		
	} else {
		return -1;
	}
}

INT64_T chirp_hdfs_lstat( const char *path, struct chirp_stat *buf )
{
	debug(D_HDFS, "lstat %s", path);
	return chirp_hdfs_stat(path, buf);
}

INT64_T chirp_hdfs_statfs( const char *path, struct chirp_statfs *buf )
{
	debug(D_HDFS, "statfs %s", path);

	INT64_T capacity = hdfs_services.get_capacity(fs);
	INT64_T used = hdfs_services.get_used(fs);
	INT64_T blocksize = hdfs_services.get_default_block_size(fs);

	if (capacity == -1 || used == -1 || blocksize == -1)
		return (errno = EIO, -1);

	buf->f_type = 0; /* FIXME */
	buf->f_bsize = blocksize;
	buf->f_blocks = capacity/blocksize;
	buf->f_bavail = buf->f_bfree = used/blocksize;
	buf->f_files = buf->f_ffree = 0;

	return 0;
}

INT64_T chirp_hdfs_fstatfs( int fd, struct chirp_statfs *buf )
{
	debug(D_HDFS, "fstatfs %d", fd);

	return chirp_hdfs_statfs("/", buf);
}

INT64_T chirp_hdfs_access( const char *path, INT64_T mode )
{
	/* W_OK is ok to delete, not to write, but we can't distinguish intent */
	/* Chirp ACL will check that we can access the file the way we want, so
	   we just do a redundant "exists" check */
	debug(D_HDFS, "access %s %ld", path, (long) mode);
	return hdfs_services.exists(fs, path);
}

INT64_T chirp_hdfs_chmod( const char *path, INT64_T mode )
{
	// The owner may only add or remove the execute bit,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "chmod %s %ld", path, (long) mode);
	mode = 0600 | (mode&0100);
	return hdfs_services.chmod(fs, path, mode);
}

INT64_T chirp_hdfs_chown( const char *path, INT64_T uid, INT64_T gid )
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "chown (ignored) %s %ld %ld", path, (long) uid, (long) gid);
	return 0;
}

INT64_T chirp_hdfs_lchown( const char *path, INT64_T uid, INT64_T gid )
{
	// Changing file ownership is silently ignored,
	// because permissions are handled through the ACL model.
	debug(D_HDFS, "lchown (ignored) %s %ld %ld", path, (long) uid, (long) gid);
	return 0;
}

INT64_T chirp_hdfs_truncate( const char *path, INT64_T length )
{
	debug(D_HDFS, "truncate %s %ld", path, (long) length);
	/* simulate truncate */
	INT64_T size = length;
	char *buffer = read_buffer(path, 0, &size);
	if (buffer == NULL) return -1;
	INT64_T fd = write_buffer(path, buffer, size);
	free(open_files[fd].name);
	free(buffer);
	open_files[fd].name = NULL;
	return 0;
}

INT64_T chirp_hdfs_utime( const char *path, time_t actime, time_t modtime )
{
	debug(D_HDFS, "utime %s %ld %ld", path, (long) actime, (long) modtime);
	return hdfs_services.utime(fs, path, modtime, actime);
}

INT64_T chirp_hdfs_md5( const char *path, unsigned char digest[16] )
{
	int fd;
	INT64_T result;
	struct chirp_stat info;

	debug(D_HDFS, "md5sum %s", path);
	
	result = chirp_hdfs_stat(path,&info);
	if(result<0) return result;

	if(S_ISDIR(info.cst_mode)) {
		errno = EISDIR;
		return -1;
	}

	fd = chirp_hdfs_open(path,O_RDONLY,0);
	if(fd>=0) {
		char buffer[65536];
		//INT64_T total=0;
		INT64_T ractual;
		INT64_T length = info.cst_size;
		md5_context_t ctx;

		md5_init(&ctx);

		while(length>0) {
		  INT64_T chunk = MIN(sizeof(buffer),length);
		  
		  ractual = hdfs_services.read(fs, open_files[fd].file, buffer, chunk);
		  if(ractual<=0) break;
		  
		  md5_update(&ctx, (unsigned char *) buffer, ractual);
		  
		  //total += ractual;
		  length -= ractual;
		}
		result = 0;
		chirp_hdfs_close(fd);
		md5_final(digest, &ctx);
	} else {
		result = -1;
	}

	return result;
}

INT64_T chirp_hdfs_chdir (const char *path)
{
  debug(D_HDFS, "chdir %s", path);
  return hdfs_services.chdir(fs, path);
}

struct chirp_filesystem chirp_hdfs_fs = {
	chirp_hdfs_init,
	chirp_hdfs_destroy,

	chirp_hdfs_open,
	chirp_hdfs_close,
	chirp_hdfs_pread,
	chirp_hdfs_pwrite,
	chirp_hdfs_sread,
	chirp_hdfs_swrite,
	chirp_hdfs_fstat,
	chirp_hdfs_fstatfs,
	chirp_hdfs_fchown,
	chirp_hdfs_fchmod,
	chirp_hdfs_ftruncate,
	chirp_hdfs_fsync,

	chirp_hdfs_opendir,
	chirp_hdfs_readdir,
	chirp_hdfs_closedir,

	chirp_hdfs_getfile,
	chirp_hdfs_putfile,

	chirp_hdfs_mkfifo,
	chirp_hdfs_unlink,
	chirp_hdfs_rename,
	chirp_hdfs_link,
	chirp_hdfs_symlink,
	chirp_hdfs_readlink,
	chirp_hdfs_chdir,
	chirp_hdfs_mkdir,
	chirp_hdfs_rmdir,
	chirp_hdfs_stat,
	chirp_hdfs_lstat,
	chirp_hdfs_statfs,
	chirp_hdfs_access,
	chirp_hdfs_chmod,
	chirp_hdfs_chown,
	chirp_hdfs_lchown,
	chirp_hdfs_truncate,
	chirp_hdfs_utime,
	chirp_hdfs_md5,

	chirp_hdfs_file_size,
	chirp_hdfs_fd_size,
};

#endif /* HAS_HDFS */
