/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifdef HAS_HDFS

#include "pfs_table.h"
#include "pfs_service.h"
#include "pfs_service_hdfs.h"

extern "C" {
#include "debug.h"
}

#include "hdfs.h"

#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <utime.h>
#include <dlfcn.h>
#include <sys/statfs.h>

extern uid_t pfs_uid;
extern gid_t pfs_gid;
extern int pfs_master_timeout;
extern int pfs_enable_small_file_optimizations;

/* Hack b/c libhdfs doesn't read hdfs-default.xml, which means these values are not defined */
#define HDFS_BUFSIZE 4096	/* io.file.buffer.size */
#define HDFS_REP     3		/* dfs.replication */
#define HDFS_BLKSIZE 8388608	/* dfs.block.size */
#define HDFS_DEFAULT_PORT 9100

#define HDFS_STAT_MODE (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH)

#define HDFS_CHECK_INIT(e) if (!is_initialized && initialize() < 0) return (e);
#define HDFS_CHECK_FS(e)   if (!fs) return (e);

#define HDFS_END debug(D_HDFS,"= %d %s",(int)result,((result>=0) ? "" : strerror(errno))); return result;

#define HDFS_LOAD_FUNC(func, func_name, func_sig) \
	do { \
		char *errmsg = dlerror(); \
		debug(D_HDFS, "loading function %s", func_name); \
		hdfs->func = (func_sig)dlsym(handle, func_name); \
		if ((errmsg = dlerror()) != NULL) { \
			debug(D_HDFS, "= %s", errmsg); \
			return -1; \
		} \
		debug(D_HDFS, "= %lld", hdfs->func); \
	} while (0);

struct hdfs_services {
	hdfsFS          (*connect)      (const char *, tPort);
	int             (*disconnect)   (hdfsFS);
	hdfsFileInfo*   (*listdir)      (hdfsFS, const char *, int *);
	hdfsFile	(*open)		(hdfsFS, const char *, int, int, short, tSize);
	int		(*close)	(hdfsFS, hdfsFile);
	int		(*flush)	(hdfsFS, hdfsFile);
	tSize		(*read)		(hdfsFS, hdfsFile, tOffset, void *, tSize);
	tSize		(*write)	(hdfsFS, hdfsFile, const void *, tSize);
	int		(*exists)	(hdfsFS, const char *);
	int		(*mkdir)	(hdfsFS, const char *);
	int		(*unlink)	(hdfsFS, const char *);
	int		(*rename)	(hdfsFS, const char *, const char *);
	hdfsFileInfo*	(*stat)		(hdfsFS, const char *);
	void		(*free_stat)	(hdfsFileInfo *, int);
};

/* TODO: Clean up handles? */
int load_hdfs_services(struct hdfs_services *hdfs) {
	void *handle;

	handle = dlopen(LIBJVM_PATH, RTLD_LAZY);
	if (!handle) {
		debug(D_HDFS, "can't open %s: %s\n", LIBJVM_PATH, dlerror());
		return -1;
	}

	handle = dlopen(LIBHDFS_PATH, RTLD_LAZY);
	if (!handle) {
		debug(D_HDFS, "can't open %s: %s\n", LIBHDFS_PATH, dlerror());
		return -1;
	}

	HDFS_LOAD_FUNC(connect,    "hdfsConnect",       void*(*)(const char*, tPort))
	HDFS_LOAD_FUNC(disconnect, "hdfsDisconnect",    int(*)(void*)) 
	HDFS_LOAD_FUNC(listdir,    "hdfsListDirectory", hdfsFileInfo *(*)(void*, const char*, int*))
	HDFS_LOAD_FUNC(open,       "hdfsOpenFile",	hdfsFile(*)(hdfsFS, const char *, int, int, short, tSize))
	HDFS_LOAD_FUNC(close,      "hdfsCloseFile",	int(*)(hdfsFS, hdfsFile))
	HDFS_LOAD_FUNC(flush,      "hdfsFlush",		int(*)(hdfsFS, hdfsFile))
	HDFS_LOAD_FUNC(read,       "hdfsPread",		tSize(*)(hdfsFS, hdfsFile, tOffset, void *, tSize))
	HDFS_LOAD_FUNC(write,      "hdfsWrite",		tSize(*)(hdfsFS, hdfsFile, const void *, tSize))
	HDFS_LOAD_FUNC(exists,     "hdfsExists",	int(*)(hdfsFS, const char *))
	HDFS_LOAD_FUNC(mkdir,      "hdfsCreateDirectory", int(*)(hdfsFS, const char *))
	HDFS_LOAD_FUNC(unlink,     "hdfsDelete",	int(*)(hdfsFS, const char *))
	HDFS_LOAD_FUNC(rename,     "hdfsRename",	int(*)(hdfsFS, const char *, const char *))
	HDFS_LOAD_FUNC(stat,	   "hdfsGetPathInfo",	hdfsFileInfo *(*)(hdfsFS, const char *))
	HDFS_LOAD_FUNC(free_stat,  "hdfsFreeFileInfo",	void(*)(hdfsFileInfo *, int))

	return 0;
}

class pfs_file_hdfs : public pfs_file
{
private:
	struct hdfs_services *hdfs;
	hdfsFS fs;
	hdfsFile handle;
public:
	pfs_file_hdfs( pfs_name *n, struct hdfs_services *hs, hdfsFS f, hdfsFile h ) : pfs_file(n) {
		hdfs = hs;
		fs = f;
		handle = h;
	}

	virtual int close() {
		int result;

		debug(D_HDFS, "closing file %s", name.rest);
		result = hdfs->close(fs, handle);
		HDFS_END
	}

	virtual int fsync() {
		int result; 

		debug(D_HDFS, "flushing file %s ", name.rest);
		result = hdfs->flush(fs, handle);
		HDFS_END
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t result;

		debug(D_HDFS, "reading from file %s", name.rest);
		result = hdfs->read(fs, handle, offset, data, length);
		HDFS_END
	}
	
	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t result;

		debug(D_HDFS, "writing to file %s ", name.rest);
		result = hdfs->write(fs, handle, data, length);
		HDFS_END
	}
};

class pfs_service_hdfs : public pfs_service {
private:
	struct hdfs_services hdfs;
	bool is_initialized;
public:
	pfs_service_hdfs() {
		is_initialized = false;
	}

	int initialize() {
		int result;

		debug(D_HDFS, "loading dynamically shared libraries");
		if (load_hdfs_services(&hdfs) < 0) {
			is_initialized = false;
			result = -1;
		} else {
			is_initialized = true;
			result = 0;
		}

		HDFS_END
	}

	virtual void * connect( pfs_name *name ) {
		hdfsFS fs;
		
		HDFS_CHECK_INIT(0)

		debug(D_HDFS, "connecting to %s:%d", name->host, name->port);
		fs = hdfs.connect(name->host, name->port);
		if (errno == EINTERNAL) {
			errno = ECONNRESET;
		}

		debug(D_HDFS, "= %ld", fs);
		return fs;
	}

	virtual void disconnect( pfs_name *name, void *fs) {
		debug(D_HDFS, "disconnecting from %s:%d", name->host, name->port);
		hdfs.disconnect((hdfsFS)fs);
	}

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		pfs_file *file = 0;
		hdfsFile handle;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(0)
		HDFS_CHECK_FS(0)

		switch (flags&O_ACCMODE) {
			case O_RDONLY:
				debug(D_HDFS, "opening file %s for reading", name->rest);
				flags = O_RDONLY;
				if (hdfs.exists(fs, name->rest) < 0) {
					debug(D_HDFS, "file %s does not exist", name->rest);
					errno = ENOENT;
					return 0;
				}
				break;
			case O_WRONLY:
				debug(D_HDFS, "opening file %s for writing", name->rest);
				flags = O_WRONLY;
				break;
			default:
				debug(D_HDFS, "invalid file open flag %d", flags&O_ACCMODE);
				errno = ENOTSUP;
				return 0;
		}
			
		struct pfs_stat buf;
		if (!this->stat(name, &buf) && S_ISDIR(buf.st_mode)) {
			errno = EISDIR;
			return 0;
		}

		handle = hdfs.open(fs, name->rest, flags, HDFS_BUFSIZE, HDFS_REP, HDFS_BLKSIZE);
		if (handle != NULL) {
			file = new pfs_file_hdfs(name, &hdfs, fs, handle);
			if (!file) {
				errno = ENOENT;
			}
		} else {
			errno = EINVAL;
			file = 0;
		}

		if (!file) {
			pfs_service_disconnect_cache(name, fs, (errno == EINTERNAL));
		}

		debug(D_HDFS, "= %ld", file);
		return file;
	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		pfs_dir *dir = new pfs_dir(name);

		hdfsFileInfo *file_list = 0;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		int num_entries = 0;

		HDFS_CHECK_INIT(0)
		HDFS_CHECK_FS(0)

		debug(D_HDFS, "checking if directory %s exists", name->rest);
		if (hdfs.exists(fs, name->rest) < 0) {
			errno = EINVAL;
			delete dir;
			return 0;
		}

		debug(D_HDFS, "getting directory of %s", name->rest);
		file_list = hdfs.listdir(fs, name->rest, &num_entries);

		if (file_list != NULL) {
			for (int i = 0; i < num_entries; i++) 
				dir->append(file_list[i].mName);
			
			hdfs.free_stat(file_list, num_entries);
		}
		
		return dir;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		hdfsFileInfo *file_info = 0;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		
		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		debug(D_HDFS, "getting stat of %s", name->rest);
		file_info = hdfs.stat(fs, name->rest);

		if (file_info != NULL) {
			pfs_service_emulate_stat(name, buf);

			if (file_info->mKind == kObjectKindDirectory) {
				buf->st_mode = S_IFDIR;
			} else {
				buf->st_mode = S_IFREG;
			}
				
			buf->st_mode |= HDFS_STAT_MODE;
			buf->st_size  = file_info->mSize;
			buf->st_atime = file_info->mLastMod;
			buf->st_mtime = file_info->mLastMod;
		
			hdfs.free_stat(file_info, 1);
			result = 0;
		} else {
			errno = ENOENT;
			result = -1;
		}
		
		pfs_service_disconnect_cache(name, (void*)fs, (errno == EINTERNAL));
		HDFS_END
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		
		HDFS_CHECK_INIT(-1)
		
		debug(D_HDFS, "getting lstat of %s", name->rest);
		result = this->stat(name, buf);
		
		HDFS_END
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		int result = -1;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		
		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		debug(D_HDFS, "getting access of %s", name->rest);
		result = hdfs.exists(fs, name->rest);
		
		pfs_service_disconnect_cache(name, (void*)fs, (errno == EINTERNAL));
		HDFS_END
	}

	virtual int chdir( pfs_name *name, char *newname ) {
		int result = -1;
		struct pfs_stat buf;
		
		HDFS_CHECK_INIT(-1)

		debug(D_HDFS, "change directory to %s", name->rest);
		if (this->stat(name, &buf) >= 0) {
			if (S_ISDIR(buf.st_mode)) {
				sprintf(newname, "/%s/%s:%d%s", name->service_name, name->host, name->port, name->rest);
				result = 0;
			} else {
				errno = ENOTDIR;
				result = -1;
			}
		}

		HDFS_END
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		
		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)
		
		debug(D_HDFS, "making directory %s", name->rest);
		result = hdfs.mkdir(fs, name->rest);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == EINTERNAL));
		HDFS_END
	}
	
	virtual int rmdir( pfs_name *name ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		
		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)
		
		debug(D_HDFS, "removing directory %s", name->rest);
		result = hdfs.unlink(fs, name->rest);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == EINTERNAL));
		HDFS_END
	}
	
	virtual int unlink( pfs_name *name ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		
		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)
		
		debug(D_HDFS, "unlinking %s", name->rest);
		result = hdfs.unlink(fs, name->rest);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == EINTERNAL));
		HDFS_END
	}

	virtual int rename( pfs_name *name, pfs_name *newname ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		
		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)
		
		debug(D_HDFS, "renaming %s to %s", name->rest, newname->rest);
		result = hdfs.rename(fs, name->rest, newname->rest);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == EINTERNAL));
		HDFS_END
	}

	virtual int get_default_port() {
		return HDFS_DEFAULT_PORT;
	}

	virtual int is_seekable() {
		return 1;
	}
	
	virtual int is_readwritable() {
		return 0;
	}
};

static pfs_service_hdfs pfs_service_hdfs_instance;
pfs_service *pfs_service_hdfs = &pfs_service_hdfs_instance;

#endif

// vim: sw=8 sts=8 ts=8 ft=cpp 
