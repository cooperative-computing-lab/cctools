/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_dircache.h"
#include "pfs_table.h"
#include "pfs_service.h"

extern "C" {
#include "debug.h"
#include "hash_table.h"
#include "hdfs_library.h"
}

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

extern int pfs_enable_small_file_optimizations;
extern const char *pfs_username;

#define HDFS_DEFAULT_PORT 9100

#define HDFS_STAT_MODE (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH)

#define HDFS_CHECK_INIT(e) if (!is_initialized && initialize() < 0) return (e);
#define HDFS_CHECK_FS(e)   if (!fs) return (e);

#define HDFS_END debug(D_HDFS,"= %d %s",(int)result,((result>=0) ? "" : strerror(errno))); return result;

static pfs_dircache hdfs_dircache;

class pfs_file_hdfs : public pfs_file
{
private:
	struct hdfs_library *hdfs;
	hdfsFS fs;
	hdfsFile handle;
public:
	pfs_file_hdfs( pfs_name *n, struct hdfs_library *hs, hdfsFS f, hdfsFile h ) : pfs_file(n) {
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

		hdfs_dircache.invalidate();

		debug(D_HDFS, "flushing file %s ", name.rest);
		result = hdfs->flush(fs, handle);
		HDFS_END
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t result;

		debug(D_HDFS, "reading from file %s", name.rest);
		result = hdfs->pread(fs, handle, offset, data, length);
		HDFS_END
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		pfs_ssize_t result;

		hdfs_dircache.invalidate();

		/* Ignore offset since HDFS does not support seekable writes. */
		debug(D_HDFS, "writing to file %s ", name.rest);
		result = hdfs->write(fs, handle, data, length);
		HDFS_END
	}
};

class pfs_service_hdfs : public pfs_service {
private:
	struct hdfs_library *hdfs;
	struct hash_table *uid_table;
	struct hash_table *gid_table;

	bool is_initialized;
public:
	pfs_service_hdfs() {
		uid_table = hash_table_create(0, 0);
		gid_table = hash_table_create(0, 0);
		is_initialized = false;
	}

	~pfs_service_hdfs() {
		hash_table_delete(uid_table);
		hash_table_delete(gid_table);
	}

	int initialize() {
		int result;

		debug(D_HDFS, "loading dynamically shared libraries");
		result = hdfs_library_envinit();
		if (result == 0) {
			hdfs = hdfs_library_open();
			if(!hdfs){
				is_initialized = false;
				result = -1;
			} else {
				is_initialized = true;
				result = 0;
			}
				}

		HDFS_END
	}

	int get_uid_from_name( const char *name ) {
		int key;

		key = (PTRINT_T)hash_table_lookup(uid_table, name);
		if (key) {
			return key;
		} else {
			struct passwd *owner;

			owner = getpwnam(name);
			if (owner) {
				hash_table_insert(uid_table, name, (void*)(PTRINT_T)owner->pw_uid);
				return owner->pw_uid;
			} else {
				return -1;
			}
		}
	}

	int get_gid_from_name( const char *name ) {
		int key;

		key = (PTRINT_T)hash_table_lookup(gid_table, name);
		if (key) {
			return key;
		} else {
			struct group *group;

			group = getgrnam(name);
			if (group) {
				hash_table_insert(gid_table, name, (void*)(PTRINT_T)group->gr_gid);
				return group->gr_gid;
			} else {
				return -1;
			}
		}
	}

	void hdfs_copy_fileinfo(pfs_name *name, hdfsFileInfo *file_info, struct pfs_stat *buf) {
		int file_uid;
		int file_gid;

		pfs_service_emulate_stat(name, buf);

		if (file_info->mKind == kObjectKindDirectory) {
			buf->st_mode  = S_IFDIR;
		} else {
			buf->st_mode = S_IFREG;
		}

		buf->st_mode   |= file_info->mPermissions;
		buf->st_size    = file_info->mSize;
		buf->st_mtime   = file_info->mLastMod;
		buf->st_atime   = file_info->mLastAccess;
		buf->st_blksize = file_info->mBlockSize;

		file_uid = get_uid_from_name(file_info->mOwner);
		if (file_uid >= 0) {
			buf->st_uid = file_uid;
		}

		file_gid = get_gid_from_name(file_info->mGroup);
		if (file_gid >= 0) {
			buf->st_gid = file_gid;
		}
	}

	virtual void * connect( pfs_name *name ) {
		static const char *groups[] = { "supergroup" };
		hdfsFS fs;
		HDFS_CHECK_INIT(0)

		debug(D_HDFS, "connecting to %s:%d as %s", name->host, name->port, pfs_username);
		fs = hdfs->connect_as_user(name->host, name->port, pfs_username, groups, 1);
		if (errno == HDFS_EINTERNAL) {
			errno = ECONNRESET;
		}

		debug(D_HDFS, "= %p", fs);
		return fs;
	}

	virtual void disconnect( pfs_name *name, void *fs) {
		debug(D_HDFS, "disconnecting from %s:%d", name->host, name->port);
		hdfs->disconnect((hdfsFS)fs);
	}

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		pfs_file *file = 0;
		hdfsFile handle;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(0)
		HDFS_CHECK_FS(0)

		hdfs_dircache.invalidate();

		switch (flags&O_ACCMODE) {
			case O_RDONLY:
				debug(D_HDFS, "opening file %s for reading", name->rest);
				flags = O_RDONLY;
				if (hdfs->exists(fs, name->rest) < 0) {
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
		if (!this->_stat(fs, name, &buf) && S_ISDIR(buf.st_mode)) {
			errno = EISDIR;
			return 0;
		}

		handle = hdfs->open(fs, name->rest, flags, 0, 0, 0);
		if (handle != NULL) {
			file = new pfs_file_hdfs(name, hdfs, fs, handle);
			if (!file) {
				errno = ENOENT;
			}
		} else {
			errno = EINVAL;
			file = 0;
		}

		pfs_service_disconnect_cache(name, fs, (errno == HDFS_EINTERNAL));

		debug(D_HDFS, "= %p", file);
		return file;
	}

	virtual pfs_dir * getdir( pfs_name *name ) {
		pfs_dir *dir = new pfs_dir(name);

		hdfsFileInfo *file_list = 0;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		int num_entries = 0;

		HDFS_CHECK_INIT(0)
		HDFS_CHECK_FS(0)

		if (pfs_enable_small_file_optimizations) {
			hdfs_dircache.begin(name->path);
		}

		debug(D_HDFS, "checking if directory %s exists", name->rest);
		if (hdfs->exists(fs, name->rest) < 0) {
			errno = EINVAL;
			delete dir;
			return 0;
		}

		debug(D_HDFS, "getting directory of %s", name->rest);
		file_list = hdfs->listdir(fs, name->rest, &num_entries);
		struct pfs_stat buf;
		if (file_list != NULL) {
			for (int i = 0; i < num_entries; i++) {
				if (pfs_enable_small_file_optimizations) {
					hdfs_copy_fileinfo(name, &file_list[i], &buf);
					hdfs_dircache.insert(file_list[i].mName, &buf, dir);
				} else {
					dir->append(file_list[i].mName);
				}
			}

			hdfs->free_stat(file_list, num_entries);
		}

		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));
		return dir;
	}

	virtual int stat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		debug(D_HDFS, "stat %s", name->rest);
		result = this->_stat(fs, name, buf);
		buf->st_mode |= (S_IXUSR | S_IXGRP);
		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));

		HDFS_END
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		debug(D_HDFS, "lstat %s", name->rest);
		result = this->_stat(fs, name, buf);
		buf->st_mode |= (S_IXUSR | S_IXGRP);
		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));

		HDFS_END
	}

	virtual int _stat( hdfsFS fs, pfs_name *name, struct pfs_stat *buf ) {
		int result;
		hdfsFileInfo *file_info = 0;

		if (hdfs_dircache.lookup(name->rest, buf)) {
			result = 0;
		} else {
			file_info = hdfs->stat(fs, name->rest);

			if (file_info != NULL) {
				hdfs_copy_fileinfo(name, file_info, buf);
				hdfs->free_stat(file_info, 1);
				result = 0;
			} else {
				errno = ENOENT;
				result = -1;
			}
		}

		HDFS_END
	}

	virtual int access( pfs_name *name, mode_t mode ) {
		int result = -1;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		debug(D_HDFS, "access %s", name->rest);
		result = hdfs->exists(fs, name->rest);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));
		HDFS_END
	}

	virtual int chdir( pfs_name *name, char *newname ) {
		int result = -1;
		struct pfs_stat buf;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		debug(D_HDFS, "chdir %s", name->rest);
		if (this->_stat(fs, name, &buf) >= 0) {
			if (S_ISDIR(buf.st_mode)) {
				sprintf(newname, "/%s/%s:%d%s", name->service_name, name->host, name->port, name->rest);
				result = 0;
			} else {
				errno = ENOTDIR;
				result = -1;
			}
		}

		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));
		HDFS_END
	}

	virtual int mkdir( pfs_name *name, mode_t mode ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		hdfs_dircache.invalidate();

		debug(D_HDFS, "mkdir %s", name->rest);
		result = hdfs->mkdir(fs, name->rest);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));
		HDFS_END
	}

	virtual int rmdir( pfs_name *name ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		hdfs_dircache.invalidate();

		debug(D_HDFS, "rmdir %s", name->rest);
		result = hdfs->unlink(fs, name->rest,1);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));
		HDFS_END
	}

	virtual int unlink( pfs_name *name ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		hdfs_dircache.invalidate();

		debug(D_HDFS, "unlink %s", name->rest);
		result = hdfs->unlink(fs, name->rest,0);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));
		HDFS_END
	}

	virtual int rename( pfs_name *name, pfs_name *newname ) {
		int result;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);

		HDFS_CHECK_INIT(-1)
		HDFS_CHECK_FS(-1)

		hdfs_dircache.invalidate();

		debug(D_HDFS, "rename %s to %s", name->rest, newname->rest);
		result = hdfs->rename(fs, name->rest, newname->rest);

		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));
		HDFS_END
	}

	/**
	 * locate file
	 *
	 * Returns locations of first block of file. */
	virtual pfs_location *locate( pfs_name *name ) {
		struct pfs_stat buf;
		hdfsFS fs = (hdfsFS)pfs_service_connect_cache(name);
		pfs_location *loc = NULL;
		char ***hosts;

		HDFS_CHECK_FS(NULL)

		debug(D_HDFS, "locate %s", name->rest);
		if (this->_stat(fs, name, &buf) >= 0) {
			if (S_ISDIR(buf.st_mode)) {
				errno = ENOTSUP;
			} else {
				hosts = hdfs->get_hosts(fs, name->rest, 0, buf.st_blksize);
				if (hosts) {
					loc = new pfs_location();
					for (int i = 0; hosts[i]; i++)
						for (int j = 0; hosts[i][j]; j++)
							loc->append(hosts[i][j]);
					hdfs->free_hosts(hosts);
				}
			}

		}

		pfs_service_disconnect_cache(name, (void*)fs, (errno == HDFS_EINTERNAL));
		return loc;
	}

	virtual int get_default_port() {
		return HDFS_DEFAULT_PORT;
	}

	virtual int is_seekable() {
		return 1;
	}
};

static pfs_service_hdfs pfs_service_hdfs_instance;
pfs_service *pfs_service_hdfs = &pfs_service_hdfs_instance;

/* vim: set noexpandtab tabstop=8: */
