/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
The chirp_global module provides a "global" view of the Chirp storage space,
presenting multiple servers as one big file tree.  It uses the catalog_query
module to obtain the global list of servers and then uses chirp_reli to access
the underlying file servers.

So as to avoid many slow queries to the catalog server, queries are cached and
sonsulted repeatedly.  Each query pulls off the details of each server in the
form of jx json expressions that are placed into a hash table according
to the server name and port.  The catalog is not queried above once per minute.
Note that no matter how often the catalog is queried, the data will be stale
due to the propagation delay from servers to the catalog.  If you are using a
catalog other than the default, set the environment variable CATALOG_HOST to
point to it.

Directory lists simply iterate through the hash table to obtain the global
list.  Stat operations on file servers query the hash table in order to
determine a few key stats, such as total storage in use and last time heard
from.  This allows an ls -l through Parrot to show the last message time and
the space used (in MB.)
*/

#include "chirp_global.h"
#include "chirp_reli.h"
#include "chirp_protocol.h"
#include "chirp_multi.h"
#include "chirp_client.h"
#include "chirp_protocol.h"
#include "catalog_query.h"

#include "macros.h"
#include "debug.h"
#include "full_io.h"
#include "sleeptools.h"
#include "hash_table.h"
#include "xxmalloc.h"
#include "jx_parse.h"

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>

static struct hash_table *server_table = 0;
static time_t last_update = 0;
static time_t update_interval = 60;
static int inhibit_catalog_queries = 0;

static int not_empty(const char *str)
{
	if(!str || !str[0] || !strcmp(str, "/")) {
		return 0;
	} else {
		return 1;
	}
}

static int is_multi_path(const char *host)
{
	return !strcmp(host, "multi") || !strcmp(host, "multi:9094");
}

static void chirp_jx_to_stat(struct jx *j, struct chirp_stat *info)
{
	memset(info, 0, sizeof(*info));
	info->cst_atime = info->cst_mtime = info->cst_ctime = jx_lookup_integer(j, "lastheardfrom");
	info->cst_size = jx_lookup_integer(j, "total") - jx_lookup_integer(j, "avail");
	info->cst_size /= 1024 * 1024;
	info->cst_mode = S_IFDIR | 0555;
}

static void chirp_blank_stat(struct chirp_stat *info)
{
	memset(info, 0, sizeof(*info));
	info->cst_mode = S_IFDIR | 0555;
}

static void parse_multi_path(const char *path, char *mhost, char *mpath)
{
	mhost[0] = 0;
	mpath[0] = 0;

	sscanf(path, "/%[^/]%s", mhost, mpath);

	if(mhost[0] && !mpath[0]) {
		strcpy(mpath, "/");
	}
}

static int server_table_load(time_t stoptime)
{
	struct catalog_query *q;
	struct jx *j;
	char *key;
	void *item;

	if((last_update + update_interval) > time(0)) {
		return 1;
	}

	if(!server_table) {
		server_table = hash_table_create(0, 0);
		if(!server_table)
			return 0;
	}

	if(inhibit_catalog_queries) {
		debug(D_CHIRP, "catalog queries disabled\n");
		return 1;
	}

	hash_table_firstkey(server_table);
	while(hash_table_nextkey(server_table, &key, &item)) {
		hash_table_remove(server_table, key);
		jx_delete(item);
	}

	debug(D_CHIRP, "querying catalog at %s", CATALOG_HOST);

	struct jx *jexpr = jx_operator(
		JX_OP_EQ,
		jx_symbol("type"),
		jx_string("chirp")
	);
	q = catalog_query_create(CATALOG_HOST, jexpr, stoptime);

	if(!q) {
		jx_delete(jexpr);
		return 0;
	}

	while((j = catalog_query_read(q, stoptime))) {
		char name[CHIRP_PATH_MAX];
		const char *hname;
		int port;

		hname = jx_lookup_string(j, "name");
		if(hname) {
			port = jx_lookup_integer(j, "port");
			if(!port)
				port = CHIRP_PORT;
			sprintf(name, "%s:%d", hname, port);
			hash_table_insert(server_table, name, j);
		} else {
			jx_delete(j);
		}
	}
	catalog_query_delete(q);
	last_update = time(0);

	return 1;
}

static struct jx *server_lookup(const char *host, time_t stoptime)
{
	if(server_table_load(stoptime)) {
		return hash_table_lookup(server_table, host);
	} else {
		return 0;
	}
}

void chirp_global_inhibit_catalog(int onoff)
{
	inhibit_catalog_queries = onoff;
}

struct chirp_file *chirp_global_open(const char *host, const char *path, INT64_T flags, INT64_T mode, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_open(mhost, mpath, flags, mode, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_open(host, path, flags, mode, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EISDIR;
			return 0;
		} else {
			if(flags & O_CREAT) {
				errno = EACCES;
			} else {
				errno = ENOENT;
			}
			return 0;
		}
	} else {
		errno = EISDIR;
		return 0;
	}
}

INT64_T chirp_global_close(struct chirp_file * file, time_t stoptime)
{
	return chirp_reli_close(file, stoptime);
}

INT64_T chirp_global_pread(struct chirp_file * file, void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	return chirp_reli_pread(file, buffer, length, offset, stoptime);
}

INT64_T chirp_global_pwrite(struct chirp_file * file, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime)
{
	return chirp_reli_pwrite(file, buffer, length, offset, stoptime);
}

INT64_T chirp_global_sread(struct chirp_file * file, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime)
{
	return chirp_reli_sread(file, buffer, length, stride_length, stride_skip, offset, stoptime);
}

INT64_T chirp_global_swrite(struct chirp_file * file, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime)
{
	return chirp_reli_swrite(file, buffer, length, stride_length, stride_skip, offset, stoptime);
}

INT64_T chirp_global_fstat(struct chirp_file * file, struct chirp_stat * buf, time_t stoptime)
{
	return chirp_reli_fstat(file, buf, stoptime);
}

INT64_T chirp_global_fstatfs(struct chirp_file * file, struct chirp_statfs * buf, time_t stoptime)
{
	return chirp_reli_fstatfs(file, buf, stoptime);
}

INT64_T chirp_global_fchown(struct chirp_file * file, INT64_T uid, INT64_T gid, time_t stoptime)
{
	return chirp_reli_fchown(file, uid, gid, stoptime);
}

INT64_T chirp_global_fchmod(struct chirp_file * file, INT64_T mode, time_t stoptime)
{
	return chirp_reli_fchmod(file, mode, stoptime);
}

INT64_T chirp_global_ftruncate(struct chirp_file * file, INT64_T length, time_t stoptime)
{
	return chirp_reli_ftruncate(file, length, stoptime);
}

INT64_T chirp_global_flush(struct chirp_file * file, time_t stoptime)
{
	return chirp_reli_flush(file, stoptime);
}

INT64_T chirp_global_getfile(const char *host, const char *path, FILE * stream, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_getfile(mhost, mpath, stream, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_getfile(host, path, stream, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EISDIR;
			return -1;
		} else {
			errno = EACCES;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_getfile_buffer(const char *host, const char *path, char **buffer, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_getfile_buffer(mhost, mpath, buffer, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_getfile_buffer(host, path, buffer, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EISDIR;
			return -1;
		} else {
			errno = EACCES;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_putfile(const char *host, const char *path, FILE * stream, INT64_T mode, INT64_T length, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_putfile(mhost, mpath, stream, mode, length, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_putfile(host, path, stream, mode, length, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EISDIR;
			return -1;
		} else {
			errno = EACCES;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_putfile_buffer(const char *host, const char *path, const char *buffer, INT64_T mode, INT64_T length, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_putfile_buffer(mhost, mpath, buffer, mode, length, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_putfile_buffer(host, path, buffer, mode, length, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EISDIR;
			return -1;
		} else {
			errno = EACCES;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_getlongdir(const char *host, const char *path, chirp_longdir_t callback, void *arg, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_getlongdir(mhost, mpath, callback, arg, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_getlongdir(host, path, callback, arg, stoptime);
	} else if(not_empty(host)) {
		return chirp_reli_getlongdir(host, "/", callback, arg, stoptime);
	} else {
		if(server_table_load(stoptime)) {
			char *key;
			void *item;
			struct chirp_stat info;

			hash_table_firstkey(server_table);
			while(hash_table_nextkey(server_table, &key, &item)) {
				chirp_jx_to_stat(item, &info);
				callback(key, &info, arg);
			}
			chirp_blank_stat(&info);
			callback("multi", &info, arg);
			return 0;
		} else {
			errno = ENOENT;
			return -1;
		}
	}
}

INT64_T chirp_global_getdir(const char *host, const char *path, chirp_dir_t callback, void *arg, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_getdir(mhost, mpath, callback, arg, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_getdir(host, path, callback, arg, stoptime);
	} else if(not_empty(host)) {
		return chirp_reli_getdir(host, "/", callback, arg, stoptime);
	} else {
		if(server_table_load(stoptime)) {
			char *key;
			void *item;
			hash_table_firstkey(server_table);
			while(hash_table_nextkey(server_table, &key, &item)) {
				callback(key, arg);
			}
			callback("multi", arg);
			return 0;
		} else {
			errno = ENOENT;
			return -1;
		}
	}
}

INT64_T chirp_global_getacl(const char *host, const char *path, chirp_dir_t callback, void *arg, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_getacl(mhost, mpath, callback, arg, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_getacl(host, path, callback, arg, stoptime);
	} else if(not_empty(host)) {
		return chirp_reli_getacl(host, "/", callback, arg, stoptime);
	} else {
		errno = EINVAL;
		return -1;
	}
}

INT64_T chirp_global_setacl(const char *host, const char *path, const char *subject, const char *rights, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_setacl(mhost, mpath, subject, rights, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_setacl(host, path, subject, rights, stoptime);
	} else if(not_empty(host)) {
		return chirp_reli_setacl(host, "/", subject, rights, stoptime);
	} else {
		errno = EINVAL;
		return -1;
	}
}

INT64_T chirp_global_whoami(const char *host, const char *path, char *buf, INT64_T length, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_whoami(mhost, buf, length, stoptime);
	} else if(not_empty(host)) {
		return chirp_reli_whoami(host, buf, length, stoptime);
	} else {
		errno = EINVAL;
		return -1;
	}
}

INT64_T chirp_global_locate(const char *host, const char *path, chirp_loc_t callback, void *arg, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EINVAL;
		return -1;
		/*char mhost[CHIRP_PATH_MAX];
		   char mpath[CHIRP_PATH_MAX];
		   parse_multi_path(path,mhost,mpath);
		   return chirp_multi_locate(mhost,mpath,callback,arg,stoptime);
		 */
	} else if(not_empty(host)) {
		return chirp_reli_locate(host, path, callback, arg, stoptime);
	} else {
		errno = EINVAL;
		return -1;
	}
}

INT64_T chirp_global_unlink(const char *host, const char *path, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_unlink(mhost, mpath, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_unlink(host, path, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_rename(const char *host, const char *path, const char *newpath, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];

		char mnewhost[CHIRP_PATH_MAX];
		char mnewpath[CHIRP_PATH_MAX];

		parse_multi_path(path, mhost, mpath);
		parse_multi_path(path, mnewhost, mnewpath);

		if(!strcmp(mhost, mnewhost)) {
			return chirp_multi_rename(mhost, mpath, mnewpath, stoptime);
		} else {
			errno = EXDEV;
			return -1;
		}
	} else if(not_empty(path)) {
		return chirp_reli_rename(host, path, newpath, stoptime);
	} else {
		errno = EXDEV;
		return -1;
	}
}

INT64_T chirp_global_link(const char *host, const char *path, const char *newpath, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_symlink(mhost, mpath, newpath, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_link(host, path, newpath, stoptime);
	} else {
		errno = EXDEV;
		return -1;
	}
}

INT64_T chirp_global_symlink(const char *host, const char *path, const char *newpath, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_symlink(mhost, mpath, newpath, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_symlink(host, path, newpath, stoptime);
	} else {
		errno = EXDEV;
		return -1;
	}
}

INT64_T chirp_global_readlink(const char *host, const char *path, char *buf, INT64_T length, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_readlink(mhost, mpath, buf, length, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_readlink(host, path, buf, length, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EINVAL;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EINVAL;
		return -1;
	}
}

INT64_T chirp_global_mkdir(const char *host, const char *path, INT64_T mode, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_mkdir(mhost, mpath, mode, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_mkdir(host, path, mode, stoptime);
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_rmdir(const char *host, const char *path, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_rmdir(mhost, mpath, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_rmdir(host, path, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_rmall(const char *host, const char *path, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = ENOSYS;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_rmall(host, path, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_stat(const char *host, const char *path, struct chirp_stat * buf, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_stat(mhost, mpath, buf, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_stat(host, path, buf, stoptime);
	} else if(not_empty(host)) {
		struct jx *j = server_lookup(host, stoptime);
		if(j) {
			chirp_jx_to_stat(j, buf);
			return 0;
		} else {
			return chirp_reli_stat(host, "/", buf, stoptime);
		}
	} else {
		chirp_blank_stat(buf);
		return 0;
	}
}

INT64_T chirp_global_lstat(const char *host, const char *path, struct chirp_stat * buf, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_lstat(mhost, mpath, buf, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_lstat(host, path, buf, stoptime);
	} else if(not_empty(host)) {
		struct jx *j = server_lookup(host, stoptime);
		if(j) {
			chirp_jx_to_stat(j, buf);
			return 0;
		} else {
			return chirp_reli_lstat(host, "/", buf, stoptime);
		}
	} else {
		chirp_blank_stat(buf);
		return 0;
	}
}

INT64_T chirp_global_statfs(const char *host, const char *path, struct chirp_statfs * buf, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_statfs(mhost, mpath, buf, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_statfs(host, path, buf, stoptime);
	} else if(not_empty(host)) {
		return chirp_reli_statfs(host, "/", buf, stoptime);
	} else {
		memset(buf, 0, sizeof(*buf));
		return 0;
	}

}

INT64_T chirp_global_access(const char *host, const char *path, INT64_T mode, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_access(mhost, mpath, mode, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_access(host, path, mode, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			return 0;
		} else {
			return chirp_reli_access(host, path, mode, stoptime);
		}
	} else {
		return 0;
	}
}

INT64_T chirp_global_chmod(const char *host, const char *path, INT64_T mode, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_chmod(mhost, mpath, mode, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_chmod(host, path, mode, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}


INT64_T chirp_global_chown(const char *host, const char *path, INT64_T uid, INT64_T gid, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_chown(mhost, mpath, uid, gid, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_chown(host, path, uid, gid, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_lchown(const char *host, const char *path, INT64_T uid, INT64_T gid, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_lchown(mhost, mpath, uid, gid, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_lchown(host, path, uid, gid, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_truncate(const char *host, const char *path, INT64_T length, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_truncate(mhost, mpath, length, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_truncate(host, path, length, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EISDIR;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EISDIR;
		return -1;
	}
}

INT64_T chirp_global_utime(const char *host, const char *path, time_t actime, time_t modtime, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_utime(mhost, mpath, actime, modtime, stoptime);
	} else if(not_empty(path)) {
		return chirp_reli_utime(host, path, actime, modtime, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EISDIR;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EISDIR;
		return -1;
	}
}

INT64_T chirp_global_thirdput(const char *host, const char *path, const char *thirdhost, const char *thirdpath, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(host)) {
		return chirp_reli_thirdput(host, path, thirdhost, thirdpath, stoptime);
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_md5(const char *host, const char *path, unsigned char *digest, time_t stoptime)
{
	if(is_multi_path(host)) {
		char mhost[CHIRP_PATH_MAX];
		char mpath[CHIRP_PATH_MAX];
		parse_multi_path(path, mhost, mpath);
		return chirp_multi_md5(mhost, mpath, digest, stoptime);
	} else if(not_empty(host)) {
		return chirp_reli_md5(host, path, digest, stoptime);
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_lsalloc(const char *host, const char *path, char *alloc_path, INT64_T * size, INT64_T * inuse, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(host)) {
		return chirp_reli_lsalloc(host, path, alloc_path, size, inuse, stoptime);
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_mkalloc(const char *host, const char *path, INT64_T size, INT64_T mode, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_mkalloc(host, path, size, mode, stoptime);
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_getxattr(const char *host, const char *path, const char *name, void *data, size_t size, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_getxattr(host, path, name, data, size, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_fgetxattr(struct chirp_file * file, const char *name, void *data, size_t size, time_t stoptime)
{
	return chirp_reli_fgetxattr(file, name, data, size, stoptime);
}

INT64_T chirp_global_lgetxattr(const char *host, const char *path, const char *name, void *data, size_t size, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_lgetxattr(host, path, name, data, size, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_listxattr(const char *host, const char *path, char *list, size_t size, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_listxattr(host, path, list, size, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_flistxattr(struct chirp_file * file, char *list, size_t size, time_t stoptime)
{
	return chirp_reli_flistxattr(file, list, size, stoptime);
}

INT64_T chirp_global_llistxattr(const char *host, const char *path, char *list, size_t size, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_llistxattr(host, path, list, size, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_setxattr(const char *host, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_setxattr(host, path, name, data, size, flags, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_fsetxattr(struct chirp_file * file, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	return chirp_reli_fsetxattr(file, name, data, size, flags, stoptime);
}

INT64_T chirp_global_lsetxattr(const char *host, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_lsetxattr(host, path, name, data, size, flags, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_removexattr(const char *host, const char *path, const char *name, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_removexattr(host, path, name, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

INT64_T chirp_global_fremovexattr(struct chirp_file * file, const char *name, time_t stoptime)
{
	return chirp_reli_fremovexattr(file, name, stoptime);
}

INT64_T chirp_global_lremovexattr(const char *host, const char *path, const char *name, time_t stoptime)
{
	if(is_multi_path(host)) {
		errno = EACCES;
		return -1;
	} else if(not_empty(path)) {
		return chirp_reli_lremovexattr(host, path, name, stoptime);
	} else if(not_empty(host)) {
		if(server_lookup(host, stoptime)) {
			errno = EACCES;
			return -1;
		} else {
			errno = ENOENT;
			return -1;
		}
	} else {
		errno = EACCES;
		return -1;
	}
}

/* vim: set noexpandtab tabstop=8: */
