/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.

This module written by James Fitzgerald, B.S. 2006.
*/

#ifdef HAS_FUSE

#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 27

#include <fuse.h>
#include <pthread.h>

#include "chirp_global.h"
#include "chirp_protocol.h"
#include "chirp_reli.h"

#include "auth_all.h"
#include "debug.h"
#include "stringtools.h"
#include "itable.h"
#include "xmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <signal.h>

static int chirp_fuse_timeout = 60;
static int run_in_foreground = 0;
static struct itable *file_table = 0;
static int enable_small_file_optimizations = 1;

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void parsepath(const char *path, char *newpath, char *host)
{
	memset(newpath, 0, CHIRP_PATH_MAX);
	memset(host, 0, CHIRP_PATH_MAX);

	if(!strcmp(path, "/")) {
		// path is the root directory
		strcpy(host, "/");
		strcpy(newpath, "/");
	} else if(*path == '/') {
		// path is absolute
		sscanf(path, "/%[^/]%s", host, newpath);
		if(strcmp(newpath, "") == 0) {
			strcpy(newpath, "/"); /* path = "/host[:port]" ; no final slash */
		}
	} else {
		// path is relative
		strcpy(host, "/");
		strcpy(newpath, path);
	}

}

static void chirp_stat_to_fuse_stat(struct chirp_stat *c, struct stat *f)
{
	memset(f, 0, sizeof(*f));
	f->st_dev = c->cst_dev;
	f->st_ino = c->cst_ino;
	f->st_mode = c->cst_mode;
	f->st_nlink = c->cst_nlink;
	f->st_uid = getuid();
	f->st_gid = getgid();
	f->st_rdev = c->cst_rdev;
	f->st_size = c->cst_size;
	f->st_blksize = chirp_reli_blocksize_get();
	f->st_blocks = c->cst_blocks;
	f->st_atime = c->cst_atime;
	f->st_mtime = c->cst_mtime;
	f->st_ctime = c->cst_ctime;
}

static int chirp_fuse_getattr(const char *path, struct stat *info)
{
	INT64_T result;
	struct chirp_stat cinfo;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];

	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_lstat(host, newpath, &cinfo, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	chirp_stat_to_fuse_stat(&cinfo, info);
	return 0;
}

static int chirp_fuse_readlink(const char *path, char *buf, size_t size)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_readlink(host, newpath, buf, size, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	buf[result] = 0;

	return 0;
}

static fuse_fill_dir_t longdir_filler;
static void *longdir_buf;

static void longdir_callback(const char *name, struct chirp_stat *cinfo, void *arg)
{
	struct stat info;
	chirp_stat_to_fuse_stat(cinfo, &info);
	longdir_filler(longdir_buf, name, &info, 0);
}

static int chirp_fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	int result;

	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);

	longdir_buf = buf;
	longdir_filler = filler;

	result = chirp_global_getlongdir(host, newpath, longdir_callback, 0, time(0) + chirp_fuse_timeout);

	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;

	return 0;
}

static int chirp_fuse_mkdir(const char *path, mode_t mode)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_mkdir(host, newpath, mode, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;
}

static int chirp_fuse_unlink(const char *path)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	if(enable_small_file_optimizations) {
		result = chirp_global_rmall(host, newpath, time(0) + chirp_fuse_timeout);
	} else {
		result = chirp_global_unlink(host, newpath, time(0) + chirp_fuse_timeout);
	}
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;
}

static int chirp_fuse_rmdir(const char *path)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	if(enable_small_file_optimizations) {
		result = chirp_global_rmall(host, newpath, time(0) + chirp_fuse_timeout);
	} else {
		result = chirp_global_rmdir(host, newpath, time(0) + chirp_fuse_timeout);
	}
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;
}

static int chirp_fuse_symlink(const char *source, const char *target)
/*		source: relative filename
		target: full pathname
*/
{
	INT64_T result;
	char dest_path[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];

	parsepath(target, dest_path, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_symlink(host, source, dest_path, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;
}

static int chirp_fuse_rename(const char *from, const char *to)
{
	INT64_T result;
	char frompath[CHIRP_PATH_MAX], topath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(from, frompath, host);
	parsepath(to, topath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_rename(host, frompath, topath, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;
}

static int chirp_fuse_link(const char *from, const char *to)
{
	INT64_T result;
	char frompath[CHIRP_PATH_MAX], topath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(from, frompath, host);
	parsepath(to, topath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_link(host, frompath, topath, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;
}

static int chirp_fuse_chmod(const char *path, mode_t mode)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_chmod(host, newpath, mode, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;

	return 0;
}

static int chirp_fuse_chown(const char *path, uid_t uid, gid_t gid)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_chown(host, newpath, uid, gid, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;

	return 0;
}

static int chirp_fuse_truncate(const char *path, off_t size)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_truncate(host, newpath, size, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;
}

static int chirp_fuse_access(const char *path, int flags)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	if (flags & X_OK) {
		struct chirp_stat buf;
		/* FUSE calls access(dir, X_OK) for chdir calls. For compatibility with older chirp servers, we
		 * check for list access rights on a directory by calling stat.
		 */
		if ((chirp_global_stat(host, newpath, &buf, time(0)+chirp_fuse_timeout) == 0) && S_ISDIR(buf.cst_mode)) {
			/* we've confirmed X_OK rights, now check others if they exist... */
			flags ^= X_OK;
			flags |= F_OK; /* make sure we have *some* flags; on GNU/Linux 0 is a valid value for flags (it is F_OK actually), others it may not be */
			result = chirp_global_access(host, newpath, flags, time(0) + chirp_fuse_timeout);
		} else {
			result = chirp_global_access(host, newpath, flags, time(0) + chirp_fuse_timeout);
		}
	} else {
		result = chirp_global_access(host, newpath, flags, time(0) + chirp_fuse_timeout);
	}
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;
	return 0;
}


static int chirp_fuse_utime(const char *path, struct utimbuf *buf)
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	result = chirp_global_utime(host, newpath, buf->actime, buf->modtime, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;

	return 0;
}


static int chirp_fuse_open(const char *path, struct fuse_file_info *fi)
{
	static int file_number_counter = 1;
	struct chirp_file *file;
	int mode = 0;

	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];

	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	file = chirp_global_open(host, newpath, fi->flags, mode, time(0) + chirp_fuse_timeout);
	if(file) {
		int file_number = file_number_counter++;
		itable_insert(file_table, file_number, file);
		fi->fh = file_number;
	}

	pthread_mutex_unlock(&mutex);

	if(!file)
		return -errno;

	return 0;

}

static int chirp_fuse_release(const char *path, struct fuse_file_info *fi)
{
	struct chirp_file *file;
	int result;

	pthread_mutex_lock(&mutex);

	file = itable_lookup(file_table, fi->fh);
	if(file) {
		chirp_global_close(file, time(0) + chirp_fuse_timeout);
		itable_remove(file_table, fi->fh);
		fi->fh = 0;
		result = 0;
	} else {
		result = -EBADF;
	}

	pthread_mutex_unlock(&mutex);

	return result;
}

static int chirp_fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	struct chirp_file *file;
	INT64_T result;

	pthread_mutex_lock(&mutex);

	file = itable_lookup(file_table, fi->fh);
	if(file) {
		result = chirp_global_pread(file, buf, size, offset, time(0) + chirp_fuse_timeout);
	} else {
		result = -1;
		errno = EBADF;
	}

	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;

	return result;
}

static int chirp_fuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	struct chirp_file *file;
	INT64_T result;

	pthread_mutex_lock(&mutex);

	file = itable_lookup(file_table, fi->fh);
	if(file) {
		result = chirp_global_pwrite(file, buf, size, offset, time(0) + chirp_fuse_timeout);
	} else {
		result = -1;
		errno = EBADF;
	}

	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;

	return result;
}

static int chirp_fuse_mknod(const char *path, mode_t mode, dev_t rdev)
{
	struct chirp_file *file;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];

	parsepath(path, newpath, host);

	pthread_mutex_lock(&mutex);
	file = chirp_global_open(host, newpath, O_CREAT|O_WRONLY, mode, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(!file)
		return -errno;

	pthread_mutex_lock(&mutex);
	chirp_global_close(file, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	return 0;
}


#if FUSE_USE_VERSION==22
static int chirp_fuse_statfs(const char *path, struct statfs *info)
#else
static int chirp_fuse_statfs(const char *path, struct statvfs *info)
#endif
{
	INT64_T result;
	char newpath[CHIRP_PATH_MAX];
	char host[CHIRP_PATH_MAX];
	parsepath(path, newpath, host);
	struct chirp_statfs cinfo;

	pthread_mutex_lock(&mutex);
	result = chirp_global_statfs(host, newpath, &cinfo, time(0) + chirp_fuse_timeout);
	pthread_mutex_unlock(&mutex);

	if(result < 0)
		return -errno;

	memset(info, 0, sizeof(*info));

#if FUSE_USE_VERSION==22
	info->f_type = cinfo.f_type;
#endif
	info->f_bsize = cinfo.f_bsize;
	info->f_blocks = cinfo.f_blocks;
	info->f_bfree = cinfo.f_bfree;
	info->f_bavail = cinfo.f_bavail;
	info->f_files = cinfo.f_files;
	info->f_ffree = cinfo.f_ffree;

	return 0;
}

static struct fuse_operations chirp_fuse_operations = {
	.access = chirp_fuse_access,
	.chmod = chirp_fuse_chmod,
	.chown = chirp_fuse_chown,
	.getattr = chirp_fuse_getattr,
	.link = chirp_fuse_link,
	.mkdir = chirp_fuse_mkdir,
	.mknod = chirp_fuse_mknod,
	.open = chirp_fuse_open,
	.read = chirp_fuse_read,
	.readdir = chirp_fuse_readdir,
	.readlink = chirp_fuse_readlink,
	.release = chirp_fuse_release,
	.rename = chirp_fuse_rename,
	.rmdir = chirp_fuse_rmdir,
	.statfs = chirp_fuse_statfs,
	.symlink = chirp_fuse_symlink,
	.truncate = chirp_fuse_truncate,
	.unlink = chirp_fuse_unlink,
	.utime = chirp_fuse_utime,
	.write = chirp_fuse_write,
};

static struct fuse *fuse_instance = 0;
static struct fuse_chan *fuse_chan = 0;
static char *fuse_mountpoint;

static void exit_handler(int sig)
{
	if(fuse_instance) {
		fuse_exit(fuse_instance);
		fuse_unmount(fuse_mountpoint, fuse_chan);
		fuse_destroy(fuse_instance);
	}
	_exit(0);
}

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("use: %s <mountpath>\n", cmd);
	printf("where options are:\n");
	printf(" -a <flag>    Require this authentication mode.\n");
	printf(" -b <bytes>   Block size for network I/O. (default is %ds)\n", (int) chirp_reli_blocksize_get());
	printf(" -d <flag>    Enable debugging for this subsystem.\n");
	printf(" -D           Disable small file optimizations such as recursive delete.\n");
	printf(" -f           Run in foreground for debugging.\n");
	printf(" -i <files> Comma-delimited list of tickets to use for authentication.\n");
	printf(" -m <options> Mount options passed to FUSE.\n");
	printf(" -o <file>    Send debugging output to this file.\n");
	printf(" -t <timeout> Timeout for network operations. (default is %ds)\n", chirp_fuse_timeout);
	printf(" -v           Show program version.\n");
	printf(" -h           This message.\n");
}

int main(int argc, char *argv[])
{
	char c;
	int did_explicit_auth = 0;
	char *tickets = NULL;
	char *fa_argv[2] = {NULL, NULL};
	struct fuse_args fa;
	fa.argc = 0;
	fa.argv = fa_argv;
	fa.allocated = 0;

	debug_config(argv[0]);

	while((c = getopt(argc, argv, "d:Db:i:m:o:a:t:fhv")) != -1) {
		switch (c) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'D':
			enable_small_file_optimizations = 0;
			break;
		case 'b':
			chirp_reli_blocksize_set(atoi(optarg));
			break;
		case 'i':
			tickets = strdup(optarg);
			break;
		case 'm':
			fa.argc = 1;
			fa.argv[0] = xstrdup(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'a':
			auth_register_byname(optarg);
			did_explicit_auth = 1;
			break;
		case 't':
			chirp_fuse_timeout = string_time_parse(optarg);
			break;
		case 'f':
			run_in_foreground = 1;
			break;
		case 'v':
			show_version(argv[0]);
			return 0;
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
			break;
		}
	}

	if((argc - optind) != 1) {
		show_help(argv[0]);
		return 1;
	}

	fuse_mountpoint = argv[optind];

	if(!did_explicit_auth)
		auth_register_all();
	if(tickets) {
		auth_ticket_load(tickets);
		free(tickets);
	} else if(getenv(CHIRP_CLIENT_TICKETS)) {
		auth_ticket_load(getenv(CHIRP_CLIENT_TICKETS));
	} else {
		auth_ticket_load(NULL);
	}

	file_table = itable_create(0);

	signal(SIGHUP, exit_handler);
	signal(SIGINT, exit_handler);
	signal(SIGTERM, exit_handler);

	fuse_chan = fuse_mount(fuse_mountpoint, &fa);
	if(!fuse_chan) {
		fprintf(stderr, "chirp_fuse: couldn't access %s\n", fuse_mountpoint);
		return 1;
	}

	fuse_instance = fuse_new(fuse_chan, &fa, &chirp_fuse_operations, sizeof(chirp_fuse_operations), 0);
	if(!fuse_instance) {
		fuse_unmount(fuse_mountpoint, fuse_chan);
		fprintf(stderr, "chirp_fuse: couldn't access %s\n", fuse_mountpoint);
		return 1;
	}

	printf("chirp_fuse: mounted chirp on %s\n", fuse_mountpoint);
#ifdef CCTOOLS_OPSYS_DARWIN
	printf("chirp_fuse: to unmount: umount %s\n", fuse_mountpoint);
#else
	printf("chirp_fuse: to unmount: fusermount -u %s\n", fuse_mountpoint);
#endif

	fflush(0);

	if(!run_in_foreground)
		daemon(0, 0);

	fuse_loop(fuse_instance);

	fuse_unmount(fuse_mountpoint, fuse_chan);
	fuse_destroy(fuse_instance);

	return 0;
}

#else

#include <stdio.h>

int main(int argc, char *argv[])
{
	printf("%s: sorry, fuse support was not built in\n", argv[0]);
	return 1;
}

#endif
