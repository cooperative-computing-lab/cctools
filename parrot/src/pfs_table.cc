/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "pfs_search.h"
#include "pfs_table.h"
#include "pfs_service.h"
#include "pfs_pointer.h"
#include "pfs_file.h"
#include "pfs_mmap.h"
#include "pfs_process.h"
#include "pfs_file_cache.h"
#include "pfs_resolve.h"

extern "C" {
#include "pfs_channel.h"

#include "buffer.h"
#include "debug.h"
#include "full_io.h"
#include "get_canonical_path.h"
#include "hash_table.h"
#include "macros.h"
#include "md5.h"
#include "memfdexe.h"
#include "path.h"
#include "pattern.h"
#include "random.h"
#include "stringtools.h"
}

#include <dirent.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>

#ifndef major
/* glibc 2.26 drops sys/sysmacros.h from sys/types.h, thus we add it here */
#include <sys/sysmacros.h>
#endif

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef O_CLOEXEC
#	define O_CLOEXEC 02000000
#endif

#define E_OK 10000

extern int pfs_force_stream;
extern int pfs_force_sync;
extern int pfs_follow_symlinks;
extern int pfs_enable_small_file_optimizations;
extern int pfs_no_flock;

extern const char * pfs_initial_working_directory;

static const int _SENTINEL1 = 0;
#define NATIVE ((pfs_pointer *)&_SENTINEL1)
static const int _SENTINEL2 = 0;
#define SPECIAL ((pfs_pointer *)&_SENTINEL2)

#define SPECIAL_POINTER(pointer) (pointer == SPECIAL)
#define NATIVE_POINTER(pointer) (pointer == NATIVE)
#define PARROT_POINTER(pointer) (!(SPECIAL_POINTER(pointer) || NATIVE_POINTER(pointer) || pointer == NULL))

#define VALID_FD(fd) (0 <= fd && fd < pointer_count)
#define PARROT_FD(fd) (VALID_FD(fd) && PARROT_POINTER(pointers[fd]))

#define CHECK_FD(fd) \
	do {\
		if (!PARROT_FD(fd))\
			return (errno = EBADF, -1);\
	} while (0)

pfs_table::pfs_table()
{
	int i;

	if(pfs_initial_working_directory) {
		strcpy(working_dir,pfs_initial_working_directory);
	} else {
		::getcwd(working_dir,sizeof(working_dir));
	}
	pointer_count = sysconf(_SC_OPEN_MAX);
	pointers = new pfs_pointer* [pointer_count];
	fd_flags = new int[pointer_count];
	mmap_list = 0;

	for( i=0; i<pointer_count; i++ ) {
		pointers[i] = 0;
		fd_flags[i] = 0;
	}
}

pfs_table::~pfs_table()
{
	for(int i=0;i<pointer_count;i++) {
		this->close(i);
	}

	while(mmap_list) {
		pfs_mmap *m = mmap_list;
		mmap_list = m->next;
		delete m;
	}

	delete [] pointers;
	delete [] fd_flags;
}

pfs_table * pfs_table::fork()
{
	pfs_table *table = new pfs_table;

	for(int i=0;i<pointer_count;i++) {
		if(this->pointers[i]) {
			table->fd_flags[i] = this->fd_flags[i];
			table->pointers[i] = this->pointers[i];
			if (PARROT_POINTER(this->pointers[i])) {
				this->pointers[i]->addref();
				this->pointers[i]->file->addref();
			}
		}
	}

	strcpy(table->working_dir,this->working_dir);

	pfs_mmap *m;

	for(m=mmap_list;m;m=m->next) {
		pfs_mmap *n = new pfs_mmap(m);
		n->next = table->mmap_list;
		table->mmap_list = n;
	}

	return table;
}

void pfs_table::setparrot(int fd, int rfd, struct stat *buf)
{
	if (!PARROT_FD(fd))
		fatal("fd %d is not an open parrotfd", fd);

	if(fd == rfd || (VALID_FD(rfd) && pointers[rfd] == NULL)) {
		/* do nothing */
	} else {
		fatal("setparrot: fd %d rfd %d valid %d ptr %p",fd,rfd,VALID_FD(rfd),pointers[rfd]);
	}

	assert(fd == rfd || (VALID_FD(rfd) && pointers[rfd] == NULL));

	/* It's possible for another thread to create a native fd which is equal to
	 * the parrot fd. If that happens we change the parrot fd to what the
	 * kernel gave us. Keep in mind that we don't need to worry about another
	 * racing thread which overwrites pointers[fd] with NATIVE because after
	 * opening a parrot fd, we ignore other tracees and wait for openat to
	 * return the actual parrot fd.
	 */

	if (rfd != fd) {
		debug(D_DEBUG, "parrotfd %d changed to real fd %d", fd, rfd);
		pointers[rfd] = pointers[fd];
		fd_flags[rfd] = fd_flags[fd];
		pointers[fd] = NULL;
		fd_flags[fd] = 0;
		fd = rfd;
	}

	debug(D_DEBUG, "setting parrotfd %d to %p (%d:%d)", fd, pointers[fd], (int)buf->st_dev, (int)buf->st_ino);
	assert(S_ISREG(buf->st_mode));
	pointers[fd]->bind(buf->st_dev, buf->st_ino);
}

int pfs_table::bind( int fd, char *lpath, size_t len )
{
	if (!isnative(fd))
		return (errno = EBADF, -1);
	assert(strlen(lpath) > 0);

	/* Resolve the path... */
	struct pfs_name pname;
	if (!resolve_name(1, lpath, &pname, F_OK))
		return -1;

	if (!pname.is_local)
		return (errno = EOPNOTSUPP, -1);
	if (strlen(pname.rest) >= len)
		return (errno = ENAMETOOLONG, -1);

	strcpy(lpath, pname.rest);

	return 0;
}

void pfs_table::close_on_exec()
{
	for(int i=0;i<pointer_count;i++) {
		if(pointers[i] /* parrot, special, or native */ && fd_flags[i]&FD_CLOEXEC) {
			assert(pointers[i] != SPECIAL);
			debug(D_DEBUG, "closing on exec: %d", i);
			close(i);
		}
	}

	pfs_mmap *m;

	while(mmap_list) {
		m = mmap_list;
		mmap_list = m->next;
		delete m;
	}
}

/*
Connect this logical file descriptor in the table
to this physical file descriptor in the tracing process.
*/

void pfs_table::attach( int logical, int physical, int flags, mode_t mode, const char *name, struct stat *buf )
{
	char selfname[PATH_MAX] = "";
	assert(VALID_FD(logical) && pointers[logical] == NULL);
	if (!name) {
		char path[PATH_MAX];
		snprintf(path, PATH_MAX, "/proc/self/fd/%d", physical);
		if (::readlink(path, selfname, sizeof selfname - 1) == -1) {
			fatal("could not get name for fd %d: %s", physical, strerror(errno));
		}
		name = selfname;
	}
	pointers[logical] = new pfs_pointer(pfs_file_bootstrap(physical,name),flags,mode);
	fd_flags[logical] = 0;
	setparrot(logical, logical, buf);
}

void pfs_table::setnative( int fd, int fdflags )
{
	debug(D_DEBUG, "setting fd %d as native%s", fd, fdflags & FD_CLOEXEC ? " (FD_CLOEXEC)" : "");
	assert(VALID_FD(fd) && (pointers[fd] == NULL || pointers[fd] == NATIVE));
	pointers[fd] = NATIVE;
	fd_flags[fd] = fdflags;
}

void pfs_table::setspecial( int fd )
{
	debug(D_DEBUG, "setting fd %d as special", fd);
	assert(VALID_FD(fd) && pointers[fd] == NULL);
	pointers[fd] = SPECIAL;
	fd_flags[fd] = 0;
}

int pfs_table::isvalid( int fd )
{
	return VALID_FD(fd);
}

int pfs_table::isnative( int fd )
{
	return VALID_FD(fd) && pointers[fd] == NATIVE;
}

int pfs_table::isparrot( int fd )
{
	return PARROT_FD(fd);
}

int pfs_table::isspecial( int fd )
{
	return VALID_FD(fd) && pointers[fd] == SPECIAL;
}

void pfs_table::recvfd( pid_t pid, int fd )
{
	struct stat buf;
	if (pfs_process_stat(pid, fd, &buf) == -1)
		fatal("could not stat %d: %s", fd, strerror(errno));

	debug(D_DEBUG, "received fd %d", fd);

	pfs_pointer *pointer = pfs_pointer::lookup(buf.st_dev, buf.st_ino);
	if (pointer) {
		debug(D_DEBUG, "binding parrotfd %d to %p", fd, pointer);
		pointers[fd] = pointer;
		fd_flags[fd] = 0;
		/* No need to increment reference, sendfd (below) did so. */
	} else {
		setnative(fd, 0);
	}
}

void pfs_table::sendfd( int fd, int errored )
{
	if (PARROT_POINTER(pointers[fd])) {
		if (errored == 0) {
			char path[4096];
			get_full_name(fd, path);
			debug(D_DEBUG, "sending parrot fd %d: `%s'", fd, path);
			pointers[fd]->addref();
			pointers[fd]->file->addref();
		} else {
			/* the kernel raised an error sending the fd, decrement the reference count */
			pointers[fd]->delref();
			pointers[fd]->file->delref();
		}
	} else if (pointers[fd] == NATIVE && errored == 0) {
		debug(D_DEBUG, "sending native fd %d", fd);
	} /* else SPECIAL, we don't care */
}

/* Chose the lowest numbered file descriptor that is available. */

int pfs_table::find_empty( int lowest )
{
	int fd;

	for( fd=lowest; fd<pointer_count; fd++ ) {
		if( !pointers[fd] ) {
			return fd;
		}
	}

	return -1;
}

/*
If short_path is an absolute path, copy it to full path.
Otherwise, tack the current or symlink directory on to the front
of short_path, and copy it to full_path.
*/

void pfs_table::complete_path( const char *short_path, const char *parent_dir, char *full_path )
{
	if( short_path[0]=='/' ) {
		strcpy(full_path,short_path);
	} else {
		strcpy(full_path,parent_dir?parent_dir:working_dir);
		strcat(full_path,"/");
		strcat(full_path,short_path);
	}
	assert(full_path[0] == '/');
}

/*
Complete a path, starting with this fd assumed to be a directory.
*/

#ifndef AT_FDCWD
#	define AT_FDCWD -100
#endif

int pfs_table::complete_at_path( int dirfd, const char *path, char *full_path )
{
	if(path) {
		if(path[0]=='/') {
			strcpy(full_path,path);
		} else {
			if(dirfd==AT_FDCWD) {
				sprintf(full_path,"%s/%s",working_dir,path);
			} else {
				if (get_full_name(dirfd,full_path) == -1) return -1;
				strcat(full_path,"/");
				strcat(full_path,path);
			}
		}
	} else {
		/* some *at syscalls (see utimensat) allow path to be NULL, fill full_path with path of dirfd */
		if(dirfd==AT_FDCWD) {
			strcpy(full_path,working_dir);
		} else {
			if (get_full_name(dirfd,full_path) == -1) return -1;
		}
	}
	debug(D_DEBUG, "%s: `%s' -> `%s'", __func__, path, full_path);
	return 0;
}

void pfs_table::follow_symlink( struct pfs_name *pname, mode_t mode, int depth )
{
	char link_target[PFS_PATH_MAX];
	char link_parent[PFS_PATH_MAX];
	struct pfs_name new_pname = *pname;
	int in_proc = false;

	if (string_prefix_is(pname->path, "/proc/")) in_proc = true;

	int rlres = new_pname.service->readlink(pname,link_target,PFS_PATH_MAX-1);
	if (rlres > 0) {
		/* readlink does not NULL-terminate */
		link_target[rlres] = '\000';

		/*
		 * Some locations in /proc (e.g. /proc/$PID/ns/, /proc/$PID/fd/
		 * with pipes) might contain magic dangling symlinks that can
		 * nonetheless be opened as usual. If Parrot tries to follow them,
		 * it will return erroneous ENOENT. While under /proc, don't try
		 * to follow symlinks of this form.
		 */
		if (in_proc && string_match_regex(link_target, "^[a-z]+:\\[[0-9]+\\]$")) return;
		if (in_proc && string_match_regex(link_target, "^anon_inode:\\[?[a-zA-Z_0-9]+\\]?$")) return;

		const char *basename_start = path_basename(pname->logical_name);
		size_t dirname_len = basename_start - pname->logical_name;
		strncpy(link_parent, pname->logical_name, dirname_len);
		link_parent[dirname_len] = '\0';
		if (resolve_name(0, link_target, &new_pname, mode, true, depth + 1, link_parent)) {
			*pname = new_pname;
		}
	}
}

/*
Given a logical name from the application, expand it into
a fully-qualified logical name, resolve it according to
the mount list, split it into its components, and fill
in the name structure. Return true on success, false otherwise.
*/
extern int pfs_master_timeout;

extern FILE *namelist_file;
extern struct hash_table *namelist_table;

/*
All the syscalls calling "resolve_name" function can be divided into two categories: special_syscall & others.
special_syscall: {"open_object", "bind32", "connect32", "bind64", "connect64", "truncate", "link1", "mkalloc", "lsalloc", "whoami", "md5", "copyfile1", "copyfile2"};
As for special_syscall, the copy degree of the involved file will be fullcopy; the copy degree of files involved in other syscalls will be metadatacopy.
The following syscalls were fullcopy before, but now become metadatacopy. -- "lstat", "stat", "follow_symlink", "link2", "symlink2", "readlink", "unlink"
*/
void namelist_table_insert(const char *content, int is_special_syscall) {
	char *item_value;
	item_value = (char *)hash_table_lookup(namelist_table, content);
	const char *METADATA, *FULLCOPY;
	METADATA = "metadatacopy";
	FULLCOPY = "fullcopy";
	if(!item_value) {
		if(is_special_syscall) {
			hash_table_insert(namelist_table, content, FULLCOPY);
		} else {
			hash_table_insert(namelist_table, content, METADATA);
		}
	} else if(item_value == METADATA && is_special_syscall) {
		hash_table_remove(namelist_table, content);
		hash_table_insert(namelist_table, content, FULLCOPY);
	}
}

int pfs_table::resolve_name(int is_special_syscall, const char *cname, struct pfs_name *pname, mode_t mode, bool do_follow_symlink, int depth, const char *parent_dir ) {
	char full_logical_name[PFS_PATH_MAX];
	pfs_resolve_t result;
	size_t n;

	if (depth > PFS_MAX_RESOLVE_DEPTH)
		return errno = ELOOP, 0;

	if(strlen(cname) == 0)
		return errno = ENOENT, 0;

	complete_path(cname,parent_dir,full_logical_name);
	path_collapse(full_logical_name,pname->logical_name,1);

	/* Check permissions to edit parent directory entry. */
	if(mode & E_OK) {
		char dirname[PFS_PATH_MAX];
		char tmp[PFS_PATH_MAX];
		mode &= ~E_OK;
		path_dirname(pname->logical_name, dirname);
		result = pfs_resolve(dirname,tmp,W_OK,time(0)+pfs_master_timeout);
		switch(result) {
			case PFS_RESOLVE_DENIED:
				return errno = EACCES, 0;
			case PFS_RESOLVE_ENOENT:
				return errno = ENOENT, 0;
			case PFS_RESOLVE_FAILED:
				fatal("unable to resolve parent directory %s",dirname);
				return 0;
			default:
				break;
		}
	}

	result = pfs_resolve(pname->logical_name,pname->path,mode,time(0)+pfs_master_timeout);

	if(namelist_table) {
		namelist_table_insert(pname->path, is_special_syscall);
	}

	if(result==PFS_RESOLVE_DENIED) {
		return errno = EACCES, 0;
	} else if(result==PFS_RESOLVE_ENOENT) {
		return errno = ENOENT, 0;
	} else if(result==PFS_RESOLVE_FAILED) {
		fatal("unable to resolve file %s",pname->logical_name);
		return 0;
	} else {
		char tmp[PFS_PATH_MAX];
		path_split(pname->path,pname->service_name,tmp);
		pname->service = pfs_service_lookup(pname->service_name);
		if(!pname->service) {
			pname->service = pfs_service_lookup_default();
			strcpy(pname->service_name,"local");
			strcpy(pname->host,"localhost");
			strcpy(pname->hostport,"localhost");
			strcpy(pname->rest,pname->path);
			pname->is_local = 1;
		} else if (!strncmp(pname->service_name, "ext_", 4)) {
			strcpy(pname->rest, tmp);
			strcpy(pname->host, "ext");
			strcpy(pname->hostport, "ext");
			pname->port = 0;
		} else {
			if(!strcmp(pname->service_name,"multi")) {// if we're dealing with a multivolume, split off at the @
				path_split_multi(tmp,pname->host,pname->rest);
			} else {
				path_split(tmp,pname->host,pname->rest);
			}

			if(!pname->host[0]) {
				pname->hostport[0] = 0;
				pname->rest[0] = 0;
				return 1;
			}

			if (!strcmp(pname->service_name, "grow") && !strcmp(pname->host, "local")) {
				pname->host[0] = 0;
				pname->port = 0;
				strcpy(pname->hostport, "local");
			} else {
				char *c = strrchr(pname->host, ':');
				if(c) {
					*c = 0;
					pname->port = atoi(c+1);
				} else {
					pname->port = pname->service->get_default_port();
				}
				sprintf(pname->hostport,"%s:%d",pname->host,pname->port);
			}

			if(!strcmp(pname->service_name,"multi")) {
				strcpy(tmp,pname->rest);
				path_split(tmp,&pname->hostport[strlen(pname->hostport)],pname->rest); // reconstruct hostport as host:port@volume; path goes in rest.
			}
			if(pname->service->tilde_is_special() && !strncmp(pname->rest,"/~",2)) {
				memmove(pname->rest,&pname->rest[1],strlen(pname->rest));
			}
			pname->is_local = 0;
		}

		if(pattern_match(pname->path, "^/proc/self/?()", &n) >= 0) {
			strncpy(full_logical_name, pname->path, sizeof(full_logical_name));

			string_nformat(pname->path, sizeof(pname->path), "/proc/%d/%s", pfs_process_getpid(), &full_logical_name[n]);

			strcpy(pname->logical_name, pname->path);
			strcpy(pname->rest, pname->path);
			pname->service = pfs_service_lookup_default();
			strcpy(pname->service_name,"local");
			strcpy(pname->host,"localhost");
			strcpy(pname->hostport,"localhost");
			pname->is_local = 1;
		} else if (pattern_match(pname->path, "^/dev/fd/?()", &n) >= 0) {
			strncpy(full_logical_name, pname->path, sizeof(full_logical_name));
			string_nformat(pname->path, sizeof(pname->path), "/proc/%d/fd/%s", pfs_process_getpid(), &full_logical_name[n]);
			strcpy(pname->logical_name, pname->path);
			strcpy(pname->rest, pname->path);
			pname->service = pfs_service_lookup_default();
			strcpy(pname->service_name,"local");
			strcpy(pname->host,"localhost");
			strcpy(pname->hostport,"localhost");
			pname->is_local = 1;
		}

		/* Enable cross service symlink resolution */
		if (do_follow_symlink && pfs_follow_symlinks) {
			follow_symlink(pname, mode, depth + 1);
		}

		return 1;
	}
}

pfs_dir * pfs_table::open_directory(pfs_name *pname, int flags)
{
	pfs_dir *file;
	if((flags&O_RDWR)||(flags&O_WRONLY)) {
		errno = EISDIR;
		file = 0;
	} else {
		file = pname->service->getdir(pname);
	}
	return file;
}

pfs_pointer *pfs_table::getopenfile( pid_t pid, int fd )
{
	struct pfs_process *target = pfs_process_lookup(pid);
	if(target && target->table) {
		if (!target->table->isvalid(fd)) {
			return (errno = ENOENT, (pfs_pointer *)NULL);
		}
		pfs_pointer *desc = target->table->pointers[fd];
		if (PARROT_POINTER(desc)) {
			return desc;
		} else if (NATIVE_POINTER(desc)) {
			return (errno = ECHILD, (pfs_pointer *)NULL); /* hack, allow open to proceed natively */
		} else {
			assert(desc == SPECIAL || desc == NULL);
			return (errno = ENOENT, (pfs_pointer *)NULL);
		}
	} else {
		return (errno = ESRCH, (pfs_pointer *)NULL);
	}
}

pfs_file * pfs_table::open_object( const char *lname, int flags, mode_t mode, int force_cache )
{
	pfs_name pname;
	pfs_file *file=0;
	mode_t open_mode = X_OK;
	int force_stream = pfs_force_stream;

	if(flags & O_RDWR) {
		open_mode |= R_OK|W_OK;
	} else if (flags & O_WRONLY) {
		open_mode |= W_OK;
	} else {
		open_mode |= R_OK;
	}

	// Hack: Disable caching when doing plain old file copies.

		if(
				!strcmp(pfs_current->name,"cp") ||
				!strcmp(string_back(pfs_current->name,3),"/cp")
		) {
				force_stream = 1;
		}

	// Hack: Almost all calls to open a directory are routed
	// through opendir(), which sets O_DIRECTORY.  In a few
	// cases, such as the use of openat in pwd, the flag
	// is not set, set we detect it here.

	const char *basename = path_basename(lname);
	if(!strcmp(basename,".") || !strcmp(basename,"..")) {
		flags |= O_DIRECTORY;
	}

	// If a file is opened with O_CREAT, we should check for write permissions
	// on the parent directory. However, this seems to cause problems if
	// system directories (or the filesystem root) are marked RO.
	if(resolve_name(1,lname,&pname,open_mode)) {
		if((flags&O_CREAT) && (flags&O_DIRECTORY)) {
			// Linux ignores O_DIRECTORY in this combination
			flags &= ~O_DIRECTORY;
		}
		char *pid = NULL;
		if(flags&O_DIRECTORY) {
			if (pattern_match(pname.rest, "^/proc/(%d+)/fd/?$", &pid) >= 0) {
				int i;
				pfs_dir *dir = new pfs_dir(&pname);
				pid_t ipid = atoi(pid);
				/* idea here is to not include a SPECIAL fd in this directory */
				for (i = 0; i < pointer_count; i++) {
					pfs_pointer *desc = getopenfile(ipid, i);
					if (desc || errno == ECHILD) {
						struct dirent dirent;
						dirent.d_ino = random_uint();
						dirent.d_off = 0;
						dirent.d_reclen = sizeof(dirent);
						snprintf(dirent.d_name, sizeof(dirent.d_name), "%d", i);
						dirent.d_type = DT_LNK;
						dir->append(&dirent);
					}
				}
				file = dir;
			} else {
				file = open_directory(&pname, flags);
			}
		} else if(pname.service->is_local()) {
			char *fd = NULL;
			if (pattern_match(pname.rest, "^/proc/(%d+)/fd/(%d+)$", &pid, &fd) >= 0) {
				pfs_pointer *desc = getopenfile(atoi(pid), atoi(fd));
				if (desc) {
					desc->file->addref();
					return desc->file;
				} else if (errno == ESRCH || errno == ECHILD) {
					/* outside of Parrot or native, let kernel deal with it... */
					file = pname.service->open(&pname,flags,mode);
					if(!file && (errno == EISDIR)) {
						file = open_directory(&pname, flags);
					}
				}
			} else if (pattern_match(pname.rest, "^/proc/(%d+)/maps$", &pid) >= 0) {
				extern char pfs_temp_per_instance_dir[PATH_MAX];
				static const char name[] = "parrot-maps";

				int fd = memfdexe(name, pfs_temp_per_instance_dir);
				if (fd >= 0) {
					buffer_t B[1];
					buffer_init(B);
					mmap_proc(atoi(pid), B);
					full_write(fd, buffer_tostring(B), buffer_pos(B));
					::lseek(fd, 0, SEEK_SET);
					buffer_free(B);
					file = pfs_file_bootstrap(fd, name);
				} else {
					errno = ENOENT;
					file = 0;
				}
			} else {
				file = pname.service->open(&pname,flags,mode);
				if(!file && (errno == EISDIR)) {
					file = open_directory(&pname, flags);
				}
			}
			free(fd);
		} else if(pname.service->is_seekable()) {
			if(force_cache) {
				file = pfs_cache_open(&pname,flags,mode);
				if(!file && (errno == EISDIR)) {
					file = open_directory(&pname, flags);
				}
			} else {
				file = pname.service->open(&pname,flags,mode);
				if(!file && (errno == EISDIR)) {
					file = open_directory(&pname, flags);
				}
			}
		} else {
			if(force_stream) {
				file = pname.service->open(&pname,flags,mode);
				if(!file && (errno == EISDIR)) {
					file = open_directory(&pname, flags);
				}
			} else {
				file = pfs_cache_open(&pname,flags,mode);
				if(!file && (errno == EISDIR)) {
					file = open_directory(&pname, flags);
				}
			}
		}
		free(pid);
	} else {
		file = 0;
	}

	return file;
}

int pfs_table::open( const char *lname, int flags, mode_t mode, int force_cache, char *path, size_t len )
{
	int result = -1;
	pfs_file *file=0;

	/* Apply the umask to our mode */
	mode = mode &~(pfs_current->umask);

#if defined(linux) & !defined(O_BINARY)
#define O_BINARY 0x8000
#endif

	/* Get rid of meaningless undocumented flags */
	flags = flags & ~O_BINARY;

#ifdef O_SYNC
	if(pfs_force_sync) flags |= O_SYNC;
#endif

	result = find_empty(0);
	if(result>=0) {
		file = open_object(lname,flags,mode,force_cache);
		if(file) {
			if(path && file->canbenative(path, len)) {
				file->close();
				result = -2;
			} else {
				pointers[result] = new pfs_pointer(file,flags,mode);
				fd_flags[result] = 0;
				if (flags&O_CLOEXEC)
					fd_flags[result] |= FD_CLOEXEC;
				if(flags&O_APPEND) this->lseek(result,0,SEEK_END);
			}
		} else if (errno == ECHILD /* hack: indicates to open natively */) {
			snprintf(path, len, "%s", lname);
			result = -2;
		} else {
			result = -1;
		}
	} else {
		errno = EMFILE;
		result = -1;
	}

	return result;
}

int pfs_table::get_real_fd( int fd )
{
	CHECK_FD(fd);
	return pointers[fd]->file->get_real_fd();
}

int pfs_table::get_full_name( int fd, char *name )
{
	CHECK_FD(fd);
	strcpy(name,pointers[fd]->file->get_name()->path);
	return 0;
}

int pfs_table::get_local_name( int fd, char *name )
{
	CHECK_FD(fd);
	return pointers[fd]->file->get_local_name(name);
}

/*
Close is a little tricky.
The file pointer might be in use by several dups,
or the file itself might be in use by several opens.
*/

int pfs_table::close( int fd )
{
	/* FIXME: if a previously mmaped file is written to, we ought to clean up
	 * the channel cache on close. Otherwise, subsequent mmaps might return
	 * stale data. Related:
	 * https://github.com/cooperative-computing-lab/cctools/issues/1584
	 */

	if (isnative(fd)) {
		debug(D_DEBUG, "marking closed native fd %d", fd);
		pointers[fd] = NULL;
		fd_flags[fd] = 0;
		return 0;
	} else {
		CHECK_FD(fd);

		debug(D_DEBUG, "closing parrot fd %d", fd);
		pfs_pointer *p = pointers[fd];
		pfs_file *f = p->file;

		int result = 0;

		if(f->refs()==1) {
			result = f->close();
			delete f;
		} else {
			f->delref();
		}

		if(p->refs()==1) {
			delete p;
		} else {
			p->delref();
		}

		pointers[fd]=0;
		fd_flags[fd]=0;
		return result;
	}
}

pfs_ssize_t pfs_table::read( int fd, void *data, pfs_size_t nbyte )
{
	pfs_ssize_t result = -1;

	CHECK_FD(fd);

	result = this->pread(fd,data,nbyte,pointers[fd]->tell());
	if(result>0) pointers[fd]->bump(result);

	return result;
}

pfs_ssize_t pfs_table::write( int fd, const void *data, pfs_size_t nbyte )
{
	pfs_ssize_t result = -1;

	CHECK_FD(fd);

	result = this->pwrite(fd,data,nbyte,pointers[fd]->tell());
	if(result>0) pointers[fd]->bump(result);

	return result;
}

static void stream_warning( pfs_file *f )
{
	if(!f->get_name()->is_local && !pfs_current->did_stream_warning) {
		debug(D_NOTICE,"Program: %s",pfs_current->name);
		debug(D_NOTICE,"Is using file: %s",f->get_name()->path);
		debug(D_NOTICE,"For non-sequential access.");
		debug(D_NOTICE,"This won't work with streaming (-s) turned on.");
		pfs_current->did_stream_warning = 1;
	}
}

pfs_ssize_t pfs_table::pread( int fd, void *data, pfs_size_t nbyte, pfs_off_t offset )
{
	pfs_ssize_t result = -1;

	CHECK_FD(fd);

	if( (!data) || (nbyte<0) ) {
		errno = EINVAL;
		result = -1;
	} else if( nbyte==0 ) {
		result = 0;
	} else {
		pfs_file *f = pointers[fd]->file;
		if(!f->is_seekable() && f->get_last_offset()!=offset) {
			stream_warning(f);
			errno = ESPIPE;
			result = -1;
		} else {
			result = f->read( data, nbyte, offset );
			if(result>0) f->set_last_offset(offset+result);
		}
	}

	return result;
}

pfs_ssize_t pfs_table::pwrite( int fd, const void *data, pfs_size_t nbyte, pfs_off_t offset )
{
	pfs_ssize_t result = -1;

	CHECK_FD(fd);

	if( (!data) || (nbyte<0) ) {
		errno = EINVAL;
		result = -1;
	} else if( nbyte==0 ) {
		result = 0;
	} else {
		pfs_file *f = pointers[fd]->file;
		if(!f->is_seekable() && f->get_last_offset()!=offset) {
			stream_warning(f);
			errno = ESPIPE;
			result = -1;
		} else {
			result = f->write( data, nbyte, offset );
			if(result>0) f->set_last_offset(offset+result);
		}
	}

	return result;
}

pfs_ssize_t pfs_table::readv( int fd, const struct iovec *vector, int count )
{
	int i;
	pfs_ssize_t result = 0;
	pfs_ssize_t chunk;

	CHECK_FD(fd);

	for( i = 0; i < count; i++ ) {
		chunk = this->read( fd, vector->iov_base, vector->iov_len );
		if( chunk < 0 ) return chunk;
		result += chunk;
		if( chunk != (pfs_ssize_t) vector->iov_len ) return result;
		vector++;
	}

	return result;
}

pfs_ssize_t pfs_table::writev( int fd, const struct iovec *vector, int count )
{
	int i;
	pfs_ssize_t result = 0;
	pfs_ssize_t chunk;

	CHECK_FD(fd);

	for( i = 0; i < count; i++ ) {
		chunk = this->write( fd, vector->iov_base, vector->iov_len );
		if( chunk < 0 ) return chunk;
		result += chunk;
		if( chunk != (pfs_ssize_t) vector->iov_len ) return result;
		vector++;
	}

	return result;
}

pfs_off_t pfs_table::lseek( int fd, pfs_off_t offset, int whence )
{
	pfs_file *f;
	pfs_pointer *p;
	pfs_off_t result = -1;

	CHECK_FD(fd);

	p = pointers[fd];
	f = p->file;
	if(!f->is_seekable()) {
		errno = ESPIPE;
		result = -1;
	} else {
		result = p->seek(offset,whence);
	}

	return result;
}

int pfs_table::dup2( int ofd, int nfd, int flags )
{
	if (!VALID_FD(ofd) || !VALID_FD(nfd))
		return (errno = EBADF, -1);
	if (ofd == nfd)
		return nfd;

	debug(D_DEBUG, "dup2(%d, %d, %x)", ofd, nfd, flags);

	close(nfd);

	pointers[nfd] = pointers[ofd];
	if (PARROT_POINTER(pointers[nfd])) {
		pointers[nfd]->addref();
		pointers[nfd]->file->addref();
	}
	fd_flags[nfd] = flags;

	return nfd;
}

int pfs_table::fchdir( int fd )
{
	int result = -1;

	CHECK_FD(fd);

	pfs_name *pname = pointers[fd]->file->get_name();
	result = this->chdir(pname->path);

	return result;
}

int pfs_table::ftruncate( int fd, pfs_off_t size )
{
	int result = -1;

	CHECK_FD(fd);

	if( size<0 ) {
		result = 0;
	} else {
		result = pointers[fd]->file->ftruncate(size);
	}

	return result;
}

int pfs_table::fstat( int fd, struct pfs_stat *b )
{
	int result;

	CHECK_FD(fd);

	pfs_file *file = pointers[fd]->file;
	result = file->fstat(b);
	if(result>=0) {
		b->st_blksize = file->get_block_size();
	}

	return result;
}


int pfs_table::fstatfs( int fd, struct pfs_statfs *buf )
{
	CHECK_FD(fd);

	return pointers[fd]->file->fstatfs(buf);
}


int pfs_table::fsync( int fd )
{
	CHECK_FD(fd);

	return pointers[fd]->file->fsync();
}

int pfs_table::flock( int fd, int op )
{
	CHECK_FD(fd);

	if (pfs_no_flock) return 0;
	return pointers[fd]->file->flock(op);
}

int pfs_table::fcntl( int fd, int cmd, void *arg )
{
	int result;
	int flags;

	if (!VALID_FD(fd))
		return (errno = EBADF, -1);

	/* fcntl may operate on the *file descriptor* table or the *open file description* table */

	if (cmd == F_GETFD || cmd == F_SETFD) {
		if (!(PARROT_POINTER(pointers[fd]) || NATIVE_POINTER(pointers[fd])))
			return (errno = EBADF, -2);
		if (cmd == F_GETFD) {
			result = fd_flags[fd];
		} else if (cmd == F_SETFD) {
			fd_flags[fd] = (intptr_t)arg;
			result = 0;
		} else assert(0);
		return result;
	}

	/* now open file description table: */

	if (!PARROT_POINTER(pointers[fd]))
		return (errno = EBADF, -1);

	switch (cmd) {
		case F_GETFL:
			result = pointers[fd]->flags;
			break;
		case F_SETFL:
			flags = (PTRINT_T)arg;
			pointers[fd]->flags = flags;
			flags |= O_NONBLOCK;
			pointers[fd]->file->fcntl(cmd,(void*)(PTRINT_T)flags);
			result = 0;
			break;

		/*
			A length of zero to FREESP indicates the file
			should be truncated at the start value.
			Otherwise, we don't support it.
		*/

#ifdef F_FREESP
		case F_FREESP:
			{
				struct flock *f = (struct flock *)arg;

				if( (f->l_whence==0) && (f->l_len==0) ) {
					result = this->ftruncate(fd,f->l_start);
				} else {
					errno = ENOSYS;
					result = -1;
				}
			}
			break;
#endif

#ifdef F_FREESP64
		case F_FREESP64:
			{
				struct flock64 *f64 = (struct flock64 *)arg;

				if( (f64->l_whence==0) && (f64->l_len==0) ) {
					result = this->ftruncate(fd,f64->l_start);
				} else {
					errno = ENOSYS;
					result = -1;
				}
			}
			break;
#endif

		default:
			result = pointers[fd]->file->fcntl(cmd,arg);
			break;
	}

	return result;
}

int pfs_table::fchmod( int fd, mode_t mode )
{
	CHECK_FD(fd);

	return pointers[fd]->file->fchmod(mode);
}

int pfs_table::fchown( int fd, struct pfs_process *p, uid_t uid, gid_t gid )
{
	CHECK_FD(fd);

	int result = pointers[fd]->file->fchown(uid,gid);

	/*
	If the service doesn't implement it, but its our own uid,
	then fake success, as tools like cp do this very often.
	*/

	if(result<0 && errno==ENOSYS && uid==p->euid && gid==p->egid) {
		result = 0;
	}

	return result;
}

/*
Some things to note about chdir.

We rely on the underlying service to resolve complex
paths containing symbolic links, parents (..), and so forth,
by performing the chdir and then returning the new canonical
name for the path.  It is not correct for us to simply unwind
such paths ourselves, because by following those elements,
we may end up somewhere completely new.

However, not all services have this capability.  (For example,
rfio.)  So, if the returned canonical name has unusual elements,
they must be cleaned up before they are recorded in the working
directory.
*/

int pfs_table::chdir( const char *path )
{
	int result = -1;
	char newpath[PFS_PATH_MAX];
	pfs_name pname;

	/*
	This is a special case in Unix, do not attempt to complete
	the path and then change directory.
	*/

	if(path[0]==0) {
		errno = ENOENT;
		return -1;
	}

	if(resolve_name(0,path,&pname,X_OK)) {
		result = pname.service->chdir(&pname,newpath);
		if(result>=0) {
			path_collapse(pname.logical_name,working_dir,1);
			result = 0;
		}
	}

	return result;
}

char *pfs_table::getcwd( char *path, pfs_size_t size )
{
	char cwd[PFS_PATH_MAX];
	strcpy(cwd, working_dir);
	path_remove_trailing_slashes(cwd);
	if (strlen(cwd)+1 > (size_t)size) {
		errno = ERANGE;
		return NULL;
	}
	strcpy(path, cwd);
	return path;
}

int pfs_table::access( const char *n, mode_t mode )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(0,n,&pname,X_OK | mode)) {
		result = pname.service->access(&pname,mode);
	}

	return result;
}

int pfs_table::chmod( const char *n, mode_t mode )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,W_OK)) {
		result = pname.service->chmod(&pname,mode);
	}

	return result;
}

int pfs_table::chown( const char *n, struct pfs_process *p, uid_t uid, gid_t gid )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,W_OK)) {
		result = pname.service->chown(&pname,uid,gid);
	}

	/*
	If the service doesn't implement it, but its our own uid,
	then fake success, as tools like cp do this very often.
	*/

	if(result<0 && errno==ENOSYS && uid==p->euid && gid==p->egid) {
		result = 0;
	}

	return result;
}

int pfs_table::lchown( const char *n, uid_t uid, gid_t gid )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,W_OK,false)) {
		result = pname.service->lchown(&pname,uid,gid);
	}

	return result;
}

int pfs_table::truncate( const char *n, pfs_off_t offset )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(1,n,&pname,W_OK)) {
		result = pname.service->truncate(&pname,offset);
	}

	return result;
}

ssize_t pfs_table::getxattr (const char *path, const char *name, void *value, size_t size)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,path,&pname,R_OK)) {
		result = pname.service->getxattr(&pname,name,value,size);
	}

	return result;
}

ssize_t pfs_table::lgetxattr (const char *path, const char *name, void *value, size_t size)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,path,&pname,R_OK,false)) {
		result = pname.service->lgetxattr(&pname,name,value,size);
	}

	return result;
}

ssize_t pfs_table::fgetxattr (int fd, const char *name, void *value, size_t size)
{
	CHECK_FD(fd);

	return pointers[fd]->file->fgetxattr(name,value,size);
}

ssize_t pfs_table::listxattr (const char *path, char *list, size_t size)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,path,&pname,R_OK)) {
		result = pname.service->listxattr(&pname,list,size);
	}

	return result;
}

ssize_t pfs_table::llistxattr (const char *path, char *list, size_t size)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,path,&pname,R_OK,false)) {
		result = pname.service->llistxattr(&pname,list,size);
	}

	return result;
}

ssize_t pfs_table::flistxattr (int fd, char *list, size_t size)
{
	CHECK_FD(fd);

	return pointers[fd]->file->flistxattr(list,size);
}

int pfs_table::setxattr (const char *path, const char *name, const void *value, size_t size, int flags)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,path,&pname,W_OK)) {
		result = pname.service->setxattr(&pname,name,value,size,flags);
	}

	return result;
}

int pfs_table::lsetxattr (const char *path, const char *name, const void *value, size_t size, int flags)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,path,&pname,W_OK,false)) {
		result = pname.service->lsetxattr(&pname,name,value,size,flags);
	}

	return result;
}

int pfs_table::fsetxattr (int fd, const char *name, const void *value, size_t size, int flags)
{
	CHECK_FD(fd);

	return pointers[fd]->file->fsetxattr(name,value,size,flags);
}

int pfs_table::removexattr (const char *path, const char *name)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,path,&pname,W_OK)) {
		result = pname.service->removexattr(&pname,name);
	}

	return result;
}

int pfs_table::lremovexattr (const char *path, const char *name)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,path,&pname,W_OK,false)) {
		result = pname.service->lremovexattr(&pname,name);
	}

	return result;
}

int pfs_table::fremovexattr (int fd, const char *name)
{
	CHECK_FD(fd);

	return pointers[fd]->file->fremovexattr(name);
}

int pfs_table::utime( const char *n, struct utimbuf *buf )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,W_OK)) {
		result = pname.service->utime(&pname,buf);
	}

	return result;
}

int pfs_table::utimens( const char *n, const struct timespec times[2] )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,W_OK)) {
		result = pname.service->utimens(&pname,times);
	}

	return result;
}

int pfs_table::lutimens( const char *n, const struct timespec times[2] )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,W_OK,false)) {
		result = pname.service->lutimens(&pname,times);
	}

	return result;
}


int pfs_table::unlink( const char *n )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(0,n,&pname,E_OK,false)) {
		result = pname.service->unlink(&pname);
		if(result==0) {
			pfs_cache_invalidate(&pname);
			pfs_channel_update_name(pname.path,0);
		}
	}

	return result;
}

int pfs_table::stat( const char *n, struct pfs_stat *b )
{
	pfs_name pname;
	int result = -1;

	/* You don't need to have read permission on a file to stat it. */
	if(resolve_name(0,n,&pname,F_OK)) {
		result = pname.service->stat(&pname,b);
		if(result>=0) {
			b->st_blksize = pname.service->get_block_size();
		} else if(errno==ENOENT && !pname.hostport[0]) {
			pfs_service_emulate_stat(&pname,b);
			b->st_mode = S_IFDIR | 0555;
			result = 0;
		}
	}

	return result;
}

int pfs_table::statfs( const char *n, struct pfs_statfs *b )
{
	pfs_name pname;
	int result = -1;

	/* You don't need to have read permission on a file to stat it. */
	if(resolve_name(0,n,&pname,F_OK)) {
		result = pname.service->statfs(&pname,b);
	}

	return result;
}

int pfs_table::lstat( const char *n, struct pfs_stat *b )
{
	pfs_name pname;
	int result=-1;

	/* You don't need to have read permission on a file to stat it. */
	if(resolve_name(0,n,&pname,F_OK,false)) {
		result = pname.service->lstat(&pname,b);
		if(result>=0) {
			b->st_blksize = pname.service->get_block_size();
		} else if(errno==ENOENT && !pname.hostport[0]) {
			pfs_service_emulate_stat(&pname,b);
			b->st_mode = S_IFDIR | 0555;
			result = 0;
		}
	}

	return result;
}

int pfs_table::rename( const char *n1, const char *n2 )
{
	pfs_name p1, p2;
	int result = -1;

	if(resolve_name(0,n1,&p1,E_OK,false) && resolve_name(0,n2,&p2,E_OK,false)) {
		if(p1.service==p2.service) {
			result = p1.service->rename(&p1,&p2);
			if(result==0) {
				pfs_cache_invalidate(&p1);
				pfs_cache_invalidate(&p2);
				pfs_channel_update_name(p1.path, p2.path);
			}
		} else {
			errno = EXDEV;
		}
	}

	return result;
}

int pfs_table::link( const char *n1, const char *n2 )
{
	pfs_name p1, p2;
	int result = -1;

	// Require write on the target to prevent linking into a RW directory
	// and bypassing restrictions
	if(resolve_name(0,n1,&p1,W_OK,false) && resolve_name(0,n2,&p2,E_OK,false)) {
		if(p1.service==p2.service) {
			result = p1.service->link(&p1,&p2);
		} else {
			errno = EXDEV;
		}
	}

	return result;
}

int pfs_table::symlink( const char *target, const char *path )
{
	pfs_name pname;
	int result = -1;

	/*
	Note carefully: Symlinks are used to store all sorts
	of information by applications.  They need not be
	valid, and we often cannot interpret them at runtime.
	Thus, we only call resolve_name on the link name,
	not on the contents.  The link contents are passed
	verbatim down to the needed driver.
	*/

	if(resolve_name(0,path,&pname,E_OK,false)) {
		result = pname.service->symlink(target,&pname);
	}

	return result;
}

/*
Readlink is ordinarily passed down to each driver.
However, when we are examining the /proc filesystem,
there are a few elements that must be manually interpreted
so that the caller gets the logical name rather than the
physical name, which may have been redirected to the
cache directory.

Note that /proc/self is handled in resolve_name, where it
is manually mapped to /proc/(pid), otherwise the path would
refer to parrot itself.
*/

int pfs_table::readlink( const char *n, char *buf, pfs_size_t size )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,R_OK,false)) {
		char *pid = NULL, *fd = NULL;
		if(pattern_match(pname.path, "^/proc/(%d+)/fd/(%d+)$",&pid,&fd) >= 0) {
			pfs_pointer *desc = getopenfile(atoi(pid), atoi(fd));
			if (desc) {
				const char *path = desc->file->get_name()->path;
				strncpy(buf,path,size);
				result = MIN(strlen(path),(size_t)size);
			} else if (errno == ECHILD) {
				/* native... */
				result = ::readlink(pname.path,buf,size);
			} else {
				result = -1;
			}
		} else if(pattern_match(pname.path, "^/proc/(%d+)/exe", &pid) >= 0) {
			struct pfs_process *target = pfs_process_lookup(atoi(pid));
			if(target) {
				const char *path = target->name;
				size_t count = MIN(strlen(path), (size_t)size);
				memcpy(buf,path,count);
				result = (int)count;
			} else {
				result = pname.service->readlink(&pname,buf,size);
			}
		} else {
			result = pname.service->readlink(&pname,buf,size);
		}
		free(pid);
		free(fd);
	} else {
		result = -1;
		errno = ENOENT;
	}

	return result;
}

int pfs_table::mknod( const char *n, mode_t mode, dev_t dev )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,E_OK)) {
		result = pname.service->mknod(&pname,mode,dev);
	}

	return result;
}

int pfs_table::mkdir( const char *n, mode_t mode )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,E_OK)) {
		result = pname.service->mkdir(&pname,mode);
	}

	return result;
}

int pfs_table::rmdir( const char *n )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,E_OK,false)) {
		result = pname.service->rmdir(&pname);
	}

	return result;
}

struct dirent * pfs_table::fdreaddir( int fd )
{
	if (!PARROT_FD(fd))
		return (errno = EBADF, (struct dirent *)NULL);

	pfs_off_t next_offset;
	pfs_pointer *fp = pointers[fd];
	struct dirent *result = fp->file->fdreaddir(fp->tell(),&next_offset);
	if(result)
		fp->seek(next_offset,SEEK_SET);

	return result;
}

int pfs_table::mkalloc( const char *n, pfs_ssize_t size, mode_t mode )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(0,n,&pname,E_OK)) {
		result = pname.service->mkalloc(&pname,size,mode);
	}

	return result;
}

int pfs_table::lsalloc( const char *n, char *a, pfs_ssize_t *total, pfs_ssize_t *avail )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(1,n,&pname,R_OK)) {
		result = pname.service->lsalloc(&pname,a,total,avail);
		if(result==0) {
			strcpy(a,pname.path);
		}
	}

	return result;
}

int pfs_table::whoami( const char *n, char *buf, int length )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(1,n,&pname,F_OK)) {
		result = pname.service->whoami(&pname,buf,length);
	}

	return result;
}

static int search_to_access (int flags)
{
	int access_flags = F_OK;
	if (flags & PFS_SEARCH_R_OK)
		access_flags |= R_OK;
	if (flags & PFS_SEARCH_W_OK)
		access_flags |= W_OK;
	if (flags & PFS_SEARCH_X_OK)
		access_flags |= X_OK;
	return access_flags;
}

static int search_error (int err, int errsource, char *path, char *buffer, size_t *i, size_t len) {
	size_t n = snprintf(buffer+*i, len-*i,  "%s%d|%d|%s", *i==0 ? "" : "|", err, errsource, path);

	if (n>=len-*i) {
		errno = ERANGE;
		return -1;
	} else {
		*i += n;
		return 0;
	}
}

static int search_stat_pack(const struct pfs_stat *p_info, char *buffer, size_t *i, size_t len) {
	struct stat info;
	COPY_STAT(*p_info, info);
	size_t n = snprintf(buffer + *i, len - *i,
		"|%ld,%ld,%ld,%ld,"
		 "%ld,%ld,%ld,%ld,"
		 "%ld,%ld,%ld,%ld,"
		 "%ld",
		(long)info.st_dev,
		(long)info.st_ino,
		(long)info.st_mode,
		(long)info.st_nlink,
		(long)info.st_uid,
		(long)info.st_gid,
		(long)info.st_rdev,
		(long)info.st_size,
		(long)info.st_atime,
		(long)info.st_mtime,
		(long)info.st_ctime,
		(long)info.st_blksize,
		(long)info.st_blocks
	);

	if (n>=len-*i) {
		return -1;
	} else {
		*i += n;
		return 0;
	}
}

/* NOTICE: this function's logic should be kept in sync with function of same
 * name in chirp_fs_local.c. */
static int search_match_file(const char *pattern, const char *name)
{
	debug(D_DEBUG, "search_match_file(`%s', `%s')", pattern, name);
	/* Decompose the pattern in atoms which are each matched against. */
	while (1) {
		char atom[PFS_PATH_MAX];
		const char *end = strchr(pattern, '|'); /* disjunction operator */

		memset(atom, 0, sizeof(atom));
		if (end)
			strncpy(atom, pattern, (size_t)(end-pattern));
		else
			strcpy(atom, pattern);

		/* Here we might have a pattern like '*' which matches any file so we
		 * iteratively pull leading components off of `name' until we get a
		 * match.  In the case of '*', we would pull off all leading components
		 * until we reach the file name, which would always match '*'.
		 */
		const char *test = name;
		do {
			int result = fnmatch(atom, test, FNM_PATHNAME);
			debug(D_DEBUG, "fnmatch(`%s', `%s', FNM_PATHNAME) = %d", atom, test, result);
			if(result == 0) {
				return 1;
			}
			test = strchr(test, '/');
			if (test) test += 1;
		} while (test);

		if (end)
			pattern = end+1;
		else
			break;
	}

	return 0;
}

/* NOTICE: this function's logic should be kept in sync with function of same
 * name in chirp_fs_local.c. */
static int search_should_recurse(const char *base, const char *pattern)
{
	debug(D_DEBUG, "search_should_recurse(base = `%s', pattern = `%s')", base, pattern);
	/* Decompose the pattern in atoms which are each matched against. */
	while (1) {
		char atom[PFS_PATH_MAX];

		if (*pattern != '/') return 1; /* unanchored pattern is always recursive */

		const char *end = strchr(pattern, '|'); /* disjunction operator */
		memset(atom, 0, sizeof(atom));
		if (end)
			strncpy(atom, pattern, (size_t)(end-pattern));
		else
			strcpy(atom, pattern);

		/* Here we want to determine if `base' matches earlier parts of
		 * `pattern' to see if we should recurse in the directory `base'. To do
		 * this, we strip off final parts of `pattern' until we get a match.
		 */
		while (*atom) {
			int result = fnmatch(atom, base, FNM_PATHNAME);
			debug(D_DEBUG, "fnmatch(`%s', `%s', FNM_PATHNAME) = %d", atom, base, result);
			if(result == 0) {
				return 1;
			}
			char *last = strrchr(atom, '/');
			if (last) {
				*last = '\0';
			} else {
				break;
			}
		}

		if (end)
			pattern = end+1;
		else
			break;
	}
	return 0;
}

/* NOTICE: this function's logic should be kept in sync with function of same
 * name in chirp_fs_local.c. */
static int search_directory(pfs_table *t, const char * const base, char *fullpath, const char *pattern, int flags, char *buffer, size_t len, size_t *i)
{
	if(strlen(pattern) == 0)
		return 0;

	debug(D_DEBUG, "search_directory(base = `%s', fullpath = `%s', pattern = `%s', flags = %d, ...)", base, fullpath, pattern, flags);

	int metadata = flags & PFS_SEARCH_METADATA;
	int stopatfirst = flags & PFS_SEARCH_STOPATFIRST;
	int includeroot = flags & PFS_SEARCH_INCLUDEROOT;

	int result = 0;
	int fd = t->open(fullpath, O_DIRECTORY|O_RDONLY, 0, 0, NULL, 0);
	char *current = fullpath + strlen(fullpath);	/* point to end to current directory */

	if(fd >= 0) {
		errno = 0;
		struct dirent *entry;
		while((entry = t->fdreaddir(fd))) {
			struct pfs_stat buf;
			int access_flags = search_to_access(flags);
			char *name = entry->d_name;

			if(strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
				continue;
			sprintf(current, "/%s", name);

			int stat_result = t->stat(fullpath, &buf);

			if(search_match_file(pattern, base)) {
				const char *matched = includeroot ? fullpath+1 : base; /* fullpath+1 because chirp_root_path is always "./" !! */

				result += 1;
				if(access_flags == F_OK || t->access(fullpath, access_flags) == 0) {

					if(metadata) {
						if(stat_result) {
							if (search_error(errno, PFS_SEARCH_ERR_STAT, fullpath, buffer, i, len) == -1)
								return -1;
						} else {
							size_t l = snprintf(buffer+*i, len-*i, "%s0|%s", *i==0 ? "" : "|", matched);
							if (l >= len-*i) {
								errno = ERANGE;
								return -1;
							}
							*i += l;
							if (search_stat_pack(&buf, buffer, i, len) == -1) {
								errno = ERANGE;
								return -1;
							}
							if(stopatfirst) return 1;
						}
					} else {
						size_t l = snprintf(buffer+*i, len-*i, "%s0|%s|", *i == 0 ? "" : "|", matched);
						if (l >= len-*i) {
							errno = ERANGE;
							return -1;
						}
						*i += l;
						if(stopatfirst) return 1;
					}
				} /* FIXME access failure */
			}

			if(stat_result == 0 && S_ISDIR(buf.st_mode) && search_should_recurse(base, pattern)) {
				int n = search_directory(t, base, fullpath, pattern, flags, buffer, len, i);
				if(n > 0) {
					result += n;
					if(stopatfirst)
						return result;
				}
			}
			*current = '\0';	/* clear current entry */
			errno = 0;
		}

		if (errno) {
			if (search_error(errno, PFS_SEARCH_ERR_READ, fullpath, buffer, i, len) == -1) {
				t->close(fd); /* can't report error anyway at this point */
				errno = ERANGE;
				return -1;
			}
		}

		if (t->close(fd) == -1) {
			if (search_error(errno, PFS_SEARCH_ERR_CLOSE, fullpath, buffer, i, len) == -1) {
				errno = ERANGE;
				return -1;
			}
		}
	} else {
		if (search_error(errno, PFS_SEARCH_ERR_OPEN, fullpath, buffer, i, len) == -1) {
			errno = ERANGE;
			return -1;
		}
	}

	return result;
}

static int is_pattern (const char *pattern)
{
	if (*pattern != '/')
		return 1; /* unrooted expressions are patterns */
	for (; *pattern; pattern += 1) {
		switch (*pattern) {
			case '\\':
#if 0
				/* we need to change the pattern to remove the backslashes
				 * so we can do exact matches, future work.
				 */
				pattern += 1;
				if (*pattern == '\0') {
					return 0;
				}
				break;
#endif
			case '*':
			case '?':
			case '[':
			case '|':
				return 1;
			case '"':
			case '\'':
			{
				/*
				const char quote = *pattern;
				quote = quote;
				*/
				/* quoting behavior isn't very clear... */
			}
			default:
				break;
		}
	}
	return 0;
}

int pfs_table::search( const char *paths, const char *patt, int flags, char *buffer, size_t buffer_length, size_t *i )
{
	pfs_name pname;
	const char *start = paths;
	const char *end;
	const char *pattern = patt;
	int found = 0;
	int result;

	debug(D_DEBUG, "%s(%s, %s, %d, %p, %zu, %p)", __FUNCTION__, paths, patt, flags, buffer, buffer_length, i);

	int done = 0;

	do {
		if (strlen(start)==0) break;

		char path[PFS_PATH_MAX+1];
		char directory[PFS_PATH_MAX+1];
		end = strchr(start, PFS_SEARCH_DELIMITER);

		if (end) {
			if (start == end) { /* "::" ? */
				strcpy(path, ".");
			} else {
				ptrdiff_t length = end-start; /* C++ doesn't let us properly cast these to void pointers for proper byte length */
				memset(path, 0, sizeof(path));
				strncpy(path, start, length);
			}
			start = end+1;
		} else {
			strcpy(path, start);
			done = 1;
		}

		path_collapse(path, directory, 0);

		debug(D_DEBUG, "searching directory `%s'", directory);

		if (!is_pattern(pattern)) {
			struct pfs_stat statbuf;
			int access_flags = search_to_access(flags);
			const char *base = directory + strlen(directory);

			debug(D_DEBUG, "pattern `%s' will be exactly matched", pattern);

			strcat(directory, pattern);

			result = this->stat(directory, &statbuf);
			if (result == 0) {
				const char *matched;
				if (flags & PFS_SEARCH_INCLUDEROOT)
					matched = directory;
				else
					matched = base;

				if (access_flags == F_OK || this->access(directory, access_flags) == 0) {
					size_t l = snprintf(buffer+*i, buffer_length-*i, "%s0|%s", *i==0 ? "" : "|", matched);

					if (l >= buffer_length-*i) {
						errno = ERANGE;
						return -1;
					}

					*i += l;

					if (flags & PFS_SEARCH_METADATA) {
						if (search_stat_pack(&statbuf, buffer, i, buffer_length) == -1) {
							errno = ERANGE;
							return -1;
						}
					} else {
						if ((size_t)snprintf(buffer+*i, buffer_length-*i, "|") >= buffer_length-*i) {
							errno = ERANGE;
							return -1;
						}
						(*i)++;
					}

					result = 1;
				}
			} else {
				result = 0;
			}
		} else {
			/* Check to see if search is implemented in the service */
			if(resolve_name(0,path, &pname, X_OK)) {
				debug(D_DEBUG, "attempting service `%s' search routine for path `%s'", pname.service_name, pname.path);
				if ((result = pname.service->search(&pname, pattern, flags, buffer, buffer_length, i))==-1 && errno == ENOSYS) {
					debug(D_DEBUG, "no service to search found: falling back to manual search `%s'", directory);
					result = search_directory(this, directory+strlen(directory), directory, pattern, flags, buffer, buffer_length, i);
				}
				debug(D_DEBUG, "= %d (`%s' search)", result, pname.service_name);
			} else
				result = -1;
		}

		if (result == -1)
			return -errno;
		else if (flags & PFS_SEARCH_STOPATFIRST && result == 1) {
			return result;
		} else
			found += result;
	} while (!done);

	return found;
}

int pfs_table::getacl( const char *n, char *buf, int length )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(0,n,&pname,R_OK)) {
		result = pname.service->getacl(&pname,buf,length);
	}

	return result;
}

int pfs_table::setacl( const char *n, const char *subject, const char *rights )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(0,n,&pname,W_OK)) {
		result = pname.service->setacl(&pname,subject,rights);
	}

	return result;
}

int pfs_table::locate( const char *n, char *buf, int length )
{
	static pfs_location *loc = 0;
	pfs_name pname;

	debug(D_SYSCALL, "locating \"%s\"", n);

	if(n && strlen(n)) {
		if(loc) delete(loc);
		loc = 0;

		if(resolve_name(0, n, &pname, X_OK)) {
			loc = pname.service->locate(&pname);
		}
	}

	if(loc) {
		int result = 0;
		char path[PFS_PATH_MAX];
		result = loc->retrieve(path, PFS_PATH_MAX);
		if(result) {
			memset(buf, 0, length);
			strncpy(buf, path, length);
			return result;
		}
	}

	return 0;

}


pfs_ssize_t pfs_table::copyfile( const char *source, const char *target )
{
	pfs_name psource, ptarget;
	pfs_file *sourcefile;
	pfs_file *targetfile;
	pfs_stat info;
	pfs_ssize_t result;

	if(!pfs_enable_small_file_optimizations) {
		errno = ENOSYS;
		return -1;
	}

	if(resolve_name(1,source,&psource,R_OK)<0) return -1;
	if(resolve_name(1,target,&ptarget,W_OK|E_OK)<0) return -1;

	if(psource.service == ptarget.service) {
		result = ptarget.service->thirdput(&psource,&ptarget);
	} else if(psource.service->is_local()) {
		result = ptarget.service->putfile(&psource,&ptarget);
	} else if(ptarget.service->is_local()) {
		result = psource.service->getfile(&psource,&ptarget);
	} else {
		result = -1;
	}

	if(result<0) {
		if(errno==ENOSYS || psource.service==ptarget.service) {
			sourcefile = open_object(source,O_RDONLY,0,0);
			if(!sourcefile) return -1;

			result = sourcefile->fstat(&info);
			if(result<0) {
				sourcefile->close();
				delete sourcefile;
				return -1;
			}

			if(S_ISDIR(info.st_mode)) {
				sourcefile->close();
				delete sourcefile;
				errno = EISDIR;
				return -1;
			}

			targetfile = open_object(target,O_WRONLY|O_CREAT|O_TRUNC,0777,0);
			if(!targetfile) {
				sourcefile->close();
				delete sourcefile;
				return -1;
			}

			result = copyfile_slow(sourcefile,targetfile);

			sourcefile->close();
			delete sourcefile;

			targetfile->close();
			delete targetfile;
		}
	}

	return result;
}

pfs_ssize_t pfs_table::fcopyfile(int sourcefd, int targetfd) {
	CHECK_FD(sourcefd);
	CHECK_FD(targetfd);

	if (copyfile_slow(pointers[sourcefd]->file, pointers[targetfd]->file) > -1) {
		return 0;
	} else {
		errno = ENOTTY;
		return -1;
	}
}

pfs_ssize_t pfs_table::copyfile_slow( pfs_file *sourcefile, pfs_file *targetfile )
{
	pfs_ssize_t total, ractual, wactual;
	void *buffer;
	int buffer_size;

	buffer_size = MAX(sourcefile->get_block_size(),targetfile->get_block_size());
	buffer = malloc(buffer_size);

	total = 0;

	while(1) {
		ractual = sourcefile->read(buffer,buffer_size,total);
		if(ractual<=0) break;

		wactual = targetfile->write(buffer,ractual,total);
		if(wactual!=ractual) break;

		total += ractual;
	}

	free(buffer);

	if(ractual==0) {
		return total;
	} else {
		return -1;
	}
}

int pfs_table::md5( const char *path, unsigned char *digest )
{
	pfs_name pname;
	int result;

	if(!pfs_enable_small_file_optimizations) {
		errno = ENOSYS;
		return -1;
	}

	if(resolve_name(1,path,&pname,R_OK)<0) return -1;

	result = pname.service->md5(&pname,digest);

	if(result<0 && errno==ENOSYS) {
		result = md5_slow(path,digest);
	}

	return result;
}

int pfs_table::md5_slow( const char *path, unsigned char *digest )
{
	md5_context_t context;
	pfs_file *file;
	unsigned char *buffer;
	int buffer_size;
	pfs_off_t total=0;
	int result;

	file = open_object(path,O_RDONLY,0,0);
	if(!file) return -1;

	buffer_size = file->get_block_size();
	buffer = (unsigned char *)malloc(buffer_size);

	md5_init(&context);

	while(1) {
		result = file->read(buffer,buffer_size,total);
		if(result<=0) break;

		md5_update(&context,buffer,result);
		total += result;
	}


	file->close();
	delete file;

	free(buffer);

	if(result==0) {
		md5_final(digest,&context);
		return 0;
	} else {
		return -1;
	}
}

void pfs_table::mmap_proc (pid_t pid, buffer_t *B)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "/proc/%d/maps", (int)pid);
	FILE *maps = fopen(path, "r");

	struct pfs_process *p = pfs_process_lookup(pid);
	if (p) {
		for(struct pfs_mmap *m = p->table->mmap_list; m; m = m->next) {
			buffer_putfstring(B, "%016" PRIx64 "-%016" PRIx64, (uint64_t)(uintptr_t)m->logical_addr, (uint64_t)(uintptr_t)m->logical_addr+(uint64_t)m->map_length);
			buffer_putfstring(B, " ");
			buffer_putfstring(B, "%c", m->prot & PROT_READ ? 'r' : '-');
			buffer_putfstring(B, "%c", m->prot & PROT_WRITE ? 'w' : '-');
			buffer_putfstring(B, "%c", m->prot & PROT_EXEC ? 'w' : '-');
			buffer_putfstring(B, "%c", m->flags & MAP_PRIVATE ? 'p' : '-');
			buffer_putfstring(B, " ");
			buffer_putfstring(B, "%16" PRIx64, m->file_offset);
			buffer_putfstring(B, " ");
			buffer_putfstring(B, "%02" PRIx32 ":%02" PRIx32, major(m->finfo.st_dev), minor(m->finfo.st_dev));
			buffer_putfstring(B, " ");
			buffer_putfstring(B, "%8" PRIu64, m->finfo.st_ino);
			buffer_putfstring(B, " ");
			buffer_putfstring(B, "%s", m->fpath);
			buffer_putfstring(B, "\n");
		}
	}

	if (maps) {
		char line[4096];
		while (fgets(line, sizeof(line), maps)) {
			/* we reformat some entries for consistency */
			char *start = NULL, *end = NULL, *perm = NULL, *off = NULL, *dev = NULL, *ino = NULL, *path = NULL;
			if (pattern_match(line, "^(%x+)%-(%x+)%s+(%S+)%s+(%x+)%s+([%d:]+)%s+(%d+)%s+(.-)%s*$", &start, &end, &perm, &off, &dev, &ino, &path) >= 0) {
				size_t current = buffer_pos(B);
				buffer_putfstring(B, "%016" PRIx64 "-%016" PRIx64, (uint64_t)strtoul(start, NULL, 16), (uint64_t)strtoul(end, NULL, 16));
				buffer_putfstring(B, " %s", perm);
				buffer_putfstring(B, " %16" PRIx64, (uint64_t)strtoul(off, NULL, 16));
				buffer_putfstring(B, " %s", dev);
				buffer_putfstring(B, " %8" PRIu64, (uint64_t)strtoul(ino, NULL, 16));
				buffer_putfstring(B, " %s", path);
				buffer_putliteral(B, "\n");
				if (pattern_match(path, "%[%w+%]%s*$") >= 0) {
					/* OKAY: heap/stack/etc. */
				} else if (pattern_match(dev, "0+:0+") >= 0) {
					/* OKAY: anonymous mapping */
				} else if (pattern_match(path, ".-parrot%-channel") < 0) {
					/* OKAY: ! parrot mapping */
				} else {
					/* not printed */
					buffer_rewind(B, current);
				}
			}
			free(start); free(end); free(perm); free(off); free(dev); free(ino); free(path);
		}
		fclose(maps);
	}
}

void pfs_table::mmap_print()
{
	struct pfs_mmap *m;

	debug(D_CHANNEL,"%12s %8s %8s %8s %4s %4s %s","address","length","foffset", "channel", "prot", "flag", "file");

	for(m=mmap_list;m;m=m->next) {
		debug(D_CHANNEL,"%12llx %8llx %8llx %8llx %4x %4x %s",(long long)m->logical_addr,(long long)m->map_length,(long long)m->file_offset,(long long)m->channel_offset,m->prot,m->flags,m->file->get_name()->path);
	}
}

static int load_file_to_channel( pfs_file *file, pfs_size_t length, pfs_size_t start, pfs_size_t blocksize )
{
	pfs_size_t data_left = length;
	pfs_size_t offset = 0;
	pfs_size_t chunk, actual;

	while(data_left>0) {
		chunk = MIN(data_left,blocksize);
		actual = file->read(pfs_channel_base()+start+offset,chunk,offset);
		if(actual>0) {
			offset += actual;
			data_left -= actual;
		} else if(actual==0) {
			memset(pfs_channel_base()+start+offset,0,data_left);
			offset += data_left;
			data_left = 0;
		} else {
			break;
		}
	}

	if(data_left) {
		debug(D_CHANNEL,"loading: failed: %s",strerror(errno));
		return 0;
	} else {
		/*
		we must invalidate the others' mapping of this file,
		otherwise, they will see old data that was in this place.
		*/
		msync(pfs_channel_base()+start,length,MS_INVALIDATE|MS_ASYNC);
		return 1;
	}
}

static int save_file_from_channel( pfs_file *file, pfs_size_t file_offset, pfs_size_t channel_offset, pfs_size_t map_length, pfs_size_t blocksize )
{
	pfs_size_t data_left = map_length;
	pfs_size_t chunk, actual;

	while(data_left>0) {
		chunk = MIN(data_left,blocksize);
		actual = file->write(pfs_channel_base()+channel_offset+file_offset,chunk,file_offset);
		if(actual>0) {
			file_offset += actual;
			data_left -= actual;
		} else {
			break;
		}
	}

	if(data_left) {
		debug(D_CHANNEL,"writing: failed: %s",strerror(errno));
		return 0;
	}

	return 1;
}

pfs_size_t pfs_table::mmap_create_object( pfs_file *file, pfs_size_t channel_offset, pfs_size_t map_length, pfs_size_t file_offset, int prot, int flags )
{
	pfs_mmap *m;

	m = new pfs_mmap(file, 0, channel_offset, map_length, file_offset, prot, flags);

	m->next = mmap_list;
	mmap_list = m;

	return channel_offset;
}

pfs_size_t pfs_table::mmap_create( int fd, pfs_size_t file_offset, size_t map_length, int prot, int flags )
{
	pfs_file *file;
	pfs_size_t channel_offset;
	pfs_ssize_t file_length;

	CHECK_FD(fd);

	if(!(pointers[fd]->flags&(O_WRONLY|O_RDWR|O_APPEND)) && prot&PROT_WRITE && flags&MAP_SHARED)
		return (errno = EACCES, -1);

	file = pointers[fd]->file;
	file_length = file->get_size();

	if(file_length<0)
		return (errno = ENODEV, -1);

	/* FIXME we don't check the range because it's valid to mmap a file plus extra. However, we don't allocate space in the channel for this! */
	//else if(!(file_offset < file_length)) /* beginning of range [off, off+len) */
	//	return (errno = ENXIO, -1);
	//else if(!((file_offset+map_length) <= file_length)) /* end of range [off, off+len) */
	//	return (errno = ENXIO, -1);

	if(!pfs_channel_lookup(file->get_name()->path,&channel_offset)) {
		if(!pfs_channel_alloc(file->get_name()->path,file_length,&channel_offset))
			return (errno = ENOMEM, -1);

		debug(D_CHANNEL,"%s loading to channel %llx size %llx",file->get_name()->path,(long long)channel_offset,(long long)file_length);

		if(!load_file_to_channel(file,file_length,channel_offset,1024*1024)) {
			pfs_channel_free(channel_offset);
			return -1;
		}

		channel_offset = mmap_create_object(file, channel_offset, map_length, file_offset, prot, flags);

		/* pfs_channel_alloc adds a ref and so does mmap_create_object, remove the extra: */
		pfs_channel_free(channel_offset);
		return channel_offset;
	} else {
		debug(D_CHANNEL,"%s cached at channel %llx",file->get_name()->path,(long long)channel_offset);
		return mmap_create_object(file, channel_offset, map_length, file_offset, prot, flags);
	}
}

int pfs_table::mmap_update( uintptr_t logical_addr, size_t channel_offset )
{
	if(mmap_list && !mmap_list->logical_addr) {
		mmap_list->logical_addr = logical_addr;
		return 0;
	}

	debug(D_NOTICE,"warning: mmap logical address (%llx) does not match any map with channel offset (%llx)",(long long)logical_addr,(long long)channel_offset);

	errno = ENOENT;
	return -1;
}

int pfs_table::mmap_delete( uintptr_t logical_addr, size_t length )
{
	long pgsize = sysconf(_SC_PAGESIZE);
	uintptr_t s = logical_addr & ~(pgsize-1); /* first page; 0 out lower bits */
	uintptr_t e = (logical_addr+length+pgsize-1) & ~(pgsize-1); /* first page NOT IN MAPPING; 0 out lower bits */

	debug(D_DEBUG, "munmap(%016" PRIxPTR ", %" PRIxPTR ") --> unmap [%016" PRIxPTR ", %016" PRIxPTR ")", logical_addr, length, s, e);

	for(pfs_mmap *m = mmap_list, **p = &mmap_list; m; p=&m->next, m=m->next) {
		if( s >= m->logical_addr && ( s < (m->logical_addr+m->map_length ) ) ) {
			*p = m->next; // Remove the map from the list.

			// Write back the portion of the file that is mapped in.
			if(m->flags&MAP_SHARED && m->prot&PROT_WRITE && m->file) {
				save_file_from_channel(m->file,m->file_offset,m->channel_offset,m->map_length,1024*1024);
			}

			/* If we are deleting a mapping that has no logical address, then mmap failed. Don't attempt to split the mapping. */
			if (!(s == 0 && length == 0)) {
				// If there is a fragment left over before the unmap, add it as a new map
				// This will increase the reference count of both the file and the memory object.

				if(m->logical_addr < s) {
					pfs_mmap *newmap = new pfs_mmap(m);
					newmap->map_length = s - m->logical_addr;
					newmap->next = *p;
					*p = newmap;
					debug(D_DEBUG, "split off memory fragment [%016" PRIxPTR ", %016" PRIxPTR ") size = %zu", newmap->logical_addr, newmap->logical_addr+newmap->map_length, newmap->map_length);
				}

				// If there is a fragment left over after the unmap, add it as a new map
				// This will increase the reference count of both the file and the memory object.

				if(e < (m->logical_addr+m->map_length)) {
					pfs_mmap *newmap = new pfs_mmap(m);
					newmap->logical_addr = e;
					newmap->map_length -= e - m->logical_addr;
					newmap->file_offset += e - m->logical_addr;
					newmap->next = *p;
					*p = newmap;
					debug(D_DEBUG, "split off memory fragment [%016" PRIxPTR ", %016" PRIxPTR ") size = %zu", newmap->logical_addr, newmap->logical_addr+newmap->map_length, newmap->map_length);
				}
			}

			delete m; // Delete the mapping, which may also delete the file object and free the channel.

			return 0;
		}
	}

	/*
	It is quite common that an munmap will not match any existing mapping.
	This happens particularly for anonymous mmaps, which are not recorded here.
	In this case, simply return succcess;
	*/

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
