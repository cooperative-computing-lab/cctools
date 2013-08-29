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

extern "C" {
#include "debug.h"
#include "stringtools.h"
#include "macros.h"
#include "full_io.h"
#include "get_canonical_path.h"
#include "pfs_resolve.h"
#include "pfs_channel.h"
#include "md5.h"
}

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <ctype.h>
#include <math.h>
#include <signal.h>
#include <malloc.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/poll.h>

extern int pfs_force_stream;
extern int pfs_force_sync;
extern int pfs_follow_symlinks;
extern int pfs_enable_small_file_optimizations;

extern const char * pfs_initial_working_directory;

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

	pfs_mmap *m;

	while(mmap_list) {
		m = mmap_list;
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
			this->pointers[i]->addref();
			this->pointers[i]->file->addref();
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

void pfs_table::close_on_exec()
{
	int i;

	for(i=0;i<pointer_count;i++) {
		if(this->pointers[i]) {
			if((this->fd_flags[i])&FD_CLOEXEC) {
				this->close(i);
			}
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

void pfs_table::attach( int logical, int physical, int flags, mode_t mode, const char *name )
{
	pointers[logical] = new pfs_pointer(pfs_file_bootstrap(physical,name),flags,mode);
	fd_flags[logical] = 0;
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

/* Remove multiple slashes and /. from a path */

void pfs_table::collapse_path( const char *l, char *s, int remove_dotdot )
{
	string_collapse_path(l,s,remove_dotdot);
}

/*
If short_path is an absolute path, copy it to full path.
Otherwise, tack the current directory on to the front
of short_path, and copy it to full_path.
*/
 
void pfs_table::complete_path( const char *short_path, char *full_path )
{
        if( short_path[0]=='/' ) {
                strcpy(full_path,short_path);
        } else {
                strcpy(full_path,working_dir);
                strcat(full_path,"/");
                strcat(full_path,short_path);
        }
}

/*
Complete a path, starting with this fd assumed to be a directory.
*/

void pfs_table::complete_at_path( int dirfd, const char *path, char *full_path )
{
	if(path) {
		if(path[0]=='/') {
			strcpy(full_path,path);
		} else {
#ifdef AT_FDCWD
			if(dirfd==AT_FDCWD) {
				sprintf(full_path,"%s/%s",working_dir,path);
			} else
#endif
			{
				get_full_name(dirfd,full_path);
				strcat(full_path,"/");
				strcat(full_path,path);
			}
		}
	} else {
		/* some *at syscalls (see utimensat) allow path to be NULL, fill full_path with path of dirfd */
#ifdef AT_FDCWD
		if(dirfd==AT_FDCWD) {
			strcpy(full_path,working_dir);
		} else
#endif
		{
			get_full_name(dirfd,full_path);
		}
	}
}

void pfs_table::follow_symlink( struct pfs_name *pname, int depth )
{
	char link_target[PFS_PATH_MAX];
	char absolute_link_target[PFS_PATH_MAX];
	char *name_to_resolve = link_target;
	struct pfs_name new_pname = *pname;

	int rlres = new_pname.service->readlink(pname,link_target,PFS_PATH_MAX-1);
	if (rlres > 0) {
		/* readlink does not NULL-terminate */
		link_target[rlres] = '\000';
		/* Is link target relative ? */
		if (link_target[0] != '/') {
			 const char *basename_start = string_basename(pname->path);
			 if (basename_start) {
				int dirname_len = basename_start - pname->path;
				snprintf(absolute_link_target,
					PFS_PATH_MAX, "%*.*s%s",
					dirname_len, dirname_len, pname->path, 
					link_target);
				name_to_resolve = absolute_link_target;
			}
		}
		if (resolve_name(name_to_resolve, &new_pname, true, depth + 1)) {
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

int pfs_table::resolve_name( const char *cname, struct pfs_name *pname, bool do_follow_symlink, int depth ) {
	char full_logical_name[PFS_PATH_MAX];
	char tmp[PFS_PATH_MAX];
	pfs_resolve_t result;

	if (depth > PFS_MAX_RESOLVE_DEPTH) {
	    return ELOOP;
	}

	complete_path(cname,full_logical_name);

	if(!strncmp(full_logical_name,"/proc/self",10)) {
		sprintf(tmp,"/proc/%d%s",pfs_process_getpid(),&full_logical_name[10]);
		strcpy(full_logical_name,tmp);
	}

	collapse_path(full_logical_name,pname->logical_name,1);
	result = pfs_resolve(pname->logical_name,pname->path,time(0)+pfs_master_timeout);

	if(result==PFS_RESOLVE_DENIED) {
		errno = EACCES;
		return 0;
	} else if(result==PFS_RESOLVE_ENOENT) {
		errno = ENOENT;
		return 0;
	} else if(result==PFS_RESOLVE_FAILED) {
		fatal("unable to resolve file %s",pname->logical_name);
		return 0;
	} else {
		string_split_path(pname->path,pname->service_name,tmp);
		pname->service = pfs_service_lookup(pname->service_name);
		if(!pname->service) {
			pname->service = pfs_service_lookup_default();
			strcpy(pname->service_name,"local");
			strcpy(pname->host,"localhost");
			strcpy(pname->hostport,"localhost");
			strcpy(pname->rest,pname->path);
			pname->is_local = 1;
		} else {
			if(!strcmp(pname->service_name,"multi")) {// if we're dealing with a multivolume, split off at the @
				string_split_multipath(tmp,pname->host,pname->rest);
			} else {
				string_split_path(tmp,pname->host,pname->rest);
			}
			
			if(!pname->host[0]) {
				pname->hostport[0] = 0;
				pname->rest[0] = 0;
				return 1;
			}
			char *c = strrchr(pname->host,':');
			if(c) {
				*c = 0;
				pname->port = atoi(c+1);
			} else {
				pname->port = pname->service->get_default_port();
			}
			sprintf(pname->hostport,"%s:%d",pname->host,pname->port);
		
			if(!strcmp(pname->service_name,"multi")) {
				strcpy(tmp,pname->rest);
				string_split_path(tmp,&pname->hostport[strlen(pname->hostport)],pname->rest); // reconstruct hostport as host:port@volume; path goes in rest.
			}
			if(pname->service->tilde_is_special() && !strncmp(pname->rest,"/~",2)) {
				memmove(pname->rest,&pname->rest[1],strlen(pname->rest));
			}
			pname->is_local = 0;
		}

		/* Enable cross service symlink resolution */
		if (do_follow_symlink && pfs_follow_symlinks) {
		    follow_symlink(pname, depth + 1);
		}
		return 1;
	}
}

pfs_file * pfs_table::open_object( const char *lname, int flags, mode_t mode, int force_cache )
{
	pfs_name pname;
	pfs_file *file=0;
	int force_stream = pfs_force_stream;

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

	const char *basename = string_basename(lname);
	if(!strcmp(basename,".") || !strcmp(basename,"..")) {
		flags |= O_DIRECTORY;
	}

	if(resolve_name(lname,&pname)) {
		if(flags&O_DIRECTORY) {
			file = pname.service->getdir(&pname);
		} else if(pname.service->is_local()) {
			file = pname.service->open(&pname,flags,mode);
		} else if(pname.service->is_seekable()) {
			if(force_cache) {
				file = pfs_cache_open(&pname,flags,mode);
			} else {
				file = pname.service->open(&pname,flags,mode);
			}
		} else {
			if(force_stream) {
				file = pname.service->open(&pname,flags,mode);
			} else {
				file = pfs_cache_open(&pname,flags,mode);
			}
		}
	} else {
		file = 0;
	}

	return file;
}

int pfs_table::open( const char *lname, int flags, mode_t mode, int force_cache )
{
	int result = -1;
	pfs_file *file=0;

	if(!strcmp(lname,"/dev/tty")) {
		if(pfs_current->tty[0]) {
			lname = pfs_current->tty;
		} else {
			errno = ENXIO;
			return -1;
		}
	}

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
			pointers[result] = new pfs_pointer(file,flags,mode);
			if(flags&O_APPEND) this->lseek(result,0,SEEK_END);
		} else {
			result = -1;
		}
	} else {
		result = -1;
		errno = EMFILE;
	}

	if(result>=0 && !pfs_current->tty[0] && (!(flags&O_NOCTTY)) && isatty(file->get_real_fd())) {
		strcpy(pfs_current->tty,lname);
	}

	if(result>=0) fd_flags[result] = 0;

	return result;
}

int pfs_table::pipe( int *fds )
{
	int result=-1;
	int rfds[2];

	result = ::pipe(rfds);
	if(result>=0) {
		fds[0] = find_empty(0);
		fds[1] = find_empty(fds[0]+1);

		::fcntl(rfds[0],F_SETFL,O_NONBLOCK);
		::fcntl(rfds[1],F_SETFL,O_NONBLOCK);

		pointers[fds[0]] = new pfs_pointer(pfs_file_bootstrap(rfds[0],"rpipe"),O_RDONLY,0777);
		pointers[fds[1]] = new pfs_pointer(pfs_file_bootstrap(rfds[1],"wpipe"),O_WRONLY,0777);

		fd_flags[fds[0]] = 0;
		fd_flags[fds[1]] = 0;
	}

	return result;
}

int pfs_table::get_real_fd( int fd )
{
	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		return -1;
	}

	return pointers[fd]->file->get_real_fd();
}

int pfs_table::get_full_name( int fd, char *name )
{
	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		return -1;
	}

	strcpy(name,pointers[fd]->file->get_name()->path);
	return 0;
}

int pfs_table::get_local_name( int fd, char *name )
{
	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		return -1;
	}

	return pointers[fd]->file->get_local_name(name);
}

/*
Select is actually quite simple.  We register all the
files in the set with the master poller, and then run
a non blocking check.  If any report back, then mark the
output sets and return.  Otherwise, return -EAGAIN so
that we are put to sleep.
*/

int pfs_table::select( int n, fd_set *r, fd_set *w, fd_set *e, struct timeval *timeout )
{
	fd_set out_r, out_w, out_e;
	int i, result = 0;

	FD_ZERO(&out_r);
	FD_ZERO(&out_w);
	FD_ZERO(&out_e);

	if(n>pointer_count) n = pointer_count;

	for(i=0;i<n;i++) {
		if(!pointers[i]) continue;

		int wantflags = 0;
		if(r && FD_ISSET(i,r)) wantflags|=PFS_POLL_READ;
		if(w && FD_ISSET(i,w)) wantflags|=PFS_POLL_WRITE;
		if(e && FD_ISSET(i,e)) wantflags|=PFS_POLL_EXCEPT;
		if(!wantflags) continue;
		debug(D_POLL,"fd %d want  %s",i,pfs_poll_string(wantflags));

		int flags = pointers[i]->file->poll_ready();
		pfs_file *f = pointers[i]->file;
		debug(D_POLL,"fd %d ready %s %s",i,pfs_poll_string(flags),f->get_name()->path);

		if(wantflags&PFS_POLL_READ && flags&PFS_POLL_READ) {
			FD_SET(i,&out_r);
			result++;
		}
		if(wantflags&PFS_POLL_WRITE && flags&PFS_POLL_WRITE) {
			FD_SET(i,&out_w);
			result++;
		}
		if(wantflags&PFS_POLL_EXCEPT && flags&PFS_POLL_EXCEPT) {
			FD_SET(i,&out_e);
			result++;
		}
	}


	if(result>0) {
		if(r) FD_ZERO(r);
		if(w) FD_ZERO(w);
		if(e) FD_ZERO(e);
		for(i=0;i<n;i++) {
			if(r && FD_ISSET(i,&out_r)) FD_SET(i,r);
			if(w && FD_ISSET(i,&out_w)) FD_SET(i,w);
			if(e && FD_ISSET(i,&out_e)) FD_SET(i,e);
		}
		pfs_current->seltime.tv_sec=0;
	} else {
		if(timeout) {
			struct timeval curtime, stoptime, timeleft;

			gettimeofday(&curtime,0);
			if(pfs_current->seltime.tv_sec==0) {
				pfs_current->seltime = curtime;
			}
			timeradd(&pfs_current->seltime,timeout,&stoptime);
			if(
				(curtime.tv_sec>stoptime.tv_sec) ||
				( (curtime.tv_sec==stoptime.tv_sec) && (curtime.tv_usec>=stoptime.tv_usec) )
			) {
				result = 0;
				pfs_current->seltime.tv_sec=0;
				debug(D_POLL,"select time expired");
			} else {
				timersub(&stoptime,&curtime,&timeleft);
				debug(D_POLL,"select time remaining %d.%06d",(int)timeleft.tv_sec,(int)timeleft.tv_usec);
				pfs_poll_wakein(timeleft);
				result = -1;
				errno = EAGAIN;
			}
		} else {
			result = -1;
			errno = EAGAIN;
		}

		/*
		If result is zero, then we timed out. Clear all the output bits and return.
		Clearing is not strictly mandated by the standard, but many programs seem
		to depend on it.

		If result is not zero, then we need to register all of the fds of interest
		with the master pfs_poll mechanism, and then return EAGAIN, which will put
		this process to sleep.  When it wakes up, it will call pfs_table::select
		again and start over.
		*/

		if(result==0) {
			if(r) FD_ZERO(r);
			if(w) FD_ZERO(w);
			if(e) FD_ZERO(e);
		} else {
			for(i=0;i<n;i++) {
				if(!pointers[i]) continue;
				int flags=0;
				if(r && FD_ISSET(i,r)) flags|=PFS_POLL_READ;
				if(w && FD_ISSET(i,w)) flags|=PFS_POLL_WRITE;
				if(e && FD_ISSET(i,e)) flags|=PFS_POLL_EXCEPT;
				if(flags) pointers[i]->file->poll_register(flags);
			}
		}
	}

	return result;
}

/*
Careful with poll: if any of the file descriptors is invalid,
do not return failure right away, but mark the file descriptor
as invalid with POLLNVAL.
*/

int pfs_table::poll( struct pollfd *ufds, unsigned int nfds, int timeout )
{
	unsigned i;
	int result=0,maxfd=0;
	struct timeval tv;
	fd_set rfds, wfds, efds;

	FD_ZERO(&rfds);
	FD_ZERO(&wfds);
	FD_ZERO(&efds);

	for(i=0;i<nfds;i++) {
		int fd = ufds[i].fd;
		if(fd<0 || fd>=pointer_count || pointers[fd]==0) {
			continue;
			/* will fill in POLLNVAL later */
		} else {
			if(ufds[i].events&POLLIN)  FD_SET(fd,&rfds);
			if(ufds[i].events&POLLOUT) FD_SET(fd,&wfds);
			if(ufds[i].events&POLLERR) FD_SET(fd,&efds);
		}
		maxfd = MAX(maxfd,fd+1);
	}

	if(timeout>=0) {
		tv.tv_sec = timeout / 1000;
		tv.tv_usec = 1000*(timeout % 1000);
		result = this->select(maxfd,&rfds,&wfds,&efds,&tv);
	} else {
		result = this->select(maxfd,&rfds,&wfds,&efds,0);
	}

	if(result>0) {
		for(i=0;i<nfds;i++) {
			int fd = ufds[i].fd;
			ufds[i].revents = 0;
			if(fd<0 || fd>=pointer_count || pointers[fd]==0) {
				ufds[i].revents |= POLLNVAL;
				continue;
			}
			if(ufds[i].events&POLLIN&&FD_ISSET(fd,&rfds))
				ufds[i].revents |= POLLIN;
			if(ufds[i].events&POLLOUT&&FD_ISSET(fd,&wfds))
				ufds[i].revents |= POLLOUT;
			if(ufds[i].events&POLLERR&&FD_ISSET(fd,&efds))
				ufds[i].revents |= POLLERR;
		}
	}

	return result;
}

/*
Close is a little tricky.
The file pointer might be in use by several dups,
or the file itself might be in use by several opens.
*/

int pfs_table::close( int fd )
{
	int result = -1;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		result = -1;
		errno = EBADF;
	} else {
		pfs_pointer *p = pointers[fd];
		pfs_file *f = p->file;

		result = 0;

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
	}
	return result;
}

pfs_ssize_t pfs_table::read( int fd, void *data, pfs_size_t nbyte )
{
	pfs_ssize_t result = -1;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = this->pread(fd,data,nbyte,pointers[fd]->tell());
		if(result>0) pointers[fd]->bump(result);
	}

	return result;
}

pfs_ssize_t pfs_table::write( int fd, const void *data, pfs_size_t nbyte )
{
	pfs_ssize_t result = -1;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = this->pwrite(fd,data,nbyte,pointers[fd]->tell());
		if(result>0) pointers[fd]->bump(result);
	}

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

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else if( (!data) || (nbyte<0) ) {
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

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else if( (!data) || (nbyte<0) ) {
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

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		p = pointers[fd];
		f = p->file;
		if(!f->is_seekable()) {
			errno = ESPIPE;
			result = -1;
		} else {
			result = p->seek(offset,whence);
		}
	}

	return result;
}

int pfs_table::dup( int fd )
{
	return search_dup2( fd, 0 );
}

int pfs_table::search_dup2( int fd, int search )
{
	int i;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) || (search<0) || (search>=pointer_count) ) {
		errno = EBADF;
		return -1;
	}

	for( i=search; i<pointer_count; i++ )
		if(!pointers[i]) break;

	if( i==pointer_count ) {
		errno = EMFILE;
		return -1;
	} else {
		return dup2(fd,i);
	}
}

int pfs_table::dup2( int ofd, int nfd )
{
	int result=-1;

	if( (nfd<0) || (nfd>=pointer_count) ) {
		errno = EBADF;
		result = -1;
	} else if( (ofd<0) || (ofd>=pointer_count) || (pointers[ofd]==0) ) {
		errno = EBADF;
		result = -1;
	} else if( ofd==nfd ) {
		result = ofd;
	} else {

		// If this fd is already in use, close it.
		// But, close _can_ fail!  If that happens,
		// abort the dup with the errno from the close.

		if( pointers[nfd]!=0 ) {
			result = this->close(nfd);
		} else {
			result = 0;
		}

		if(result==0) {
			pointers[nfd] = pointers[ofd];
			pointers[nfd]->addref();
			pointers[nfd]->file->addref();
			fd_flags[nfd] = 0;
			result = nfd;
		}
	}

	return result;
}

int pfs_table::fchdir( int fd )
{
	int result = -1;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		pfs_name *pname = pointers[fd]->file->get_name();
		result = this->chdir(pname->path);
	}

	return result;
}

int pfs_table::ftruncate( int fd, pfs_off_t size )
{
	int result = -1;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		if( size<0 ) {
			result = 0;
		} else {
			result = pointers[fd]->file->ftruncate(size);
		}
	}

	return result;
}

int pfs_table::fstat( int fd, struct pfs_stat *b )
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		pfs_file *file = pointers[fd]->file;
		result = file->fstat(b);
		if(result>=0) {
			b->st_blksize = file->get_block_size();
		}
	}

	return result;
}


int pfs_table::fstatfs( int fd, struct pfs_statfs *buf )
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->fstatfs(buf);
	}


	return result;
}


int pfs_table::fsync( int fd )
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->fsync();
	}


	return result;
}

int pfs_table::flock( int fd, int op )
{
	int result = -1;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
       		result = pointers[fd]->file->flock(op);
	}


	return result;
}

int pfs_table::fcntl( int fd, int cmd, void *arg )
{
	int result;
	int flags;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else switch(cmd) {
		case F_GETFD:
			result = fd_flags[fd];
			break;
		case F_SETFD:
			fd_flags[fd] = (PTRINT_T)arg;
			result = 0;
			break;
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

		#ifdef F_DUPFD
		case F_DUPFD:
			result = this->search_dup2(fd,(PTRINT_T)arg);
			break;
		#endif

		#ifdef F_DUP2FD
		case F_DUP2FD:
			result = this->dup2(fd,(PTRINT_T)arg);
			break;
		#endif

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

int pfs_table::ioctl( int fd, int cmd, void *arg )
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->ioctl(cmd,arg);
	}


	return result;
}

int pfs_table::fchmod( int fd, mode_t mode )
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->fchmod(mode);
	}

	return result;
}

extern uid_t pfs_uid;
extern gid_t pfs_gid;

int pfs_table::fchown( int fd, uid_t uid, gid_t gid )
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->fchown(uid,gid);
	}

	/*
	If the service doesn't implement it, but its our own uid,
	then fake success, as tools like cp do this very often.
	*/

	if(result<0 && errno==ENOSYS && uid==pfs_uid && gid==pfs_gid) {
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

	if(resolve_name(path,&pname)) {
		result = pname.service->chdir(&pname,newpath);
		if(result>=0) {
			collapse_path(pname.logical_name,working_dir,1);
			result = 0;
		}
	}

	return result;
}

char *pfs_table::getcwd( char *path, pfs_size_t size )
{
	strncpy(path,working_dir,size);
	return path;
}

int pfs_table::access( const char *n, mode_t mode )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(n,&pname)) {
		result = pname.service->access(&pname,mode);
	}

	return result;
}

int pfs_table::chmod( const char *n, mode_t mode )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname)) {
		result = pname.service->chmod(&pname,mode);
	}

	return result;
}

int pfs_table::chown( const char *n, uid_t uid, gid_t gid )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname)) {
		result = pname.service->chown(&pname,uid,gid);
	}

	/*
	If the service doesn't implement it, but its our own uid,
	then fake success, as tools like cp do this very often.
	*/

	if(result<0 && errno==ENOSYS && uid==pfs_uid && gid==pfs_gid) {
		result = 0;
	}

	return result;
}

int pfs_table::lchown( const char *n, uid_t uid, gid_t gid )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname,false)) {
		result = pname.service->lchown(&pname,uid,gid);
	}

	return result;
}

int pfs_table::truncate( const char *n, pfs_off_t offset )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname)) {
		result = pname.service->truncate(&pname,offset);
	}

	return result;
}

ssize_t pfs_table::getxattr (const char *path, const char *name, void *value, size_t size)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(path,&pname)) {
		result = pname.service->getxattr(&pname,name,value,size);
	}

	return result;
}

ssize_t pfs_table::lgetxattr (const char *path, const char *name, void *value, size_t size)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(path,&pname,false)) {
		result = pname.service->lgetxattr(&pname,name,value,size);
	}

	return result;
}

ssize_t pfs_table::fgetxattr (int fd, const char *name, void *value, size_t size)
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->fgetxattr(name,value,size);
	}

	return result;
}

ssize_t pfs_table::listxattr (const char *path, char *list, size_t size)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(path,&pname)) {
		result = pname.service->listxattr(&pname,list,size);
	}

	return result;
}

ssize_t pfs_table::llistxattr (const char *path, char *list, size_t size)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(path,&pname,false)) {
		result = pname.service->llistxattr(&pname,list,size);
	}

	return result;
}

ssize_t pfs_table::flistxattr (int fd, char *list, size_t size)
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->flistxattr(list,size);
	}

	return result;
}

int pfs_table::setxattr (const char *path, const char *name, const void *value, size_t size, int flags)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(path,&pname)) {
		result = pname.service->setxattr(&pname,name,value,size,flags);
	}

	return result;
}

int pfs_table::lsetxattr (const char *path, const char *name, const void *value, size_t size, int flags)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(path,&pname,false)) {
		result = pname.service->lsetxattr(&pname,name,value,size,flags);
	}

	return result;
}

int pfs_table::fsetxattr (int fd, const char *name, const void *value, size_t size, int flags)
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->fsetxattr(name,value,size,flags);
	}

	return result;
}

int pfs_table::removexattr (const char *path, const char *name)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(path,&pname)) {
		result = pname.service->removexattr(&pname,name);
	}

	return result;
}

int pfs_table::lremovexattr (const char *path, const char *name)
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(path,&pname,false)) {
		result = pname.service->lremovexattr(&pname,name);
	}

	return result;
}

int pfs_table::fremovexattr (int fd, const char *name)
{
	int result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = -1;
	} else {
		result = pointers[fd]->file->fremovexattr(name);
	}

	return result;
}

int pfs_table::utime( const char *n, struct utimbuf *buf )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname)) {
		result = pname.service->utime(&pname,buf);
	}

	return result;
}

int pfs_table::utimens( const char *n, const struct timespec times[2] )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname)) {
		result = pname.service->utimens(&pname,times);
	}

	return result;
}

int pfs_table::lutimens( const char *n, const struct timespec times[2] )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname,false)) {
		result = pname.service->lutimens(&pname,times);
	}

	return result;
}


int pfs_table::unlink( const char *n )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(n,&pname,false)) {
		result = pname.service->unlink(&pname);
		if(result==0) pfs_cache_invalidate(&pname);
	}

	return result;
}

int pfs_table::stat( const char *n, struct pfs_stat *b )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(n,&pname)) {
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

	if(resolve_name(n,&pname)) {
		result = pname.service->statfs(&pname,b);
	}

	return result;
}

int pfs_table::lstat( const char *n, struct pfs_stat *b )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname,false)) {
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

	if(resolve_name(n1,&p1,false) && resolve_name(n2,&p2,false)) {
		if(p1.service==p2.service) {
			result = p1.service->rename(&p1,&p2);
			if(result==0) {
				pfs_cache_invalidate(&p1);
				pfs_cache_invalidate(&p2);
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

	if(resolve_name(n1,&p1,false) && resolve_name(n2,&p2,false)) {
		if(p1.service==p2.service) {
			result = p1.service->link(&p1,&p2);
		} else {
			errno = EXDEV;
		}
	}

	return result;
}

int pfs_table::symlink( const char *n1, const char *n2 )
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

	if(resolve_name(n2,&pname,false)) {
		result = pname.service->symlink(n1,&pname);
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

	if(resolve_name(n,&pname,false)) {
		int pid, fd;
		if(sscanf(pname.path,"/proc/%d/fd/%d",&pid,&fd)==2) {
			struct pfs_process *target = pfs_process_lookup(pid);
			if(target && target->table) {
				if(target->table->get_full_name(fd,buf)==0) {
					result = strlen(buf);
				} else {
					result = -1;
				}
			} else {
				errno = ENOENT;
				result = -1;
			}
		} else if(sscanf(pname.path,"/proc/%d/exe",&pid)==1) {
			struct pfs_process *target = pfs_process_lookup(pid);
			if(target) {
				strncpy(buf,target->name,size);
				result = MIN(size,(pfs_size_t)strlen(target->name));
			} else {
				result = pname.service->readlink(&pname,buf,size);
			}

		} else {
			result = pname.service->readlink(&pname,buf,size);
		}
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

	if(resolve_name(n,&pname)) {
		result = pname.service->mknod(&pname,mode,dev);
	}

	return result;
}

int pfs_table::mkdir( const char *n, mode_t mode )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname)) {
		result = pname.service->mkdir(&pname,mode);
	}

	return result;
}

int pfs_table::rmdir( const char *n )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname,false)) {
		result = pname.service->rmdir(&pname);
	}

	return result;
}

struct dirent * pfs_table::fdreaddir( int fd )
{
	struct dirent * result;

	if( (fd<0) || (fd>=pointer_count) || (pointers[fd]==0) ) {
		errno = EBADF;
		result = 0;
	} else {
		pfs_off_t next_offset;
		pfs_pointer *fp = pointers[fd];
		result = fp->file->fdreaddir(fp->tell(),&next_offset);
		if(result) fp->seek(next_offset,SEEK_SET);
	}


	return result;
}

int pfs_table::socket( int domain, int type, int protocol )
{
	int rfd;
	int result=-1;

	rfd = ::socket(domain,type,protocol);
	if(rfd>=0) {
		::fcntl(rfd,F_SETFL,O_NONBLOCK);
		result = find_empty(0);
		pointers[result] = new pfs_pointer(pfs_file_bootstrap(rfd,"socket"),O_RDWR,0777);
		fd_flags[result] = 0;
	} else {
		result = -1;
	}

	return result;
}

int pfs_table::socketpair( int domain, int type, int protocol, int *fds )
{
	int result=-1;
	int rfds[2];


	result = ::socketpair(domain,type,protocol,rfds);
	if(result>=0) {
		fds[0] = find_empty(0);
		fds[1] = find_empty(fds[0]+1);

		::fcntl(rfds[0],F_SETFL,O_NONBLOCK);
		::fcntl(rfds[1],F_SETFL,O_NONBLOCK);

		pointers[fds[0]] = new pfs_pointer(pfs_file_bootstrap(rfds[0],"socketpair"),O_RDWR,0777);
		pointers[fds[1]] = new pfs_pointer(pfs_file_bootstrap(rfds[1],"socketpair"),O_RDWR,0777);

		fd_flags[fds[0]] = 0;
		fd_flags[fds[1]] = 0;
	}

	return result;
}

int pfs_table::accept( int fd, struct sockaddr *addr, int *addrlen )
{
	int rfd;
	int result=-1;

	rfd = ::accept(get_real_fd(fd),addr,(socklen_t*)addrlen);
	if(rfd>=0) {
		result = find_empty(0);
		pointers[result] = new pfs_pointer(pfs_file_bootstrap(rfd,"socket"),O_RDWR,0777);
		::fcntl(rfd,F_SETFL,O_NONBLOCK);
		fd_flags[rfd] = 0;
	} else {
		result = -1;
	}

	return result;
}

int pfs_table::mkalloc( const char *n, pfs_ssize_t size, mode_t mode )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname)) {
		result = pname.service->mkalloc(&pname,size,mode);
	}

	return result;
}

int pfs_table::lsalloc( const char *n, char *a, pfs_ssize_t *total, pfs_ssize_t *avail )
{
	pfs_name pname;
	int result=-1;

	if(resolve_name(n,&pname)) {
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

	if(resolve_name(n,&pname)) {
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
	int fd = t->open(fullpath, O_DIRECTORY|O_RDONLY, 0, 0);
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

		string_collapse_path(path, directory, 0);

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
			if(resolve_name(path, &pname)) {
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

	if(resolve_name(n,&pname)) {
		result = pname.service->getacl(&pname,buf,length);
	}

	return result;
}

int pfs_table::setacl( const char *n, const char *subject, const char *rights )
{
	pfs_name pname;
	int result = -1;

	if(resolve_name(n,&pname)) {
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
		
		if(resolve_name(n, &pname)) {
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
	pfs_ssize_t result;

	if(!pfs_enable_small_file_optimizations) {
		errno = ENOSYS;
		return -1;
	}

	if(resolve_name(source,&psource)<0) return -1;
	if(resolve_name(target,&ptarget)<0) return -1;

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
			result = copyfile_slow(source,target);
		}
	}

	return result;
}

pfs_ssize_t pfs_table::copyfile_slow( const char *source, const char *target )
{
	pfs_file *sourcefile;
	pfs_file *targetfile;
	pfs_stat info;
	pfs_ssize_t total, ractual, wactual;
	void *buffer;
	int buffer_size;

	int result;

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

	sourcefile->close();
	delete sourcefile;

	targetfile->close();
	delete targetfile;

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

	if(resolve_name(path,&pname)<0) return -1;

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
		msync(pfs_channel_base()+start,length,MS_INVALIDATE|MS_SYNC);
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

pfs_size_t pfs_table::mmap_create_object( pfs_file *file, pfs_size_t file_offset, pfs_size_t map_length, int prot, int flags )
{
	pfs_size_t channel_offset;
	pfs_ssize_t file_length;

	file_length = file->get_size();
	if(file_length<0) return -1;

	if(!pfs_channel_lookup(file->get_name()->path,&channel_offset)) {

		if(!pfs_channel_alloc(file->get_name()->path,file_length,&channel_offset)) {
			errno = ENOMEM;
			return -1;
		}

		debug(D_CHANNEL,"%s loading to channel %llx size %llx",file->get_name()->path,(long long)channel_offset,(long long)file_length);

		if(!load_file_to_channel(file,file_length,channel_offset,1024*1024)) {
			pfs_channel_free(channel_offset);
			return -1;
		}
	} else {
	  debug(D_CHANNEL,"%s cached at channel %llx",file->get_name()->path,(long long)channel_offset);
	}

	pfs_mmap *m;

	m = new pfs_mmap( file, 0, channel_offset, map_length, file_offset, prot, flags );

	m->next = mmap_list;
	mmap_list = m;

	return channel_offset;
}

pfs_size_t pfs_table::mmap_create( int fd, pfs_size_t file_offset, pfs_size_t map_length, int prot, int flags )
{
	return mmap_create_object(pointers[fd]->file,file_offset,map_length,prot,flags);
}

int pfs_table::mmap_update( pfs_size_t logical_addr, pfs_size_t channel_offset )
{
	if(mmap_list && !mmap_list->logical_addr) {
		mmap_list->logical_addr = logical_addr;
		return 0;
	}

	debug(D_NOTICE,"warning: mmap logical address (%llx) does not match any map with channel offset (%llx)",(long long)logical_addr,(long long)channel_offset);

	errno = ENOENT;
	return -1;
}

int pfs_table::mmap_delete( pfs_size_t logical_addr, pfs_size_t length )
{
	pfs_mmap *m, **p;

	p = &mmap_list;

	for(m=mmap_list;m;p=&m->next,m=m->next) {
		if( logical_addr >= m->logical_addr && ( logical_addr < (m->logical_addr+m->map_length ) ) ) {

			// Remove the map from the list.
			*p = m->next;

			// Write back the portion of the file that is mapped in.
			if(m->flags&MAP_SHARED && m->prot&PROT_WRITE && m->file) {
				save_file_from_channel(m->file,m->file_offset,m->channel_offset,m->map_length,1024*1024);
			}

			// If there is a fragment left over before the unmap, add it as a new map
			// This will increase the reference count of both the file and the memory object.

			if(logical_addr>m->logical_addr) {
				mmap_create_object(
					m->file,
					m->file_offset,
					logical_addr-m->logical_addr,
					m->prot,
					m->flags);
				mmap_update(m->logical_addr,0);
			}

			// If there is a fragment left over after the unmap, add it as a new map
			// This will increase the reference count of both the file and the memory object.

			if((logical_addr+length) < (m->logical_addr+m->map_length)) {
				mmap_create_object(
					m->file,
					m->file_offset+m->map_length-(m->logical_addr-logical_addr),
					m->map_length - length - (logical_addr - m->logical_addr),
					m->prot,
					m->flags);
				mmap_update(logical_addr+length,0);
			}

			// Decrement (and possibly free) the file in the channel.
			pfs_channel_free(m->channel_offset);

			// Delete the mapping, which may also delete the file object.
			delete m;

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
