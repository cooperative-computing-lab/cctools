/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef CCTOOLS_CPU_I386

#include "int_sizes.h"
#include "pfs_process.h"

int pfs_dispatch64( struct pfs_process *p, INT64_T signum )
{
	return 0;
}

#else

#include "pfs_sysdeps64.h"
#include "pfs_channel.h"
#include "pfs_process.h"
#include "pfs_sys.h"
#include "pfs_poll.h"
#include "pfs_service.h"

extern "C" {
#include "tracer.h"
#include "stringtools.h"
#include "full_io.h"
#include "xxmalloc.h"
#include "int_sizes.h"
#include "macros.h"
#include "debug.h"
}

#include <unistd.h>

#include <fcntl.h>
#include <termios.h>

#include <linux/sockios.h>

#include <net/if.h>

#include <sys/file.h>
#include <sys/mman.h>
#include <sys/personality.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/utsname.h>
#include <sys/wait.h>

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

extern struct pfs_process *pfs_current;
extern char *pfs_temp_dir;
extern char *pfs_false_uname;
extern uid_t pfs_uid;
extern gid_t pfs_gid;

extern pid_t trace_this_pid;

extern INT64_T pfs_syscall_count;
extern INT64_T pfs_read_count;
extern INT64_T pfs_write_count;

extern int pfs_trap_after_fork;

extern char *pfs_ldso_path;
extern int *pfs_syscall_totals64;

extern void handle_specific_process( pid_t pid );

/*
Divert this incoming system call to a read or write on the I/O channel
*/

static void divert_to_channel( struct pfs_process *p, INT64_T syscall, const void *uaddr, INT64_T length, pfs_size_t channel_offset )
{
	INT64_T args[5];
	args[0] = pfs_channel_fd();
	args[1] = (INT64_T) uaddr;
	args[2] = length;
	args[3] = (INT64_T) (channel_offset);
	tracer_args_set(p->tracer,syscall,args,4);
	p->syscall_args_changed = 1;
	p->diverted_length = length;
}

/*
Divert this incoming system call to something harmless with the given result.
*/

static void divert_to_dummy( struct pfs_process *p, INT64_T result )
{
	p->syscall_dummy = 1;
	p->syscall_result = result;
	tracer_args_set(p->tracer,SYSCALL64_getpid,0,0);
}

static int errno_in_progress( int e )
{
	return (e==EAGAIN || e==EALREADY || e==EINPROGRESS);
}

/*
The large majority of calls, given below in decode_syscall
have a simple transformation using tracer_copy_{in,out}.
Read, write, and similar syscalls need better performance
and thus have more specialized implementations shown here.
*/

/*
SYSCALL64_read and friends are implemented by loading the data
into the channel, and then redirecting the system call
to read from the channel fd. SYSCALL64_readv is done exactly
the same as read, but only the first chunk is actually
read.  The caller must examine the result and then keep reading.
*/

#define POINTER( i ) ((void*)(PTRINT_T)(i))

static void decode_read( struct pfs_process *p, INT64_T entering, INT64_T syscall, INT64_T *args )
{
	INT64_T fd = args[0];
	void *uaddr = POINTER(args[1]);
	pfs_size_t length = args[2];
	pfs_off_t offset = args[3];
	char *local_addr;
	
	if(entering) {
		if(!pfs_channel_alloc(0,length,&p->io_channel_offset)) {
			divert_to_dummy(p,-ENOMEM);
			return;
		}
		local_addr = pfs_channel_base() + p->io_channel_offset;

		if(syscall==SYSCALL64_read) {
			p->syscall_result = pfs_read(fd,local_addr,length);
		} else if(syscall==SYSCALL64_pread) {
			p->syscall_result = pfs_pread(fd,local_addr,length,offset);
		} else if(syscall==SYSCALL64_recvfrom) {
			p->syscall_result = pfs_recvfrom(fd,local_addr,length,args[3],(struct sockaddr *)args[4],(int*)args[5]);
		}

		p->diverted_length = 0;

		if(p->syscall_result==0) {
			divert_to_dummy(p,0);
		} else if(p->syscall_result>0) {
			divert_to_channel(p,SYSCALL64_pread,uaddr,p->syscall_result,p->io_channel_offset);
			pfs_read_count += p->syscall_result;
		} else if( errno==EAGAIN ) {
			if(p->interrupted) {
				p->interrupted = 0;
				divert_to_dummy(p,-EINTR);
			} else if(pfs_is_nonblocking(fd)) {
				divert_to_dummy(p,-EAGAIN);
			} else {
				pfs_channel_free(p->io_channel_offset);
				p->state = PFS_PROCESS_STATE_WAITREAD;
				INT64_T rfd = pfs_get_real_fd(fd);
				if(rfd>=0) pfs_poll_wakeon(rfd,PFS_POLL_READ);
			}
		} else {
			divert_to_dummy(p,-errno);
		}
	} else {
		/*
		This is an ugly situation.
		If we arrive here with EINTR, it means that we have copied
		all of the data into the channel, taken any side effects
		of accessing the remote storage device, but the system call
		happened not to read it because of an incoming signal.
		We have no way of re-trying the channel read, so we do
		the ugly slow copy out instead.
		*/

		if( (p->syscall_result==-EINTR) && (p->diverted_length>0) ) {
			tracer_copy_out(p->tracer,pfs_channel_base()+p->io_channel_offset,uaddr,p->diverted_length);
			p->syscall_result = p->diverted_length;
			tracer_result_set(p->tracer,p->syscall_result);
		}

		pfs_channel_free(p->io_channel_offset);
	}
}

/*
decode_write is much the same as read.  We allocate space
in the channel, and then redirect the caller to write
to it.  When the syscall completes, we write the data
to its destination and then set the result.
*/

static void decode_write( struct pfs_process *p, INT64_T entering, INT64_T syscall, INT64_T *args )
{
	if(entering) {
		void *uaddr = POINTER(args[1]);
		INT64_T length = args[2];
		if(!pfs_channel_alloc(0,length,&p->io_channel_offset)) {
			divert_to_dummy(p,-ENOMEM);
			return;
		}
		divert_to_channel(p,SYSCALL64_pwrite,uaddr,length,p->io_channel_offset);
	} else {
		INT64_T actual_result;
		tracer_result_get(p->tracer,&actual_result);

		if(actual_result>0) {
			INT64_T fd = args[0];
			pfs_off_t offset = args[3];
			char *local_addr = pfs_channel_base() + p->io_channel_offset;

			if(syscall==SYSCALL64_write) {
				p->syscall_result = pfs_write(fd,local_addr,actual_result);
			} else if(syscall==SYSCALL64_pwrite) {
				p->syscall_result = pfs_pwrite(fd,local_addr,actual_result,offset);
			} else if(syscall==SYSCALL64_sendto) {
				p->syscall_result = pfs_sendto(fd,local_addr,actual_result,args[3],(struct sockaddr *)args[4],args[5]);
			}

			if(p->syscall_result>=0) {
				tracer_result_set(p->tracer,p->syscall_result);
				pfs_channel_free(p->io_channel_offset);
				p->state = PFS_PROCESS_STATE_KERNEL;
				entering = 0;
				pfs_write_count += p->syscall_result;
			} else {
				if(errno==EAGAIN && !pfs_is_nonblocking(fd)) {
					p->state = PFS_PROCESS_STATE_WAITWRITE;
					INT64_T rfd = pfs_get_real_fd(fd);
					if(rfd>=0) pfs_poll_wakeon(rfd,PFS_POLL_WRITE);
				} else {
					p->syscall_result = -errno;
					tracer_result_set(p->tracer,p->syscall_result);
					pfs_channel_free(p->io_channel_offset);
					if(p->syscall_result==-EPIPE) {
						// make sure that we are not in a wait state,
						// otherwise pfs_process_raise will re-dispatch.
						p->state = PFS_PROCESS_STATE_KERNEL;
						pfs_process_raise(p->pid,SIGPIPE,1);
					}
				}
			}
		} else {
			pfs_channel_free(p->io_channel_offset);
		}
	}
}

static struct pfs_kernel_iovec * iovec_alloc_in( struct pfs_process *p, struct pfs_kernel_iovec *uv, int count )
{
	struct pfs_kernel_iovec *v;
	int size = sizeof(struct pfs_kernel_iovec)*count;

	v = (struct pfs_kernel_iovec *) malloc(size);
	if(v) {
		tracer_copy_in(p->tracer,v,uv,size);
		return v;
	} else {
		return 0;
	}
}

static int iovec_size( struct pfs_process *p, struct pfs_kernel_iovec *v, int count )
{
	int i, total=0;
	for(i=0;i<count;i++) {
		total += v[i].iov_len;
	}
	return total;	
}

static int iovec_copy_in( struct pfs_process *p, char *buf, struct pfs_kernel_iovec *v, int count )
{
	int i, pos=0;
	for(i=0;i<count;i++) {
		tracer_copy_in(p->tracer,&buf[pos],(void*)v[i].iov_base,v[i].iov_len);
		pos += v[i].iov_len;
	}
	return pos;
}

static int iovec_copy_out( struct pfs_process *p, char *buf, struct pfs_kernel_iovec *v, int count )
{
	int i, pos=0;
	for(i=0;i<count;i++) {
		tracer_copy_out(p->tracer,&buf[pos],(void*)v[i].iov_base,v[i].iov_len);
		pos += v[i].iov_len;
	}
	return pos;
}

/*
Both readv and writev have a careful but inefficient implementation.
For each uio block, we examine the data structures, do a manual
read and write in our local buffer, and then copy the data over.
I assume that these are not heavily used system calls, although
they do seem to appear sporadically in X11, the dynamic linker,
and sporadically in networking utilities.
*/

static void decode_readv( struct pfs_process *p, INT64_T entering, INT64_T syscall, INT64_T *args )
{
	if(entering) {
		INT64_T fd = args[0];
		struct pfs_kernel_iovec *uv = (struct pfs_kernel_iovec *) args[1];
		INT64_T count = args[2];

		struct pfs_kernel_iovec *v;
		INT64_T size;
		char *buffer;
		INT64_T result;
		
		if(!uv || count<=0) {
			divert_to_dummy(p,-EINVAL);
			return;
		}

		v = iovec_alloc_in(p,uv,count);
		if(v) {
			size = iovec_size(p,v,count);
			buffer = (char*) malloc(size);
			if(buffer) {
				result = pfs_read(fd,buffer,size);
				if(result>=0) {
					iovec_copy_out(p,buffer,v,count);
					divert_to_dummy(p,result);
				} else {
					if(errno==EAGAIN && !pfs_is_nonblocking(fd)) {
						p->state = PFS_PROCESS_STATE_WAITREAD;
						int rfd = pfs_get_real_fd(fd);
						if(rfd>=0) pfs_poll_wakeon(rfd,PFS_POLL_READ);
					} else {
						divert_to_dummy(p,-errno);
					}
				}
				free(buffer);
			} else {
				divert_to_dummy(p,-ENOMEM);
			}
			free(v);		
		} else {
			divert_to_dummy(p,-ENOMEM);
		}
	}
}

static void decode_writev( struct pfs_process *p, INT64_T entering, INT64_T syscall, INT64_T *args )
{
	if(entering) {
		INT64_T fd = args[0];
		struct pfs_kernel_iovec *uv = (struct pfs_kernel_iovec *) args[1];
		INT64_T count = args[2];

		struct pfs_kernel_iovec *v;
		INT64_T size;
		char *buffer;
		INT64_T result;

		if(!uv || count<=0) {
			divert_to_dummy(p,-EINVAL);
			return;
		}

		v = iovec_alloc_in(p,uv,count);
		if(v) {
			size = iovec_size(p,v,count);
			buffer = (char *) malloc(size);
			if(buffer) {
				iovec_copy_in(p,buffer,v,count);
				result = pfs_write(fd,buffer,size);
				if(result>=0) {
					divert_to_dummy(p,result);
				} else if(result<0) {
					if(errno==EAGAIN && !pfs_is_nonblocking(fd)) {
						/*
                                                WAITREAD is correct here, because WAITWRITE
						would cause us to be called again with entering=0
						*/
						p->state = PFS_PROCESS_STATE_WAITREAD;
						int rfd = pfs_get_real_fd(fd);
						if(rfd>=0) pfs_poll_wakeon(rfd,PFS_POLL_WRITE);
					} else {
						divert_to_dummy(p,-errno);
					}
				}
				free(buffer);
			} else {
				divert_to_dummy(p,-ENOMEM);
			}
			free(v);		
		} else {
			divert_to_dummy(p,-ENOMEM);
		}
	}
}

static void decode_stat( struct pfs_process *p, INT64_T entering, INT64_T syscall, INT64_T *args )
{
	INT64_T fd = args[0];
	void *uaddr = POINTER(args[1]);
	char *local_addr;
	struct pfs_stat lbuf;
	struct pfs_kernel_stat kbuf;
	char path[PFS_PATH_MAX];

	if(entering) {
		p->io_channel_offset = 0;

		if(syscall==SYSCALL64_stat) {
			tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
			p->syscall_result = pfs_stat(path,&lbuf);
		} else if(syscall==SYSCALL64_lstat) {
			tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
			p->syscall_result = pfs_lstat(path,&lbuf);
		} else if(syscall==SYSCALL64_fstat) {
			p->syscall_result = pfs_fstat(fd,&lbuf);
		}

		if(p->syscall_result>=0) {
			if(!pfs_channel_alloc(0,sizeof(kbuf),&p->io_channel_offset)) {
				divert_to_dummy(p,-ENOMEM);
			} else {
				local_addr = pfs_channel_base() + p->io_channel_offset;
				COPY_STAT(lbuf,kbuf);
				memcpy(local_addr,&kbuf,sizeof(kbuf));
				divert_to_channel(p,SYSCALL64_pread,uaddr,sizeof(kbuf),p->io_channel_offset);
			}
		} else {
			divert_to_dummy(p,-errno);
		}
	} else {
		if(p->syscall_result>=0) {
			pfs_channel_free(p->io_channel_offset);
			divert_to_dummy(p,0);
		}
	}
}

static void decode_statfs( struct pfs_process *p, INT64_T entering, INT64_T syscall, INT64_T *args )
{
	INT64_T fd = args[0];
	void *uaddr = POINTER(args[1]);
	char *local_addr;
	struct pfs_statfs lbuf;
	struct pfs_kernel_statfs kbuf;
	char path[PFS_PATH_MAX];
	INT64_T bufsize;

	if(entering) {
		p->io_channel_offset = 0;

		if(syscall==SYSCALL64_statfs) {
			tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
			p->syscall_result = pfs_statfs(path,&lbuf);
		} else if(syscall==SYSCALL64_fstatfs) {
			p->syscall_result = pfs_fstatfs(fd,&lbuf);
		}

		if(p->syscall_result>=0) {
			if(!pfs_channel_alloc(0,sizeof(kbuf),&p->io_channel_offset)) {
				divert_to_dummy(p,-ENOMEM);
			} else {
				local_addr = pfs_channel_base() + p->io_channel_offset;
				COPY_STATFS(lbuf,kbuf);
				memcpy(local_addr,&kbuf,sizeof(kbuf));
				bufsize = sizeof(kbuf);
				divert_to_channel(p,SYSCALL64_pread,uaddr,bufsize,p->io_channel_offset);
			}
		} else {
			divert_to_dummy(p,-errno);
		}
	} else {
		if(p->syscall_result>=0) {
			pfs_channel_free(p->io_channel_offset);
			divert_to_dummy(p,0);
		}
	}
}

/*
This function is an inexpensive test to see if a given
filename is executable.  It is not all-inclusive, nor should
it be considered a reliable security device.  This function is
over-optimistic in some cases, but if it falsely reports
true, the later real execve() may still fail.
*/

static int is_executable( const char *path )
{
	struct pfs_stat buf;

	if(pfs_stat(path,&buf)!=0) return 0;

	if(buf.st_mode&S_ISUID || buf.st_mode&S_ISGID) {
		debug(D_NOTICE,"cannot execute the program %s because it is setuid.",path);
		errno = EACCES;
		return 0;
	}

	if(buf.st_mode&S_IXUSR || buf.st_mode&S_IXGRP || buf.st_mode&S_IXOTH) {
		return 1;
	} else {
		errno = EACCES;
		return 0;
	}
}

static void redirect_ldso( struct pfs_process *p, const char *ldso, INT64_T *args, char * const start_of_available_scratch )
{
	pid_t child_pid;
	int child_status;
	char real_physical_name[PFS_PATH_MAX];
	char ldso_physical_name[PFS_PATH_MAX];
	char *argv[PFS_ARG_MAX], *ext_argv;
	char *ext_real_logical_name;
	char *ext_ldso_physical_name;
	char *ext_real_physical_name;
	INT64_T i, argc;

	strcpy(real_physical_name, p->new_physical_name);
	debug(D_PROCESS,"redirect_ldso: called on %s (%s)", p->new_logical_name, real_physical_name);

	if(pfs_get_local_name(ldso,ldso_physical_name,0,0)!=0) {
		debug(D_PROCESS,"redirect_ldso: cannot get physical name of %s",ldso);

		return;
	}

	/* Unwise to check ldso recursively */
	if (strcmp(real_physical_name, ldso_physical_name) == 0) return;

	/* Test whether loading with ldso would work by */
	/* running ldso --verify on the executable (may be static) */

	child_pid = fork();
	if (child_pid < 0) {
		debug(D_PROCESS,"redirect_ldso: cannot fork");
		return;
	}
	if (child_pid == 0) {
		int fd = open("/dev/null", O_WRONLY);
		if (fd >= 0) {
			close(1);
			close(2);
			dup(fd);
			dup(fd);
		}
		execlp(ldso_physical_name, ldso_physical_name, "--verify", real_physical_name, NULL);
		return;
	}
	waitpid(child_pid, &child_status, 0);

	if (!WIFEXITED(child_status)) {
		debug(D_PROCESS,"redirect_ldso: %s --verify %s didn't exit normally. status == %d", ldso_physical_name, real_physical_name, child_status);
		return;
	}
	if (WEXITSTATUS(child_status) != 0) {
		debug(D_PROCESS,"redirect_ldso: %s --verify %s exited with status %d", ldso_physical_name, real_physical_name, WEXITSTATUS(child_status));
		return;
	}
	
	/* Start with the physical name of ldso  */
	ext_ldso_physical_name = start_of_available_scratch;

	/* strcpy(p->new_logical_name,ldso); */
	strcpy(p->new_physical_name,ldso_physical_name);

	/* then the "real" physical name */
	ext_real_physical_name = ext_ldso_physical_name + strlen(ldso_physical_name) + 1;
	/* and the "real" logical name */
	ext_real_logical_name = ext_real_physical_name + strlen(real_physical_name) + 1;

	/* the new argv goes in the scratch area next */
	ext_argv = ext_real_logical_name + strlen(p->new_logical_name) + 1;

	/* load in the arguments given by the program and count them up */
	tracer_copy_in(p->tracer,argv,POINTER(args[1]),sizeof(argv));
	for(argc=0;argv[argc] && argc<PFS_ARG_MAX;argc++) {}

	/* The original scratch area should have already been saved */

	/* write out the new exe, logical and physical names */
	tracer_copy_out(p->tracer,p->new_logical_name,ext_real_logical_name,strlen(p->new_logical_name)+1);
	tracer_copy_out(p->tracer,ldso_physical_name,ext_ldso_physical_name,strlen(ldso_physical_name)+1);
	tracer_copy_out(p->tracer,real_physical_name,ext_real_physical_name,strlen(real_physical_name)+1);
	/* rebuild the argv copy it out */
	for(i=argc;i>0;i--) argv[i] = argv[i-1];
	argc+=1;
	argv[0] = ext_real_logical_name;
	argv[1] = ext_real_physical_name;
	argv[argc] = 0;
	for(i=0;i<=argc;i++) {
		tracer_copy_out(p->tracer,&argv[i],ext_argv+sizeof(char*)*i,sizeof(char*));
	}

	/* change the registers to reflect argv */
	args[0] = (INT64_T) ext_ldso_physical_name;
	args[1] = (INT64_T) ext_argv;
	tracer_args_set(p->tracer,p->syscall,args,3);

	debug(D_PROCESS,"redirect_ldso: will execute %s %s",ldso,real_physical_name);
}

/*
Several things to note about exec.

An entry to execve looks like a normal syscall.
An exit from execve indicates a successfull execve in progress.
Finally, a *third* event with args[0]==0 indicates an execve
that has completed with the new image active.

Now, we cannot execute the path named by the execve directly.
It must be resolved through PFS, because our idea of the 
current dir (or even the meaning of the name) may be quite
different.  We resolve the file name into a local path,
perhaps by pulling it into the cache.

In the simple (second) case, we copy the new local name
into the address space of the process and exec that instead.
If the exec fails, we must restore the changed bytes afterwards.

In the complex (first) case, the program contains a pound-bang
indicating an interpreter.  We instead resolve the interpreter
as the executable and fiddle around with the job's argv to
indicate that.  Then, we do much the same as the first case.
*/

static void decode_execve( struct pfs_process *p, INT64_T entering, INT64_T syscall, INT64_T *args )
{
	char *scratch_addr  = (char*)pfs_process_scratch_address(p);
	int   scratch_size  = PFS_SCRATCH_SIZE;
	char *scratch_avail = scratch_addr;

	if(args[0]==0) {
		debug(D_PROCESS,"execve: %s complete in 64 bit mode",p->name);
		debug(D_PSTREE,"%d exec %s",p->pid,p->new_logical_name);

		p->state = PFS_PROCESS_STATE_USER;
		p->completing_execve = 0;

	} else if(entering) {
		char path[PFS_PATH_MAX];
		char firstline[PFS_PATH_MAX];

		tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));

		p->new_logical_name[0] = 0;
		p->new_physical_name[0] = 0;
		firstline[0] = 0;

		strcpy(p->new_logical_name,path);

		/* If path is not executable, we simply return, as the
		next call to exec will fail with the correct error
		message. The previous behaviour called
		divert_to_dummy, but this caused the error message to
		be lost. */

		if(!is_executable(path) ||
		   (pfs_get_local_name(path,p->new_physical_name,firstline,sizeof(firstline))<0)) {
			p->new_physical_name[0] = 0;
			p->completing_execve = 1;
			return;
		}

		/* remove any newlines or spaces at the end */

		char *c = firstline;

		while(*c) c++;
		if(c>firstline) c--;

		while( *c=='\n' || *c==' ' ) {
			*c = 0;
			c--;
		}

		/* reset the heap address so it is re-computed */

		p->heap_address = 0;

		if(!strncmp(firstline,"#!",2)) {
			char *argv[PFS_ARG_MAX], *ext_argv;
			char *interp, *ext_interp;
			char *interparg, *ext_interparg;
			char *scriptarg, *ext_scriptarg;
			char *ext_physical_name;
			INT64_T i, argc, shiftargs;

			debug(D_PROCESS,"execve: %s is an interpreted executable",p->new_logical_name);

			/* interp points to the interpreter */
			/* store it in the scratch area */

			interp = &firstline[2];
			while(isspace(*interp)) interp++;
			ext_interp = scratch_addr;

			/* interparg points to the internal argument */
			/* scriptarg points to the script itself */
			interparg = strchr(interp,' ');
			if(interparg) {
				*interparg = 0;
				interparg++;	
				while(isspace(*interparg)) interparg++;
				ext_interparg = ext_interp + strlen(interp) + 1;
				scriptarg = path;
				ext_scriptarg = ext_interparg + strlen(interparg) + 1;
				debug(D_PROCESS,"execve: instead do %s %s %s",interp,interparg,scriptarg);
				shiftargs = 2;
			} else {
				interparg = 0;
				ext_interparg = ext_interp + strlen(interp) + 1; /* BUG ??? Why shoudln't it skip interp ? */
				scriptarg = path;
				ext_scriptarg = ext_interparg;
				shiftargs = 1;
				debug(D_PROCESS,"execve: instead do %s %s",interp,scriptarg);
			}


			/* make sure the new interp is loaded */
			strcpy(p->new_logical_name,interp);
			if(pfs_get_local_name(interp,p->new_physical_name,0,0)!=0) {
				p->new_physical_name[0] = 0;
				return;
			}

			/* the physical name of the interp is next */
			ext_physical_name = ext_scriptarg + strlen(scriptarg) + 1;

			/* make sure redirect_ldso doesn't clobber arguments */
			scratch_avail = ext_physical_name;

			/* the new argv goes in the scratch area next */
			ext_argv = ext_physical_name + strlen(p->new_physical_name) + 1;

			/* load in the arguments given by the program and count them up */
			tracer_copy_in(p->tracer,argv,POINTER(args[1]),sizeof(argv));
			for(argc=0;argv[argc] && argc<PFS_ARG_MAX;argc++) {}

			/* save the scratch area */
			tracer_copy_in(p->tracer,p->scratch_data,scratch_addr,scratch_size);

			/* write out the new interp, arg, and physical name */
			tracer_copy_out(p->tracer,interp,ext_interp,strlen(interp)+1);
			if(interparg) tracer_copy_out(p->tracer,interparg,ext_interparg,strlen(interparg)+1);
			tracer_copy_out(p->tracer,scriptarg,ext_scriptarg,strlen(scriptarg)+1);
			tracer_copy_out(p->tracer,p->new_physical_name,ext_physical_name,strlen(p->new_physical_name)+1);
			/* rebuild the argv copy it out */
			for(i=argc-1+shiftargs;i>0;i--) argv[i] = argv[i-shiftargs];
			argc+=shiftargs;
			argv[0] = ext_interp;
			if(interparg) {
				argv[1] = ext_interparg;
				argv[2] = ext_scriptarg;
			} else {
				argv[1] = ext_scriptarg;
			}
			argv[argc] = 0;
			for(i=0;i<=argc;i++) {
				tracer_copy_out(p->tracer,&argv[i],ext_argv+sizeof(char*)*i,sizeof(char*));
			}

			/* change the registers to reflect argv */
			args[0] = (INT64_T) ext_physical_name;
			args[1] = (INT64_T) ext_argv;
			tracer_args_set(p->tracer,p->syscall,args,3);
		} else {
			debug(D_PROCESS,"execve: %s is an ordinary executable",p->new_logical_name);

			/* save all of the data we are going to clobber */
			tracer_copy_in(p->tracer,p->scratch_data,scratch_addr,scratch_size);

			/* store the new local path */
			tracer_copy_out(p->tracer,p->new_physical_name,scratch_addr,strlen(p->new_physical_name)+1);

			/* set the new program name to the logical name */
			args[0] = (INT64_T) scratch_addr;

			/* up to three arguments (args[0-2]) were modified. */
			tracer_args_set(p->tracer,p->syscall,args,3);
		}

		if (pfs_ldso_path) {
			redirect_ldso(p, pfs_ldso_path, args, scratch_avail);
		}

		/* This forces the next call to return to decode_execve, see comment at top of decode_syscall */
		p->completing_execve = 1;

		debug(D_PROCESS,"execve: %s about to start",p->new_logical_name);
	} else { /* That is, we are not entering */
		INT64_T actual_result;
		tracer_result_get(p->tracer,&actual_result);

		if(actual_result==0) {
			debug(D_PROCESS,"execve: %s succeeded in 64 bit mode",p->new_logical_name);
			strcpy(p->name,p->new_logical_name);

			/* after a successful exec, signal handlers are reset */
			memset(p->signal_interruptible,0,sizeof(p->signal_interruptible));

			/* and certain files in the file table are closed */
			p->table->close_on_exec();

			/* and our knowledge of the address space is gone. */
			p->heap_address = 0;
			p->break_address = 0;
		} else if(p->new_physical_name[0]){
			/* If we did not succeed and we are not
			entering, then the exec must have
			failed. Since new_physical_name is defined,
			that means the scratch was modified too, so we
			need to restore it. */

			debug(D_PROCESS,"execve: %s failed: %s",p->new_logical_name,strerror(-actual_result));
			debug(D_PROCESS,"execve: restoring scratch area at %x",scratch_addr);
			
			tracer_copy_out(p->tracer,p->scratch_data,(void*)scratch_addr,scratch_size);

			p->completing_execve = 0;
		} else {

			/* If we get here, then we are not entering,
			   and p->new_physical_name was never set because
			   is_executable(path) failed. This could
			   happen when the executable is being
			   searched in the PATH directories. Here we
			   do nothing, as nothing has been modified,
			   and the third call to execve never occurs,
			   from which parrot concludes there was an
			   error. */

			p->completing_execve = 0;
			debug(D_PROCESS,"execve: %s failed: %s\n",p->new_logical_name,strerror(-actual_result));
		}

	}
}
/*
Memory mapped files are loaded into the channel,
the whole file regardless of what portion is actually
mapped.  The channel cache keeps a reference count.
*/

static void decode_mmap( struct pfs_process *p, INT64_T syscall, INT64_T entering, INT64_T *args )
{
	if(entering) {
		INT64_T addr, prot, fd, flags;
		pfs_size_t length, source_offset, channel_offset;
		INT64_T nargs[TRACER_ARGS_MAX];

		memcpy(nargs,p->syscall_args,sizeof(p->syscall_args));

		addr = nargs[0];
		length = nargs[1];
		prot = nargs[2];
		flags = nargs[3];
		fd = nargs[4];
		source_offset = nargs[5];

		debug(D_SYSCALL,"mmap addr=0x%x len=0x%x prot=0x%x flags=0x%x fd=%d offset=0x%llx",addr,length,prot,flags,fd,source_offset);

		if(flags & MAP_ANONYMOUS) {
			debug(D_SYSCALL,"mmap skipped b/c anonymous");
			return;
		}

		channel_offset = pfs_mmap_create(fd,source_offset,length,prot,flags);
		if(channel_offset<0) {
			divert_to_dummy(p,-errno);
			return;
		}

		nargs[3] = flags & ~MAP_DENYWRITE;
		nargs[4] = pfs_channel_fd();
		nargs[5] = channel_offset+source_offset;

		debug(D_SYSCALL,"channel_offset=0x%llx source_offset=0x%llx total=0x%x",channel_offset,source_offset,nargs[5]);
		debug(D_SYSCALL,"mmap changed: fd=%d offset=0x%x",nargs[4],nargs[5]);

	      	tracer_args_set(p->tracer,p->syscall,nargs,6);
		p->syscall_args_changed = 1;
	} else {
		/*
		On exit from the system call, retrieve the logical address of
		the mmap as returned to the application.  Then, update the 
		mmap record that corresponds to the proper channel offset.
		On failure, we must unmap the object, which will have a logical
		address of zero because it was never set.
		*/

		if(args[3]&MAP_ANONYMOUS) {
			// nothing to do
		} else {
			tracer_result_get(p->tracer,&p->syscall_result);

			if(p->syscall_result!=-1) {
				pfs_mmap_update(p->syscall_result,0);
			} else {
				pfs_mmap_delete(0,0);
			}

		}
	}

}

static INT64_T decode_ioctl_siocgifconf( struct pfs_process *p, INT64_T fd, INT64_T cmd, void *uaddr )
{
	struct ifconf uifc;
	struct ifconf ifc;
	char *buffer;
	INT64_T length;
	INT64_T result;

	tracer_copy_in(p->tracer,&uifc,uaddr,sizeof(uifc));
	buffer = (char*) malloc(uifc.ifc_len);
	length = tracer_copy_in(p->tracer,buffer,uifc.ifc_buf,uifc.ifc_len);

	ifc.ifc_buf = buffer;
	ifc.ifc_len = length;

	result = pfs_ioctl(fd,cmd,&ifc);

	if(result>=0) {
		uifc.ifc_len = ifc.ifc_len;
		tracer_copy_out(p->tracer,&uifc,uaddr,sizeof(uifc));
		tracer_copy_out(p->tracer,buffer,uifc.ifc_buf,uifc.ifc_len);
	}

	free(buffer);
	
	return result;
}

static void decode_syscall( struct pfs_process *p, INT64_T entering )
{
	INT64_T *args;
	void *x = NULL;

	char path[PFS_PATH_MAX];
	char path2[PFS_PATH_MAX];

	struct sockaddr_un *paddr;
	struct sockaddr_un addr;

	if(entering) {
		p->state = PFS_PROCESS_STATE_KERNEL;
		p->syscall_dummy = 0;
		tracer_args_get(p->tracer,&p->syscall,p->syscall_args);

		/*
		SYSCALL_execve has a different value in 32 and 64 bit modes.
		When an execve forces a switch between execution modes, the
		old system call number is retained, even though the mode has
		changed.  So, we must explicitly check for this condition
		and fix up the system call number to end up in the right code.
		*/

		if(p->completing_execve) {
			if(p->syscall != SYSCALL64_execve)
			{
				debug(D_PROCESS, "Changing execve code number from 32 to 64 bit mode.\n"); 
				p->syscall = SYSCALL64_execve;
			}
			p->completing_execve = 0;
		}


		debug(D_SYSCALL,"%s",tracer_syscall_name(p->tracer,p->syscall));
		p->syscall_original = p->syscall;
		pfs_syscall_count++;

		if(pfs_syscall_totals64) {
			int s = p->syscall;
			if(s>=0 && s<SYSCALL64_MAX) {
				pfs_syscall_totals64[p->syscall]++;
			}
		}
	}

	args = p->syscall_args;

	switch(p->syscall) {
		case SYSCALL64_execve:
			decode_execve(p,entering,p->syscall,args);
			break;

		/*
		Some variants of fork do not propagate ptrace, so we
		must convert them into clone with appropriate flags.
		Once a fork is started, we must trace only that pid
		so that we can determine the child pid before seeing
		any events from the child. On return, we must fill
		in the child process with its parent's ppid.
		*/

		case SYSCALL64_fork:
		case SYSCALL64_clone:
			if(entering) {
				INT64_T newargs[4];
				INT64_T newargs_count;
				if(p->syscall==SYSCALL64_fork || p->syscall==SYSCALL64_vfork) {
					newargs[0] = CLONE_PTRACE|CLONE_PARENT|SIGCHLD;
					newargs[1] = 0;
					newargs_count = 2;
					p->syscall_args_changed = 1;
					debug(D_SYSCALL,"converting fork into clone(%x)",newargs[0]);
				} else {
					newargs[0] = (args[0]&~0xff)|CLONE_PTRACE|CLONE_PARENT|SIGCHLD;
					newargs_count = 1;
					debug(D_SYSCALL,"adjusting clone(%x,%x,%x,%x) -> clone(%x)",args[0],args[1],args[2],args[3],newargs[0]);
				}
				tracer_args_set(p->tracer,SYSCALL64_clone,newargs,newargs_count);
				trace_this_pid = p->pid;
			} else {
				INT64_T childpid;
				struct pfs_process *child;
				tracer_result_get(p->tracer,&childpid);
				if(childpid>0) {
					INT64_T child_signal,clone_files;
					if(p->syscall_original==SYSCALL64_fork) {
						child_signal = SIGCHLD;
						clone_files = 0;
					} else {
						child_signal = args[0]&0xff;
						clone_files = args[0]&CLONE_FILES;
					}
					pid_t notify_parent;
					if(args[0]&(CLONE_PARENT|CLONE_THREAD)) {
						notify_parent = p->ppid;
					} else {
						notify_parent = p->pid;
					}
					child = pfs_process_create(childpid,p->pid,notify_parent,clone_files,child_signal);
					child->syscall_result = 0;
					if(args[0]&CLONE_THREAD) child->tgid = p->tgid;
					if(p->syscall_original==SYSCALL64_fork) {
						memcpy(child->syscall_args,p->syscall_args,sizeof(p->syscall_args));
						child->syscall_args_changed = 1;
					}
					if(pfs_trap_after_fork) {
						child->state = PFS_PROCESS_STATE_KERNEL;
					} else {
						child->state = PFS_PROCESS_STATE_USER;
					}
					debug(D_PROCESS,"%d created pid %d",p->pid,childpid);
					/* now trace any process at all */
					trace_this_pid = -1;
				}

			}
			break;

		/*
		Note that we do not support vfork.  The behavior of vfork
		varies greatly from kernel to kernel, and is in fact impossible
		to support through ptrace without a kernel patch in some cases.
		However, glibc is smart and converts vfork into fork if the
		kernel response that it does not exist.  So, failed vforks
		eventually end up in the previous case.  Also note parrot_helper.so,
		which also aims to solve this problem.
		*/

		case SYSCALL64_vfork:
			if(entering) {
				debug(D_NOTICE,"sorry, I cannot run this program (%s) without parrot_helper.so.",p->name);
				divert_to_dummy(p,-ENOSYS);
			}
			break;

		case SYSCALL64_wait4:
			if(entering) {
				pfs_process_waitpid(p,args[0],(int*)args[1],args[2],(struct rusage*)args[3]);
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/*
		We don't do anything special with exit.  Just let it
		run to completion, and then process the exit event
		in the main loop.
		*/

		case SYSCALL64_exit_group:
			if(entering) pfs_process_exit_group(p);
			break;
	
		case SYSCALL64_exit:
			break;

		/*
		Here begin all of the I/O operations, given in the
		same order as in pfs_table.  Notice that most operations
		use the simple but slow tracer_copy_{in,out} routines.
		When performance is important (write,mmap), we resort
		to redirection I/O to the side channel.
		*/

		/* File descriptor creation */

		case SYSCALL64_open:
		case SYSCALL64_creat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				if(p->syscall==SYSCALL64_creat) {
					p->syscall_result = pfs_open(path,O_CREAT|O_WRONLY|O_TRUNC,args[1]);
				} else {
					p->syscall_result = pfs_open(path,args[1],args[2]);
				}
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_personality:
			if(entering) {
				unsigned long persona = args[0];
				switch (persona) {
					case PER_LINUX:
					case PER_LINUX_32BIT:
					case 0xffffffff: /* get personality */
						/* allow the call to go through to the kernel */
						break;
					default:
						fatal("cannot execute program with personality %d", persona);
				}
			}
			break;

		case SYSCALL64_pipe:
		case SYSCALL64_pipe2:
			if(entering) {
				int fds[2];
				p->syscall_result = pfs_pipe(fds);
				if(p->syscall_result<0) {
					p->syscall_result = -errno;
				} else {
					// The pipe2 variant also sets flags on the pipe ends.
					if(p->syscall==SYSCALL64_pipe2) {
						pfs_fcntl(fds[0],F_SETFL,(void*)args[1]);
						pfs_fcntl(fds[1],F_SETFL,(void*)args[1]);
					}
					tracer_copy_out(p->tracer,(void*)fds,POINTER(args[0]),sizeof(fds));
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/* operations on open files */

		case SYSCALL64_close:
			if(entering) {
				p->syscall_result = pfs_close(args[0]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_read:
		case SYSCALL64_pread:
			decode_read(p,entering,p->syscall,args);
			break;

		case SYSCALL64_write:
		case SYSCALL64_pwrite:
			decode_write(p,entering,p->syscall,args);
			break;

		case SYSCALL64_readv:
			decode_readv(p,entering,p->syscall,args);
			break;

		case SYSCALL64_writev:
			decode_writev(p,entering,p->syscall,args);
			break;

		case SYSCALL64_socket:
			if(entering) {
				p->syscall_result = pfs_socket(args[0],args[1],args[2]);
				if(p->syscall_result<0) p->syscall_result=-errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_bind:
			if(entering) {
				x = xxmalloc(args[2]+2);
				tracer_copy_in(p->tracer,x,POINTER(args[1]),args[2]);
				paddr = (struct sockaddr_un *)x;
				if(paddr->sun_family==AF_UNIX) {
					pfs_name pname;
					((char*)x)[args[2]] = 0;
					if(pfs_resolve_name(paddr->sun_path,&pname)) {
						addr.sun_family = AF_UNIX;
						strcpy(addr.sun_path,pname.path);
						p->syscall_result = pfs_bind(args[0],(struct sockaddr *)&addr,sizeof(addr));
					} else {
						p->syscall_result = -1;
					}
				} else {
					p->syscall_result = pfs_bind(args[0],(struct sockaddr *)x,args[2]);
				}
				if(p->syscall_result<0) p->syscall_result=-errno;
				free(x);
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_connect:
			if(entering) {
				x = xxmalloc(args[2]+2);
				tracer_copy_in(p->tracer,x,POINTER(args[1]),args[2]);
				paddr = (struct sockaddr_un *)x;
				if(paddr->sun_family==AF_UNIX) {
					pfs_name pname;
					((char*)x)[args[2]] = 0;
					if(pfs_resolve_name(paddr->sun_path,&pname)) {
						addr.sun_family = AF_UNIX;
						strcpy(addr.sun_path,pname.path);
						p->syscall_result = pfs_connect(args[0],(struct sockaddr *)&addr,sizeof(addr));
					} else {
						p->syscall_result = -1;
					}
				} else {
					p->syscall_result = pfs_connect(args[0],(struct sockaddr*)x,args[2]);
				}
				if(p->syscall_result>=0) {
					divert_to_dummy(p,p->syscall_result);
				} else if(errno_in_progress(errno)) {
					if(p->interrupted) {
						p->interrupted = 0;
						divert_to_dummy(p,-EINTR);
					} else if(pfs_is_nonblocking(args[0])) {
						divert_to_dummy(p,-EINPROGRESS);
					} else {
						p->state = PFS_PROCESS_STATE_WAITREAD;
						INT64_T rfd = pfs_get_real_fd(args[0]);
						if(rfd>=0) pfs_poll_wakeon(rfd,PFS_POLL_READ|PFS_POLL_WRITE|PFS_POLL_EXCEPT);
					}
				} else {
					divert_to_dummy(p,-errno);
				}
				free(x);
			}
			break;

		case SYSCALL64_listen:
			if(entering) {
				p->syscall_result = pfs_listen(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result=-errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_accept:
			if(entering) {
				int length;
				if(args[1]) {
					tracer_copy_in(p->tracer,&length,POINTER(args[2]),sizeof(length));
					x = xxmalloc(length);
					p->syscall_result = pfs_accept(args[0],(struct sockaddr*)x,&length);
				} else {
					x = 0;
					p->syscall_result = pfs_accept(args[0],0,0);
				}
				if(p->syscall_result>=0) {
					if(x) {
						tracer_copy_out(p->tracer,x,POINTER(args[1]),length);
						tracer_copy_out(p->tracer,&length,POINTER(args[2]),sizeof(length));
					}
					divert_to_dummy(p,p->syscall_result);
				} else if(errno_in_progress(errno)) {
					if(p->interrupted) {
						p->interrupted = 0;
						divert_to_dummy(p,-EINTR);
					} else if(pfs_is_nonblocking(args[0])) {
						divert_to_dummy(p,-EAGAIN);
					} else {
						p->state = PFS_PROCESS_STATE_WAITREAD;
						INT64_T rfd = pfs_get_real_fd(args[0]);
						if(rfd>=0) pfs_poll_wakeon(rfd,PFS_POLL_READ);
					}
				} else {
					divert_to_dummy(p,-errno);
				}
				if(x) free(x);
			}
			break;

		case SYSCALL64_getsockname:
			if(entering) {
				int length,savelength;
				tracer_copy_in(p->tracer,&length,POINTER(args[2]),sizeof(length));
				x = xxmalloc(length);
				savelength = length;
				p->syscall_result = pfs_getsockname(args[0],(struct sockaddr *)x,&length);
				if(p->syscall_result<0) {
					p->syscall_result=-errno;
				} else {
					tracer_copy_out(p->tracer,x,POINTER(args[1]),MIN(length,savelength));
					tracer_copy_out(p->tracer,&length,POINTER(args[2]),sizeof(length));
				}
				free(x);
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_getpeername:
			if(entering) {
				int length;
				tracer_copy_in(p->tracer,&length,POINTER(args[2]),sizeof(length));
				x = xxmalloc(length);
				p->syscall_result = pfs_getpeername(args[0],(struct sockaddr *)x,&length);
				if(p->syscall_result<0) {
					p->syscall_result=-errno;
				} else {
					tracer_copy_out(p->tracer,x,POINTER(args[1]),length);
					tracer_copy_out(p->tracer,&length,POINTER(args[2]),sizeof(length));
				}
				free(x);
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_socketpair:
			if(entering) {
				int fds[2];
				p->syscall_result = pfs_socketpair(args[0],args[1],args[2],fds);
				if(p->syscall_result<0) {
					p->syscall_result=-errno;
				} else {
					tracer_copy_out(p->tracer,fds,POINTER(args[3]),sizeof(fds));
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_sendto: {
			INT64_T t4 = 0;
			if(args[4]) {
				x = xxmalloc(args[5]);
				tracer_copy_in(p->tracer,x,POINTER(args[4]),args[5]);
				t4 = args[4];
				args[4] = (INT64_T)x;
			}
			decode_write(p,entering,p->syscall,args);
			if(args[4]) {
				args[4] = t4;
				free(x);
			}
       			break;
		}

		case SYSCALL64_recvfrom:
			if(entering) {
				INT64_T t4 = 0, t5 = 0;
				int length;
				if(args[4]) {
					tracer_copy_in(p->tracer,&length,POINTER(args[5]),sizeof(length));
					x = xxmalloc(length);
					t4 = args[4];
					t5 = args[5];
					args[4] = (INT64_T) x;
					args[5] = (INT64_T) &length;
				}
				decode_read(p,entering,p->syscall,args);
				if(args[4]) {
					args[4] = t4;
					args[5] = t5;
					if(p->syscall_result>=0) {
						tracer_copy_out(p->tracer,x,POINTER(args[4]),length);
						tracer_copy_out(p->tracer,&length,POINTER(args[5]),sizeof(length));
					}
					free(x);
				}
			} else {
				decode_read(p,entering,p->syscall,args);
			}
			break;

		case SYSCALL64_shutdown:
			if(entering) {
				p->syscall_result = pfs_shutdown(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result=-errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_setsockopt:
			if(entering) {
				x = xxmalloc(args[4]);
				tracer_copy_in(p->tracer,x,POINTER(args[3]),args[4]);
				p->syscall_result = pfs_setsockopt(args[0],args[1],args[2],x,args[4]);
				if(p->syscall_result<0) p->syscall_result=-errno;
				free(x);
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_getsockopt:
			if(entering) {
				int length;
				tracer_copy_in(p->tracer,&length,POINTER(args[4]),sizeof(length));
				x = xxmalloc(length);
				p->syscall_result = pfs_getsockopt(args[0],args[1],args[2],x,&length);
				if(p->syscall_result<0) {
					p->syscall_result=-errno;
				} else {
					tracer_copy_out(p->tracer,x,POINTER(args[3]),length);
					tracer_copy_out(p->tracer,&length,POINTER(args[4]),sizeof(length));
				}		
				free(x);
				divert_to_dummy(p,p->syscall_result);
			}
			break;
		case SYSCALL64_sendmsg:
		case SYSCALL64_recvmsg:
			if(entering) {
				struct msghdr umsg;
				struct pfs_kernel_iovec *uvec = NULL;
				struct msghdr msg;
				struct iovec vec;

				/* Copy in the msghdr structure */
				tracer_copy_in(p->tracer,&umsg,POINTER(args[1]),sizeof(umsg));

				/* Build a copy of all of the various sub-pointers */

				if(umsg.msg_name && umsg.msg_namelen>0) {
					msg.msg_name = xxmalloc(umsg.msg_namelen);
					msg.msg_namelen = umsg.msg_namelen;
					tracer_copy_in(p->tracer,msg.msg_name,(void*)(umsg.msg_name),umsg.msg_namelen);
				} else {
					msg.msg_name = 0;
					msg.msg_namelen = 0;
				}
	
				if(umsg.msg_iov) {
					uvec = iovec_alloc_in(p,(struct pfs_kernel_iovec *)(void*)(umsg.msg_iov),umsg.msg_iovlen);
					msg.msg_iov = &vec;
					msg.msg_iovlen = 1;
					vec.iov_len = iovec_size(p,uvec,umsg.msg_iovlen);
					vec.iov_base = xxmalloc(vec.iov_len);
				} else {
					msg.msg_iov = 0;
					msg.msg_iovlen = 0;
				}

				if(umsg.msg_control && umsg.msg_controllen>0) {
					msg.msg_control = xxmalloc(umsg.msg_controllen);
					msg.msg_controllen = umsg.msg_controllen;
					tracer_copy_in(p->tracer,msg.msg_control,(void*)(umsg.msg_control),umsg.msg_controllen);
				} else {
					msg.msg_control = 0;
					msg.msg_controllen = 0;
				}
				msg.msg_flags = umsg.msg_flags;

				/* Do a sendmsg or recvmsg on the data, and copy out if needed */

				if(p->syscall==SYSCALL64_sendmsg) {
					iovec_copy_in(p,(char*)vec.iov_base,uvec,umsg.msg_iovlen);
					p->syscall_result = pfs_sendmsg(args[0],&msg,args[2]);
				} else {
					p->syscall_result = pfs_recvmsg(args[0],&msg,args[2]);
					if(p->syscall_result>0) {
						iovec_copy_out(p,(char*)vec.iov_base,uvec,umsg.msg_iovlen);
						if(msg.msg_name && msg.msg_namelen>0) {
							tracer_copy_out(p->tracer,msg.msg_name,(void*)(umsg.msg_name),msg.msg_namelen);
						}
						if(msg.msg_control && umsg.msg_controllen>0) {
							tracer_copy_out(p->tracer,msg.msg_control,(void*)(umsg.msg_control),msg.msg_controllen);
						}
						umsg.msg_namelen = msg.msg_namelen;
						umsg.msg_controllen = msg.msg_controllen;
						umsg.msg_flags = msg.msg_flags;
						tracer_copy_out(p->tracer,&umsg,POINTER(args[1]),sizeof(umsg));
					}
				}
	
				/* Delete the msghdr structure */

				if(msg.msg_control) free(msg.msg_control);
				if(msg.msg_iov)     free(msg.msg_iov->iov_base);
				if(uvec)            free(uvec);
				if(msg.msg_name)    free(msg.msg_name);

				if(p->syscall_result>=0) {
					divert_to_dummy(p,p->syscall_result);
				} else {
					divert_to_dummy(p,-errno);
				}
			}
			break;

                                                                 

		/*
		Now we have a series of standard file operations that
		only use the integer arguments, and are (mostly) easily
		passed back and forth.
		*/

		case SYSCALL64_lseek:
			if(entering) {
				p->syscall_result = pfs_lseek(args[0],args[1],args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_ftruncate:
			if(entering) {
				p->syscall_result = pfs_ftruncate(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
		break;

		case SYSCALL64_fstat:
			decode_stat(p,entering,SYSCALL64_fstat,args);
			break;

		case SYSCALL64_fstatfs:
			decode_statfs(p,entering,SYSCALL64_fstatfs,args);
			break;

		case SYSCALL64_flock:
			if(entering) {
				p->syscall_result = pfs_flock(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;
				
		case SYSCALL64_fsync:
		case SYSCALL64_fdatasync:
			if(entering) {
				p->syscall_result = pfs_fsync(args[0]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchdir:
			if(entering) {
				p->syscall_result = pfs_fchdir(args[0]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchown:
			if(entering) {
				p->syscall_result = pfs_fchown(args[0],args[1],args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchmod:
			if(entering) {
				p->syscall_result = pfs_fchmod(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/*
		ioctl presents both bad news and good news.
		The bad news is that all ioctl operations are driver
		specific.  I have no intention of coding up all the
		possible ioctls here, nor could I. However, it is a fair
		assumption that the third argument, when a valid pointer,
		is a continuous and small chunk of memory.  So, we copy
		in one page that it points to (if any), and use that as
		a temporary buffer.  We copy back to the application only
		those words in the buffer that change.  If none change,
		then the argument was likely interpreted as an integer
		rather than a pointer.  A nice coincidence is that 
		copy in is generally efficient, while copy out, though
		slow, is minimized.
		*/

		case SYSCALL64_ioctl:
			if(entering) {
				INT64_T fd = args[0];
				INT64_T cmd = args[1];
				void *uaddr = POINTER(args[2]);
				char buffer[65536];
				char tbuffer[65536];
				INT64_T length = 0;


				if(cmd==SIOCGIFCONF) {
					p->syscall_result = decode_ioctl_siocgifconf(p,fd,cmd,uaddr);
					divert_to_dummy(p,p->syscall_result);
					break;
				}

				if(uaddr) {
					length = tracer_copy_in(p->tracer,buffer,uaddr,sizeof(buffer));
					if(length>0) {
						memcpy(tbuffer,buffer,length);
						p->syscall_result = pfs_ioctl(fd,cmd,buffer);
					} else {
						uaddr = 0;
						p->syscall_result = pfs_ioctl(fd,cmd,uaddr);
					}
				} else {
					p->syscall_result = pfs_ioctl(fd,cmd,uaddr);
				}

				/*
				Hack: If the process asks who the controlling terminal of a tty is,
				the answer will be the Parrot pid.  Change that answer to the pgrp
				of the calling process.
				*/

				if(cmd==PFS_TIOCGPGRP) {
					pid_t newgrp = getpgid(pfs_current->pid);
					debug(D_PROCESS,"tcgetpgrp(%d) changed from %d to %d",fd,*(pid_t*)buffer,newgrp);
					*(pid_t *)buffer = newgrp;
				}

				if(p->syscall_result<0) {
					p->syscall_result = -errno;
				} else {
					if(uaddr) {
						INT64_T i, changed=0;
						for(i=0;i<length;i++) {
							if(tbuffer[i]!=buffer[i]) {
								changed = i+1;
							}
						}
						changed = _ROUND_UP(changed,sizeof(INT64_T));
						tracer_copy_out(p->tracer,buffer,uaddr,changed);
					}
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/*
		Unlike ioctl, fcntl operations are rather generic
		and operate on the file table itself.  These things
		we can parse, understand, and pass along to the 
		file table in most cases.

		We permit the user to set the O_ASYNC flag and thus
		receive activity notification via SIGIO.  However,
		we don't yet support extended signal information.
		*/

		case SYSCALL64_fcntl:
			if(entering) {
				PTRINT_T pid;
				INT64_T fd = args[0];
				INT64_T cmd = args[1];
				void *uaddr = POINTER(args[2]);
				struct flock fl;

				switch(cmd) {
					case F_DUPFD:
					case F_GETFD:
					case F_SETFD:
					case F_GETFL:
					case F_SETFL:
						p->syscall_result = pfs_fcntl(fd,cmd,uaddr);
						if(p->syscall_result<0) p->syscall_result=-errno;
						divert_to_dummy(p,p->syscall_result);

						if(cmd==F_SETFL) {
							INT64_T flags = (int)args[2];
							if(flags&O_ASYNC) {
								debug(D_PROCESS,"pid %d requests O_ASYNC on fd %d",pfs_current->pid,fd);
								p->flags |= PFS_PROCESS_FLAGS_ASYNC;
							}
						}
						break;

					case PFS_GETLK:
					case PFS_SETLK:
					case PFS_SETLKW:
						tracer_copy_in(p->tracer,&fl,uaddr,sizeof(fl));
						p->syscall_result = pfs_fcntl(fd,cmd,&fl);
						if(p->syscall_result<0) {
							p->syscall_result=-errno;
						} else {
							tracer_copy_out(p->tracer,&fl,uaddr,sizeof(fl));
						}
						divert_to_dummy(p,p->syscall_result);
						break;

					/* Pretend that the caller is the signal recipient */
					case F_GETOWN:
						divert_to_dummy(p,p->pid);
						break;

					/* But we always get the signal. */
					case F_SETOWN:
						debug(D_PROCESS,"pid %d requests F_SETOWN on fd %d",pfs_current->pid,fd);
						p->flags |= PFS_PROCESS_FLAGS_ASYNC;
						pid = getpid();
						pfs_fcntl(fd,F_SETOWN,(void*)pid);
						divert_to_dummy(p,0);
						break;

					default:
						divert_to_dummy(p,-ENOSYS);
						break;
				}
			}
			break;


		case SYSCALL64_mmap:
			decode_mmap(p,p->syscall,entering,args);
			break;


		/*
		For unmap, we update our internal records for what
		is unmapped, which may cause a flush of dirty data.
		However, we do not divert the system call, because
		we still want the real mapping undone in the process.
		*/

		case SYSCALL64_munmap:
			if(entering) {
				pfs_mmap_delete(args[0],args[1]);
			}
			break;

		/*
		For select, we must copy in all the data structures
		that are pointed to, select, and then copy out.
		Notice that on Linux, newselect has the ordinary
		interface, while SYSCALL64_select keeps all of the arguments
		in a single structure.
		*/

		case SYSCALL64_select:
			if(entering) {
				INT64_T maxfd = args[0];
				fd_set rset, wset, eset;
				struct timeval tv;
				fd_set *prset, *pwset, *peset;
				struct timeval *ptv;
				INT64_T nlongs,nbytes;

				nlongs = (maxfd+31)/32;
				nbytes = nlongs*4;

				FD_ZERO(&rset);
				FD_ZERO(&wset);
				FD_ZERO(&eset);

				if(args[1]) {
					tracer_copy_in(p->tracer,&rset,POINTER(args[1]),nbytes);
					prset = &rset;
				} else {
					prset = 0;
				}

				if(args[2]) {
					tracer_copy_in(p->tracer,&wset,POINTER(args[2]),nbytes);
					pwset = &wset;
				} else {
					pwset = 0;
				}

				if(args[3]) {
					tracer_copy_in(p->tracer,&eset,POINTER(args[3]),nbytes);
					peset = &eset;
				} else {
					peset = 0;
				}

				if(args[4]) {
					tracer_copy_in(p->tracer,&tv,POINTER(args[4]),sizeof(tv));
					ptv = &tv;
				} else {
					ptv = 0;
				}

				p->syscall_result = pfs_select(maxfd,prset,pwset,peset,ptv);

				if(p->syscall_result>=0) {
					divert_to_dummy(p,p->syscall_result);
					if(prset) tracer_copy_out(p->tracer,prset,POINTER(args[1]),nbytes);
					if(pwset) tracer_copy_out(p->tracer,pwset,POINTER(args[2]),nbytes);
					if(peset) tracer_copy_out(p->tracer,peset,POINTER(args[3]),nbytes);
					if(ptv) tracer_copy_out(p->tracer,ptv,POINTER(args[4]),sizeof(*ptv));
				} else if(errno==EAGAIN) {
					if(p->interrupted) {
						p->interrupted = 0;
						divert_to_dummy(p,-EINTR);
					} else {
						p->state = PFS_PROCESS_STATE_WAITREAD;
					}
				} else {
					divert_to_dummy(p,-errno);
				}
			}
			break;

		case SYSCALL64_poll:
			if(entering) {
				struct pollfd *ufds;
				if(args[1]>1024) {
					divert_to_dummy(p,-EINVAL);
				} else {
					INT64_T length = sizeof(*ufds)*args[1];
					ufds = (struct pollfd *) malloc(length);
					if(ufds) {
						tracer_copy_in(p->tracer,ufds,POINTER(args[0]),length);
						p->syscall_result = pfs_poll(ufds,args[1],args[2]);
						if(p->syscall_result>=0) {
							divert_to_dummy(p,p->syscall_result);
							tracer_copy_out(p->tracer,ufds,POINTER(args[0]),length);
						} else if(errno==EAGAIN) {
							if(p->interrupted) {
								p->interrupted = 0;
								divert_to_dummy(p,-EINTR);
							} else {
								p->state = PFS_PROCESS_STATE_WAITREAD;
							}
						} else {
							divert_to_dummy(p,-errno);
						}
					free(ufds);
					} else {
						divert_to_dummy(p,-ENOMEM);
					}
				}
			}
			break;

		/*
		Next, we have operations that do not modify any files
		in particular, but change the state of the file table
		within the process in question.
		*/

		case SYSCALL64_chdir:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_chdir(path);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_getcwd:
			if(entering) {
				if(pfs_getcwd(path,sizeof(path))) {
					p->syscall_result = strlen(path)+1;
					if(p->syscall_result>args[1]) {
						p->syscall_result = -ERANGE;
					} else {
						tracer_copy_out(p->tracer,path,POINTER(args[0]),p->syscall_result);
					}
				} else {
					p->syscall_result = -errno;
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_dup:
			if(entering) {
				p->syscall_result = pfs_dup(args[0]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_dup2:
			if(entering) {
				p->syscall_result = pfs_dup2(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/*
		Next we have all of the system calls that work on
		a file name, rather than an open file.  In most cases,
		we use the (fast) tracer_copy_in to fetch the file
		name, and then invoke the pfs_  XXX We should have
		some sort of bounds checking on the path name.
		*/

		case SYSCALL64_stat:
			decode_stat(p,entering,SYSCALL64_stat,args);
			break;
		case SYSCALL64_lstat:
			decode_stat(p,entering,SYSCALL64_lstat,args);
			break;
		case SYSCALL64_statfs:
			decode_statfs(p,entering,SYSCALL64_statfs,args);
			break;

		case SYSCALL64_access:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_access(path,args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_chmod:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_chmod(path,args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_chown:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_chown(path,args[1],args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_lchown:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_lchown(path,args[1],args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_truncate:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_truncate(path,args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_unlink:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_unlink(path);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_rename:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[1]),sizeof(path2));
				p->syscall_result = pfs_rename(path,path2);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_link:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[1]),sizeof(path2));
				p->syscall_result = pfs_link(path,path2);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_symlink:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[1]),sizeof(path2));
				p->syscall_result = pfs_symlink(path,path2);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_readlink:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_readlink(path,path2,sizeof(path2));
				if(p->syscall_result<0) {
					p->syscall_result = -errno;
				} else {
					tracer_copy_out(p->tracer,path2,POINTER(args[1]),p->syscall_result);
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_mknod:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_mknod(path,args[1],args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_mkdir:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_mkdir(path,args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_rmdir:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_rmdir(path);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/*
		Although pfs_table supports the high-level operations
		opendir/readdir/closedir, all we can get a hold of at
		this level is getdents, which works on an open file
		descriptor.  We copy dirents out one by one using fdreaddir,
		and transform them into the type expected by the kernel.
		If we overrun the available buffer, immediately seek
		the fd back to where it was before.
		*/

		case SYSCALL64_getdents:
			if(entering) {
				INT64_T fd = args[0];
				char *uaddr = (char*) args[1];
				INT64_T length = args[2];
				INT64_T result = 0;

				struct dirent *d;
				struct pfs_kernel_dirent buf;

				errno = 0;
				while((d=pfs_fdreaddir(fd))) {
					COPY_DIRENT(*d,buf);
					if(DIRENT_SIZE(buf)>(UINT64_T)length) {
						pfs_lseek(fd,d->d_off,SEEK_SET);
						errno = EINVAL;
						break;
					}
					tracer_copy_out(p->tracer,&buf,(void*)uaddr,buf.d_reclen);
					uaddr  += buf.d_reclen;
					length -= buf.d_reclen;
					result += buf.d_reclen;
				}

				if(result==0 && errno!=0) {
					p->syscall_result = -errno;
				} else {
					p->syscall_result = result;
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_getdents64:
                        if(entering) {
                                INT64_T fd = args[0];
                                char *uaddr = (char*) args[1];
                                INT64_T length = args[2];
                                INT64_T result = 0;
                                                                                                                   
                                struct dirent *d;
                                struct pfs_kernel_dirent64 buf;
                                                                                                                   
                                errno = 0;
                                while((d=pfs_fdreaddir(fd))) {
                                        COPY_DIRENT(*d,buf);
                                        if(DIRENT_SIZE(buf)>(UINT64_T)length) {
                                                pfs_lseek(fd,d->d_off,SEEK_SET);
                                                errno = EINVAL;
                                                break;
                                        }
                                        tracer_copy_out(p->tracer,&buf,(void*)uaddr,buf.d_reclen);
                                        uaddr  += buf.d_reclen;
                                        length -= buf.d_reclen;
                                        result += buf.d_reclen;
                                }
                                                                                                                   
                                if(result==0 && errno!=0) {
                                        p->syscall_result = -errno;
                                } else {
                                        p->syscall_result = result;
                                }
                                divert_to_dummy(p,p->syscall_result);
                        }
                        break;

		case SYSCALL64_utime:
			if(entering) {
				struct utimbuf ut;
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				if(args[1]) {
					tracer_copy_in(p->tracer,&ut,POINTER(args[1]),sizeof(ut));
				} else {
					ut.actime = ut.modtime = time(0);
				}
				p->syscall_result = pfs_utime(path,&ut);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_utimes:
			if(entering) {
				struct timeval times[2];
				struct utimbuf ut;
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				if(args[1]) {
					tracer_copy_in(p->tracer,times,POINTER(args[1]),sizeof(times));
					ut.actime = times[0].tv_sec;
					ut.modtime = times[1].tv_sec;
				} else {
					ut.actime = ut.modtime = time(0);
				}
				p->syscall_result = pfs_utime(path,&ut);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_utimensat:
			if(entering) {
				int dirfd = args[0];
				if (POINTER(args[1])) /* pathname may be NULL */
					tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				struct timespec times[2];
				if (args[2]) {
					tracer_copy_in(p->tracer,times,POINTER(args[2]),sizeof(times));
				} else {
#ifdef UTIME_NOW
					times[0].tv_nsec = UTIME_NOW;
					times[1].tv_nsec = UTIME_NOW;
#else
					times[0].tv_sec = times[1].tv_sec = time(0);
					times[0].tv_nsec = times[0].tv_nsec = 0;
#endif
				}
				int flags = args[3];

				p->syscall_result = pfs_utimensat(dirfd,POINTER(args[1]) == NULL ? NULL : path,times,flags);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_openat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_openat(args[0],path,args[2],args[3]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_mkdirat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_mkdirat(args[0],path,args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_mknodat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_mknodat(args[0],path,args[2],args[3]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchownat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_fchownat(args[0],path,args[2],args[3],args[4]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_futimesat:
			if(entering) {
				struct timeval times[2];
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				if(args[2]) {
					tracer_copy_in(p->tracer,times,POINTER(args[2]),sizeof(times));
				} else {
					gettimeofday(&times[0],0);
					times[1] = times[0];
				}
				p->syscall_result = pfs_futimesat(args[0],path,times);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_newfstatat:
			if(entering) {
				struct pfs_stat lbuf;
				struct pfs_kernel_stat kbuf;

				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_fstatat(args[0],path,&lbuf,args[3]);
				if(p->syscall_result<0) {
					p->syscall_result = -errno;
				} else {
					COPY_STAT(lbuf,kbuf);
					tracer_copy_out(p->tracer,&kbuf,POINTER(args[2]),sizeof(kbuf));
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_unlinkat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_unlinkat(args[0],path,args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_renameat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[3]),sizeof(path2));
				p->syscall_result = pfs_renameat(args[0],path,args[2],path2);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}

			break;
		case SYSCALL64_linkat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[3]),sizeof(path2));
				p->syscall_result = pfs_linkat(args[0],path,args[2],path2,args[4]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_symlinkat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[2]),sizeof(path2));
				p->syscall_result = pfs_symlinkat(path,args[1],path2);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_readlinkat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_readlinkat(args[0],path,path2,sizeof(path2));
				if(p->syscall_result<0) {
					p->syscall_result = -errno;
				} else {
					tracer_copy_out(p->tracer,path2,POINTER(args[2]),p->syscall_result);
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchmodat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_fchmodat(args[0],path,args[2],args[3]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_faccessat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_faccessat(args[0],path,args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_uname:
			if(pfs_false_uname) {
				struct utsname u;
				tracer_copy_in(p->tracer,&u,POINTER(args[0]),sizeof(struct utsname));
				strcpy(u.nodename,pfs_false_uname);
				tracer_copy_out(p->tracer,&u,POINTER(args[0]),sizeof(struct utsname));
			}
			break;

		/*
		Note that we call pfs_process_raise here so that the process data
		structures are made aware of the signal propagation, possibly kicking
		someone out of sleep.  However, we do *not* convert this call to
		a dummy, so that the sender can deliver itself, thus getting the
		correct data into the sa_info structure.
		*/

		case SYSCALL64_kill:
		case SYSCALL64_tkill:
			if(entering) {
				debug(D_PROCESS,"%s %d %d %d",tracer_syscall64_name(p->syscall),args[0],args[1],args[2]);
				p->syscall_result = pfs_process_raise(args[0],args[1],0);
				if (p->syscall_result == -1) p->syscall_result = -errno;
			}
			break;

		case SYSCALL64_tgkill:
			if(entering) {
				debug(D_PROCESS,"tgkill %d %d %d",args[0],args[1],args[2]);
				p->syscall_result = pfs_process_raise(args[1],args[2],0);
				if (p->syscall_result == -1) p->syscall_result = -errno;
			}
			break;

		/*
		We need to track the umask ourselves and use it in open.
		*/

		case SYSCALL64_umask:
			if(entering) {
				INT64_T old_umask = pfs_current->umask;
				pfs_current->umask = args[0] & 0777;
				divert_to_dummy(p,old_umask);
			}
			break;

		/*
		The tracing mechanism re-parents traced children,
		so we must fake the parent pid if the child wants
		to send its parent a signal.
		*/

		case SYSCALL64_getppid:
			divert_to_dummy(p,p->ppid);
			break;

		/*
		Always return the dummy uids.	
		*/

		case SYSCALL64_geteuid:
		case SYSCALL64_getuid:
			divert_to_dummy(p,pfs_uid);
			break;

		case SYSCALL64_getegid:
		case SYSCALL64_getgid:
			divert_to_dummy(p,pfs_gid);
			break;

		case SYSCALL64_getresuid:
			tracer_copy_out(p->tracer,&pfs_uid,POINTER(args[0]),sizeof(pfs_uid));
			tracer_copy_out(p->tracer,&pfs_uid,POINTER(args[1]),sizeof(pfs_uid));
			tracer_copy_out(p->tracer,&pfs_uid,POINTER(args[2]),sizeof(pfs_uid));
			divert_to_dummy(p,0);
			break;
		

		case SYSCALL64_getresgid:
			tracer_copy_out(p->tracer,&pfs_gid,POINTER(args[0]),sizeof(pfs_uid));
			tracer_copy_out(p->tracer,&pfs_gid,POINTER(args[1]),sizeof(pfs_uid));
			tracer_copy_out(p->tracer,&pfs_gid,POINTER(args[2]),sizeof(pfs_uid));
			divert_to_dummy(p,0);
			break;

		case SYSCALL64_setsid:
			if(entering) {
				pfs_current->tty[0] = 0;
			}
			break;

		/*
		Generally speaking, the kernel implements signal handling,
		so we just pass through operations such as sigaction and signal.
		However, we must keep track of which signals are allowed to
		interrupt I/O operations in progress.  Each process has an
		array, signal_interruptible, that records this. The SA_RESTART
		flag to sigaction can turn this on or off.  The traditional
		BSD signal() always turns it on.
		*/

		case SYSCALL64_rt_sigaction:
			if(entering) {
				if(args[1]) {
					INT64_T sig = args[0];
					struct pfs_kernel_sigaction act;
					int r = tracer_copy_in(p->tracer,&act,POINTER(args[1]),sizeof(act));
                                	if(r!=sizeof(act)) debug(D_NOTICE,"rt_sigaction: %d",r);
					if(act.pfs_sa_flags&SA_RESTART) {
						pfs_current->signal_interruptible[sig] = 0;
					} else {
						pfs_current->signal_interruptible[sig] = 1;
					}
				}
			}
			break;

		case SYSCALL64_parrot_lsalloc:
			if(entering) {
				char alloc_path[PFS_PATH_MAX];
				pfs_ssize_t avail, inuse;
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_lsalloc(path,alloc_path,&avail,&inuse);
				if(p->syscall_result>=0) {
					tracer_copy_out(p->tracer,alloc_path,POINTER(args[1]),strlen(alloc_path));
					tracer_copy_out(p->tracer,&avail,POINTER(args[2]),sizeof(avail));
					tracer_copy_out(p->tracer,&inuse,POINTER(args[3]),sizeof(inuse));
				} else {
					p->syscall_result = -errno;
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_parrot_mkalloc:
			if(entering) {
				pfs_ssize_t size;
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				tracer_copy_in(p->tracer,&size,POINTER(args[1]),sizeof(size));
				p->syscall_result = pfs_mkalloc(path,size,args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_search:
			if (entering) {
				char callsite[PFS_PATH_MAX];
				tracer_copy_in_string(p->tracer, callsite, POINTER(args[5]), sizeof(callsite));
				debug(D_SYSCALL, "search %s", callsite);

				char path[2*PFS_PATH_MAX];
				char pattern[PFS_PATH_MAX];
				int flags = args[2];
				int buffer_length = args[4];
				char *buffer = (char*) malloc(buffer_length);

				if (!buffer) {
					p->syscall_result = -ENOMEM;
					break;
				}

				size_t i = 0;
				tracer_copy_in_string(p->tracer, path, POINTER(args[0]), sizeof(path));
				tracer_copy_in_string(p->tracer, pattern, POINTER(args[1]), sizeof(pattern));
				p->syscall_result = pfs_search(path, pattern, flags, buffer, buffer_length, &i);

				if (i==0) *buffer = '\0';

				tracer_copy_out(p->tracer, buffer, POINTER(args[3]), i+1);
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_parrot_setacl:
			if(entering) {
				char path[PFS_PATH_MAX];
				char subject[PFS_PATH_MAX];
				char rights[PFS_PATH_MAX];
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				tracer_copy_in_string(p->tracer,subject,POINTER(args[1]),sizeof(subject));
				tracer_copy_in_string(p->tracer,rights,POINTER(args[2]),sizeof(rights));
				p->syscall_result = pfs_setacl(path,subject,rights);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_parrot_getacl:
		case SYSCALL64_parrot_whoami:
			if(entering) {
				char path[PFS_PATH_MAX];
				char buffer[4096];
				unsigned size=args[2];

				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				if(size>sizeof(buffer)) size = sizeof(buffer);

				if(p->syscall==SYSCALL64_parrot_getacl) {
					p->syscall_result = pfs_getacl(path,buffer,sizeof(buffer));
				} else {
					p->syscall_result = pfs_whoami(path,buffer,sizeof(buffer));
				}

				if(p->syscall_result>=0) {
					tracer_copy_out(p->tracer,buffer,POINTER(args[1]),p->syscall_result);
				} else {
					p->syscall_result = -errno;
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_parrot_locate:
			if(entering) {
				char path[PFS_PATH_MAX];
				char buffer[4096];
				unsigned size=args[2];

				if (args[0]) {
					tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
					if(size>sizeof(buffer)) size = sizeof(buffer);
				} else {
					path[0] = 0;
				}

				p->syscall_result = pfs_locate(path,buffer,sizeof(buffer));

				if(p->syscall_result>=0) {
					tracer_copy_out(p->tracer,buffer,POINTER(args[1]),p->syscall_result);
				} else {
					p->syscall_result = -errno;
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_parrot_timeout:
			if(entering) {
				char buffer[1024];
				if (args[0]) {
					tracer_copy_in_string(p->tracer,buffer,POINTER(args[0]),sizeof(buffer));
					p->syscall_result = pfs_timeout(buffer);
				} else {
					p->syscall_result = pfs_timeout(NULL);
				}

				if(p->syscall_result<0) {
					p->syscall_result = -errno;
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_parrot_copyfile:
			if(entering) {
				char source[PFS_PATH_MAX];
				char target[PFS_PATH_MAX];

				tracer_copy_in_string(p->tracer,source,POINTER(args[0]),sizeof(source));
				tracer_copy_in_string(p->tracer,target,POINTER(args[1]),sizeof(target));

				p->syscall_result = pfs_copyfile(source,target);

				if(p->syscall_result<0) {
					p->syscall_result = -errno;
				}

				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_parrot_md5:
			if(entering) {
				char digest[16];
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_md5(path,(unsigned char*)digest);
				if(p->syscall_result>=0) {
					tracer_copy_out(p->tracer,digest,POINTER(args[1]),sizeof(digest));
				} else {
					p->syscall_result = -errno;
				}

				divert_to_dummy(p,p->syscall_result);
			}
			break;
				
		/*
		Changing the userid is not allow, but for completeness,
		you can always change to your own uid.
		*/
		case SYSCALL64_setgid:
		case SYSCALL64_setregid:
		case SYSCALL64_setuid:
		case SYSCALL64_setresuid:
		case SYSCALL64_setresgid:
		case SYSCALL64_setfsuid:
		case SYSCALL64_setfsgid:
		case SYSCALL64_setreuid:
			divert_to_dummy(p,0);
			break;

		/* Whenever the break address is udpated validly, save it. */
		/* This is used as one way of computing a scratch space. */

		case SYSCALL64_brk:
			if(entering) {
			} else {
				if(p->syscall_result==0) {
					if(p->syscall_args[0]!=0) {
						p->break_address = p->syscall_args[0];
						debug(D_PROCESS,"break address: %x",p->break_address);
					}
				}
				else 
				{
					/* On brk error, our knowledge of the address space might be incorrect. */
					p->break_address = 0;
					p->heap_address = 0;
				}
			}
			break;

		/*
		Because parrot is in control of the session
		and the process parentage, it must do set/getpgid
		on behalf of the child process.
		*/
		case SYSCALL64_getpgid:
			if(entering) {	
				p->syscall_result = getpgid(p->syscall_args[0]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

               case SYSCALL64_setpgid:
                        if(entering) {  
                                p->syscall_result = setpgid(p->syscall_args[0],p->syscall_args[1]);
                                if(p->syscall_result<0) p->syscall_result = -errno;
                                divert_to_dummy(p,p->syscall_result);
                        }
                        break;

		/*
		These things are not currently permitted.
		*/
		case SYSCALL64_chroot:
		case SYSCALL64_mount:
		case SYSCALL64_sysfs:
		case SYSCALL64_umount2:
		case SYSCALL64_uselib:
		case SYSCALL64_lookup_dcookie:
		case SYSCALL64_remap_file_pages:

			divert_to_dummy(p,-EPERM);
			break;
		/*
		These system calls are historical artifacts
		or otherwise not necessary to support.
		*/
		case SYSCALL64_acct:
		case SYSCALL64_fadvise64:
			divert_to_dummy(p,-ENOSYS);
			break;

		/*
		A wide variety of calls have no relation to file
		access, so we simply send them along to the
		underlying OS.
		*/

		case SYSCALL64__sysctl:
		case SYSCALL64_adjtimex:
		case SYSCALL64_afs_syscall:
		case SYSCALL64_alarm:
		case SYSCALL64_capget:
		case SYSCALL64_capset:
		case SYSCALL64_create_module:		
		case SYSCALL64_delete_module:		
		case SYSCALL64_futex:
		case SYSCALL64_get_kernel_syms:
		case SYSCALL64_getgroups:
		case SYSCALL64_getitimer:
		case SYSCALL64_getpgrp:
		case SYSCALL64_getpid:
		case SYSCALL64_getpriority:
		case SYSCALL64_getrlimit:
		case SYSCALL64_getrusage:
		case SYSCALL64_getsid:
		case SYSCALL64_gettid:
		case SYSCALL64_gettimeofday:
		case SYSCALL64_init_module:		
		case SYSCALL64_ioperm:
		case SYSCALL64_iopl:
		case SYSCALL64_madvise:
		case SYSCALL64_mincore:
		case SYSCALL64_mlock:
		case SYSCALL64_mlockall:
		case SYSCALL64_modify_ldt:
		case SYSCALL64_mprotect:
		case SYSCALL64_mremap:
		case SYSCALL64_msync:
		case SYSCALL64_munlock:
		case SYSCALL64_munlockall:
		case SYSCALL64_nanosleep:
		case SYSCALL64_pause:
		case SYSCALL64_prctl:
		case SYSCALL64_arch_prctl:
		case SYSCALL64_query_module:
		case SYSCALL64_quotactl:
		case SYSCALL64_reboot:
		case SYSCALL64_restart_syscall:
		case SYSCALL64_rt_sigpending:
		case SYSCALL64_rt_sigprocmask:
		case SYSCALL64_rt_sigqueueinfo:
		case SYSCALL64_rt_sigreturn:
		case SYSCALL64_rt_sigsuspend:
		case SYSCALL64_rt_sigtimedwait:
		case SYSCALL64_sched_get_priority_max:
		case SYSCALL64_sched_get_priority_min:
		case SYSCALL64_sched_getparam:
		case SYSCALL64_sched_getscheduler:
		case SYSCALL64_sched_rr_get_interval:
		case SYSCALL64_sched_setparam:
		case SYSCALL64_sched_setscheduler:
		case SYSCALL64_sched_yield:
		case SYSCALL64_setdomainname:
		case SYSCALL64_setgroups:
		case SYSCALL64_sethostname:
		case SYSCALL64_setitimer:
		case SYSCALL64_setpriority:
		case SYSCALL64_setrlimit:
		case SYSCALL64_settimeofday:
		case SYSCALL64_set_tid_address:
		case SYSCALL64_sigaltstack:
		case SYSCALL64_swapoff:
		case SYSCALL64_swapon:
		case SYSCALL64_sync:
		case SYSCALL64_sysinfo:
		case SYSCALL64_syslog:
		case SYSCALL64_time:
		case SYSCALL64_times:
		case SYSCALL64_ustat:
		case SYSCALL64_vhangup:
		case SYSCALL64_sched_setaffinity:
		case SYSCALL64_sched_getaffinity:
		case SYSCALL64_set_thread_area:
		case SYSCALL64_get_thread_area:
		case SYSCALL64_timer_create:
		case SYSCALL64_timer_settime:
		case SYSCALL64_timer_gettime:
		case SYSCALL64_timer_getoverrun:
		case SYSCALL64_timer_delete:
		case SYSCALL64_clock_settime:
		case SYSCALL64_clock_gettime:
		case SYSCALL64_clock_getres:
		case SYSCALL64_clock_nanosleep:
		case SYSCALL64_migrate_pages:
		case SYSCALL64_get_robust_list:
		case SYSCALL64_set_robust_list:
		case SYSCALL64_move_pages:
			break;

		/* These *xattr system calls were originally not supported.  The main
		 * reason for this is their being unstandardized in POSIX or anywhere
		 * else.
		 *
		 * The original rationale (comment) for not having support for extended
		 * attributes also mentioned JFS being the only filesystem on Linux
		 * with support. This has since changed as, according to Wikipedia,
		 * "ext2, ext3, ext4, JFS, ReiserFS, XFS, Btrfs and OCFS2 1.6
		 * filesystems support extended attributes".
		 *
		 * Original comment also said "Libraries expect these to return
		 * EOPNOTSUPP". If the underlying filesystem does not support, we
		 * should make sure the appropriate errno is returned.
		 */

		case SYSCALL64_getxattr:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path)); /* args[0] */
				char name[4096]; /* args[1] */
				void *value; /* args[2] */
				size_t size = args[3]; /* args[3] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));
				value = malloc(size);
				if (value == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}

				p->syscall_result = pfs_getxattr(path,name,value,size);
				if(p->syscall_result>=0)
					tracer_copy_out(p->tracer,value,POINTER(args[2]),size);
				else
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(value);
			}
			break;

		case SYSCALL64_lgetxattr:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path)); /* args[0] */
				char name[4096]; /* args[1] */
				void *value; /* args[2] */
				size_t size = args[3]; /* args[3] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));
				value = malloc(size);
				if (value == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}

				p->syscall_result = pfs_lgetxattr(path,name,value,size);
				if(p->syscall_result>=0)
					tracer_copy_out(p->tracer,value,POINTER(args[2]),size);
				else
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(value);
			}
			break;

		case SYSCALL64_fgetxattr:
			if(entering) {
				int fd = args[0]; /* args[0] */
				char name[4096]; /* args[1] */
				void *value; /* args[2] */
				size_t size = args[3]; /* args[3] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));
				value = malloc(size);
				if (value == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}

				p->syscall_result = pfs_fgetxattr(fd,name,value,size);
				if(p->syscall_result>=0)
					tracer_copy_out(p->tracer,value,POINTER(args[2]),size);
				else
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(value);
			}
			break;

		case SYSCALL64_listxattr:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path)); /* args[0] */
				char *list; /* args[1] */
				size_t size = args[2]; /* args[2] */

				list = (char *) malloc(size);
				if (list == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}

				p->syscall_result = pfs_listxattr(path,list,size);
				if(p->syscall_result>=0)
					tracer_copy_out(p->tracer,list,POINTER(args[1]),size);
				else
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(list);
			}
			break;

		case SYSCALL64_llistxattr:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path)); /* args[0] */
				char *list; /* args[1] */
				size_t size = args[2]; /* args[2] */

				list = (char *) malloc(size);
				if (list == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}

				p->syscall_result = pfs_llistxattr(path,list,size);
				if(p->syscall_result>=0)
					tracer_copy_out(p->tracer,list,POINTER(args[1]),size);
				else
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(list);
			}
			break;

		case SYSCALL64_flistxattr:
			if(entering) {
				int fd = args[0]; /* args[0] */
				char *list; /* args[1] */
				size_t size = args[2]; /* args[2] */

				list = (char *) malloc(size);
				if (list == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}

				p->syscall_result = pfs_flistxattr(fd,list,size);
				if(p->syscall_result>=0)
					tracer_copy_out(p->tracer,list,POINTER(args[1]),size);
				else
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(list);
			}
			break;

		case SYSCALL64_setxattr:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path)); /* args[0] */
				char name[4096]; /* args[1] */
				void *value; /* args[2] */
				size_t size = args[3]; /* args[3] */
                int flags = args[4]; /* args[4] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));
				value = malloc(size);
				if (value == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}
                tracer_copy_in(p->tracer,value,POINTER(args[2]),size);

				p->syscall_result = pfs_setxattr(path,name,value,size,flags);
				if(p->syscall_result<0)
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(value);
			}
			break;

		case SYSCALL64_lsetxattr:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path)); /* args[0] */
				char name[4096]; /* args[1] */
				void *value; /* args[2] */
				size_t size = args[3]; /* args[3] */
                int flags = args[4]; /* args[4] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));
				value = malloc(size);
				if (value == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}
                tracer_copy_in(p->tracer,value,POINTER(args[2]),size);

				p->syscall_result = pfs_lsetxattr(path,name,value,size,flags);
				if(p->syscall_result<0)
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(value);
			}
			break;

		case SYSCALL64_fsetxattr:
			if(entering) {
				int fd = args[0]; /* args[0] */
				char name[4096]; /* args[1] */
				void *value; /* args[2] */
				size_t size = args[3]; /* args[3] */
                int flags = args[4]; /* args[4] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));
				value = malloc(size);
				if (value == NULL) {
				  divert_to_dummy(p,-ENOMEM);
				  break;
				}
                tracer_copy_in(p->tracer,value,POINTER(args[2]),size);

				p->syscall_result = pfs_fsetxattr(fd,name,value,size,flags);
				if(p->syscall_result<0)
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
				free(value);
			}
			break;

		case SYSCALL64_removexattr:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path)); /* args[0] */
				char name[4096]; /* args[1] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));

				p->syscall_result = pfs_removexattr(path,name);
				if(p->syscall_result<0)
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_lremovexattr:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path)); /* args[0] */
				char name[4096]; /* args[1] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));

				p->syscall_result = pfs_lremovexattr(path,name);
				if(p->syscall_result<0)
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fremovexattr:
			if(entering) {
				int fd = args[0]; /* args[0] */
				char name[4096]; /* args[1] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));

				p->syscall_result = pfs_fremovexattr(fd,name);
				if(p->syscall_result<0)
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/*
		These system calls could concievably be supported,
		but we haven't had the need or the time to attack
		them.  The user should know that we aren't handling them.
		*/

		case SYSCALL64_getpmsg:
		case SYSCALL64_io_cancel:
		case SYSCALL64_io_destroy:
		case SYSCALL64_io_getevents:
		case SYSCALL64_io_setup:
		case SYSCALL64_io_submit:
		case SYSCALL64_ptrace:
		case SYSCALL64_putpmsg:
		case SYSCALL64_readahead:
		case SYSCALL64_security:
		case SYSCALL64_sendfile:
			/* fallthrough */

		/*
		If anything else escaped our attention, we must know
		about it in an obvious way.
		*/

		default:
			if(entering) {
				debug(D_NOTICE,"warning: system call %d (%s) not supported for program %s",p->syscall,tracer_syscall_name(p->tracer,p->syscall),p->name);
				divert_to_dummy(p,-ENOSYS);
			}
			break;
	}

	if(!entering && p->state==PFS_PROCESS_STATE_KERNEL) {
		p->state = PFS_PROCESS_STATE_USER;
		if(p->syscall_args_changed) {
			tracer_args_set(p->tracer,p->syscall,p->syscall_args,TRACER_ARGS_MAX);
			p->syscall_args_changed = 0;
		}
		if(p->syscall_dummy) {
			tracer_result_set(p->tracer,p->syscall_result);
			p->syscall_dummy = 0;
			debug(D_SYSCALL,"= %d %s",p->syscall_result,p->syscall_result<0 ? strerror(-p->syscall_result) : "" );
		} else {
			debug(D_SYSCALL,"= ");
		}
	}

}

/*
Note that we clear the interrupted flag whenever
we start a new system call or leave an old one.
We don't want one system call to be interrupted
by a signal from a previous system call.
*/

void pfs_dispatch64( struct pfs_process *p, INT64_T signum )
{
	struct pfs_process *oldcurrent = pfs_current;
	pfs_current = p;

	switch(p->state) {
		case PFS_PROCESS_STATE_KERNEL:
		case PFS_PROCESS_STATE_WAITWRITE:
			decode_syscall(p,0);
			break;
		case PFS_PROCESS_STATE_USER:
			p->interrupted = 0;
		case PFS_PROCESS_STATE_WAITREAD:
			decode_syscall(p,1);
			break;
		case PFS_PROCESS_STATE_WAITPID:
		case PFS_PROCESS_STATE_DONE:
			p->interrupted = 0;
			break;
		default:
			debug(D_PROCESS,"process %d in unexpected state %d",p->pid,p->state);
			break;
	}

	switch(p->state) {
		case PFS_PROCESS_STATE_KERNEL:
		case PFS_PROCESS_STATE_USER:
			tracer_continue(p->tracer,signum);
			break;
		case PFS_PROCESS_STATE_WAITPID:
		case PFS_PROCESS_STATE_WAITREAD:
		case PFS_PROCESS_STATE_WAITWRITE:
		case PFS_PROCESS_STATE_DONE:
			break;
		default:
			debug(D_PROCESS,"process %d in unexpected state %d",p->pid,p->state);
			break;
	}

	pfs_current = oldcurrent;
}

#endif
