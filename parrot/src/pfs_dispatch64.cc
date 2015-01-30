/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef CCTOOLS_CPU_I386

#include "pfs_process.h"

int pfs_dispatch64( struct pfs_process *p )
{
	return 0;
}

#else /* CCTOOLS_CPU_I386 */

/* Must come first as other headers include the 32 bit version. */
#include "pfs_sysdeps64.h"

#include "linux-version.h"
#include "pfs_channel.h"
#include "pfs_dispatch.h"
#include "pfs_pointer.h"
#include "pfs_process.h"
#include "pfs_service.h"
#include "pfs_sys.h"

extern "C" {
#include "buffer.h"
#include "debug.h"
#include "int_sizes.h"
#include "macros.h"
#include "pattern.h"
#include "stringtools.h"
#include "tracer.h"
#include "xxmalloc.h"
}

#include <unistd.h>

#include <fcntl.h>

#include <sys/file.h>
#include <sys/mman.h>
#include <sys/personality.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/utsname.h>
#include <sys/wait.h>

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef EFD_CLOEXEC
#	define EFD_CLOEXEC 02000000
#endif
#ifndef EPOLL_CLOEXEC
#	define EPOLL_CLOEXEC 02000000
#endif
#ifndef FAN_CLOEXEC
#	define FAN_CLOEXEC 0x00000001
#endif
#ifndef FD_CLOEXEC
#	define FD_CLOEXEC 1
#endif
#ifndef F_DUPFD_CLOEXEC
#	define F_DUPFD_CLOEXEC 1030
#endif
#ifndef F_DUP2FD
#	define F_DUP2FD F_DUPFD
#endif
#ifndef IN_CLOEXEC
#	define IN_CLOEXEC 02000000
#endif
#ifndef O_CLOEXEC
#	define O_CLOEXEC 02000000
#endif
#ifndef MSG_CMSG_CLOEXEC
#	define MSG_CMSG_CLOEXEC 0x40000000
#endif
#ifndef PERF_FLAG_FD_CLOEXEC
#	define PERF_FLAG_FD_CLOEXEC (1UL << 3)
#endif
#ifndef SFD_CLOEXEC
#	define SFD_CLOEXEC 02000000
#endif
#ifndef SOCK_CLOEXEC
#	define SOCK_CLOEXEC 02000000
#endif
#ifndef TFD_CLOEXEC
#	define TFD_CLOEXEC 02000000
#endif

extern struct pfs_process *pfs_current;
extern char *pfs_temp_dir;
extern char *pfs_false_uname;
extern uid_t pfs_uid;
extern gid_t pfs_gid;

extern pid_t trace_this_pid;

extern INT64_T pfs_syscall_count;
extern INT64_T pfs_read_count;
extern INT64_T pfs_write_count;

extern int parrot_dir_fd;
extern int *pfs_syscall_totals64;

extern void handle_specific_process( pid_t pid );

#define POINTER( i ) ((void *)(uintptr_t)(i))

/*
Divert this incoming system call to a read or write on the I/O channel
*/

static void divert_to_channel( struct pfs_process *p, INT64_T syscall, const void *uaddr, size_t length, pfs_size_t channel_offset )
{
	INT64_T args[] = {pfs_channel_fd(), (INT64_T)(uintptr_t)uaddr, (INT64_T)length, channel_offset};
	debug(D_DEBUG, "divert_to_channel(%d, %s, %p, %zu, %" PRId64 ")", p->pid, tracer_syscall_name(p->tracer,syscall), uaddr, length, (INT64_T)channel_offset);
	debug(D_DEBUG, "--> %s(%" PRId64 ", 0x%" PRIx64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,syscall), args[0], args[1], args[2], args[3]);
	tracer_args_set(p->tracer,syscall,args,sizeof(args)/sizeof(args[0]));
	p->syscall_args_changed = 1;
	p->diverted_length = length;
	msync(pfs_channel_base()+channel_offset, length, MS_INVALIDATE|MS_ASYNC);
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

/* The point of this function is to make a nice readable path for
 * /proc/self/fd/[0-9]+ to make debugging easier. We could just as easily use a
 * static name like "p".
 */

#define MAX_PATHTOFILENAME 32
static void pathtofilename( char *path )
{
	char filename[PATH_MAX] = "pfs@";

	char *current = strchr(filename, '\0');
	const char *next = path;
	do {
		if (*next == '/') {
			*current++ = '-';
			while (*(next+1) == '/')
				next++; /* skip redundant slashes */
		} else {
			*current++ = *next;
		}
	} while (*next++);

	/* make it a reasonable (safer) size... */
	if (strlen(filename) >= MAX_PATHTOFILENAME) {
		snprintf(path, MAX_PATHTOFILENAME, "%.*s...%.*s", MAX_PATHTOFILENAME/2-2, filename, MAX_PATHTOFILENAME/2-2, filename+strlen(filename)-(MAX_PATHTOFILENAME/2-2));
	} else {
		strcpy(path, filename);
	}
}

/* The purpose of this is to allocate a unique file and use up an fd so it
 * isn't used in the future. We also need the inode # to get its unique
 * identifier.
 */

static void divert_to_parrotfd( struct pfs_process *p, INT64_T fd, char *path, const void *uaddr, int flags )
{
	pathtofilename(path);
	debug(D_DEBUG, "diverting to openat(%d, `%s', O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR)", parrot_dir_fd, path);
	INT64_T args[] = {parrot_dir_fd, (INT64_T)pfs_process_scratch_set(p, path, strlen(path)+1), O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR};
	if (flags & O_CLOEXEC)
		args[2] |= O_CLOEXEC;
	tracer_args_set(p->tracer,SYSCALL64_openat,args,sizeof(args)/sizeof(args[0]));
	p->syscall_args_changed = 1;
	p->syscall_parrotfd = fd;
	trace_this_pid = p->pid; /* this handles two processes racing to create the same file, also see comment for pfs_table::setparrot. */
}

static void handle_parrotfd( struct pfs_process *p )
{
	INT64_T actual;
	tracer_result_get(p->tracer, &actual);
	if (actual >= 0) {
		char path[PATH_MAX];
		struct stat buf;
		if (pfs_process_stat(p->pid, actual, &buf) == -1)
			fatal("could not stat %d: %s", actual, strerror(errno));
		p->table->setparrot(p->syscall_parrotfd, actual, &buf);
		pfs_process_scratch_get(p, path, sizeof(path));
		if (unlinkat(parrot_dir_fd, path, 0) == -1)
			fatal("could not unlink `%s': %s", path, strerror(errno));
	} else {
		/* could not allocate parrotfd? */
		debug(D_DEBUG, "could not allocate parrotfd: %s", strerror(-actual));
		pfs_close(p->syscall_parrotfd);
	}
	pfs_process_scratch_restore(p);
	p->syscall_parrotfd = -1;
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

static void decode_read( struct pfs_process *p, int entering, INT64_T syscall, const INT64_T *args )
{
	int fd = args[0];
	void *uaddr = POINTER(args[1]);
	pfs_size_t length = args[2];
	pfs_off_t offset = args[3];

	if(entering) {
		debug(D_DEBUG, "read(%" PRId64 ", %p, %" PRId64 ")", args[0], uaddr, args[2]);

		if(pfs_channel_alloc(0,length,&p->io_channel_offset)) {
			char *local_addr = pfs_channel_base() + p->io_channel_offset;

			if(syscall==SYSCALL64_read) {
				p->syscall_result = pfs_read(fd,local_addr,length);
			} else if(syscall==SYSCALL64_pread64) {
				p->syscall_result = pfs_pread(fd,local_addr,length,offset);
			}

			p->diverted_length = 0;

			if(p->syscall_result==0) {
				divert_to_dummy(p,0);
			} else if(p->syscall_result>0) {
				divert_to_channel(p,SYSCALL64_pread64,uaddr,p->syscall_result,p->io_channel_offset);
				pfs_read_count += p->syscall_result;
			} else {
				divert_to_dummy(p,-errno);
			}
		} else {
			divert_to_dummy(p,-ENOMEM);
		}
	} else if (!p->syscall_dummy) {
		INT64_T actual;
		tracer_result_get(p->tracer,&actual);
		debug(D_DEBUG, "channel read %" PRId64, actual);

		/*
		This is an ugly situation.
		If we arrive here with EINTR, it means that we have copied
		all of the data into the channel, taken any side effects
		of accessing the remote storage device, but the system call
		happened not to read it because of an incoming signal.
		We have no way of re-trying the channel read, so we do
		the ugly slow copy out instead.
		*/

		if(actual == -EINTR) {
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

static void decode_write( struct pfs_process *p, int entering, INT64_T syscall, const INT64_T *args )
{
	if(entering) {
		void *uaddr = POINTER(args[1]);
		INT64_T length = args[2];
		if(pfs_channel_alloc(0,length,&p->io_channel_offset)) {
			divert_to_channel(p,SYSCALL64_pwrite64,uaddr,length,p->io_channel_offset);
		} else {
			divert_to_dummy(p,-ENOMEM);
		}
	} else if (!p->syscall_dummy) {
		INT64_T actual;
		tracer_result_get(p->tracer,&actual);
		debug(D_DEBUG, "channel wrote %" PRId64, actual);

		if(actual>0) {
			int fd = args[0];
			pfs_off_t offset = args[3];
			char *local_addr = pfs_channel_base() + p->io_channel_offset;

			if(syscall==SYSCALL64_write) {
				p->syscall_result = pfs_write(fd,local_addr,actual);
			} else if(syscall==SYSCALL64_pwrite64) {
				p->syscall_result = pfs_pwrite(fd,local_addr,actual,offset);
			}

			if(p->syscall_result!=actual) {
				debug(D_SYSCALL,"write returned %"PRId64" instead of %"PRId64,p->syscall_result, actual);
			}

			if(p->syscall_result>=0)
				pfs_write_count += p->syscall_result;
			else
				p->syscall_result = -errno;
			tracer_result_set(p->tracer,p->syscall_result);
		}
		pfs_channel_free(p->io_channel_offset);
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
		tracer_copy_in(p->tracer,&buf[pos],POINTER(v[i].iov_base),v[i].iov_len);
		pos += v[i].iov_len;
	}
	return pos;
}

static int iovec_copy_out( struct pfs_process *p, void *buf, struct pfs_kernel_iovec *v, int count, size_t total )
{
	int i = 0;
	size_t current = 0;

	while (current < total) {
		if (v[i].iov_len <= (total-current)) {
			tracer_copy_out(p->tracer,((char *)buf)+current,POINTER(v[i].iov_base),v[i].iov_len);
			current += v[i].iov_len;
			i += 1;
		} else {
			tracer_copy_out(p->tracer,((char *)buf)+current,POINTER(v[i].iov_base),total-current);
			current += (total-current);
			assert(current == total);
		}
	}
	return current;
}

/*
Both readv and writev have a careful but inefficient implementation.
For each uio block, we examine the data structures, do a manual
read and write in our local buffer, and then copy the data over.
I assume that these are not heavily used system calls, although
they do seem to appear sporadically in X11, the dynamic linker,
and sporadically in networking utilities.
*/

static void decode_readv( struct pfs_process *p, int entering, INT64_T syscall, const INT64_T *args )
{
	if(entering) {
		int fd = args[0];
		struct pfs_kernel_iovec *uv = (struct pfs_kernel_iovec *) POINTER(args[1]);
		int count = args[2];

		struct pfs_kernel_iovec *v;
		int size;
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
					iovec_copy_out(p,buffer,v,count,result);
					divert_to_dummy(p,result);
				} else {
					divert_to_dummy(p,-errno);
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

static void decode_writev( struct pfs_process *p, int entering, INT64_T syscall, const INT64_T *args )
{
	if(entering) {
		int fd = args[0];
		struct pfs_kernel_iovec *uv = (struct pfs_kernel_iovec *) POINTER(args[1]);
		int count = args[2];

		struct pfs_kernel_iovec *v;
		int size;
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
					divert_to_dummy(p,-errno);
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

static void decode_stat( struct pfs_process *p, int entering, INT64_T syscall, const INT64_T *args )
{
	if(entering) {
		char path[PFS_PATH_MAX];
		struct pfs_stat lbuf;

		if(syscall==SYSCALL64_stat) {
			tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
			p->syscall_result = pfs_stat(path,&lbuf);
		} else if(syscall==SYSCALL64_lstat) {
			tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
			p->syscall_result = pfs_lstat(path,&lbuf);
		} else if(syscall==SYSCALL64_fstat) {
			p->syscall_result = pfs_fstat(args[0],&lbuf);
		}

		if(p->syscall_result>=0) {
			p->io_channel_offset = 0;
			if(pfs_channel_alloc(0,sizeof(struct pfs_kernel_stat),&p->io_channel_offset)) {
				struct pfs_kernel_stat kbuf;
				char *local_addr = pfs_channel_base() + p->io_channel_offset;
				COPY_STAT(lbuf,kbuf);
				memcpy(local_addr,&kbuf,sizeof(kbuf));
				divert_to_channel(p,SYSCALL64_pread64,POINTER(args[1]),sizeof(kbuf),p->io_channel_offset);
			} else {
				divert_to_dummy(p,-ENOMEM);
			}
		} else {
			divert_to_dummy(p,-errno);
		}
	} else if (!p->syscall_dummy) {
		INT64_T actual;
		tracer_result_get(p->tracer,&actual);
		debug(D_DEBUG, "channel read %" PRId64, actual);
		pfs_channel_free(p->io_channel_offset);
		tracer_result_set(p->tracer, 0);
	}
}

static void decode_statfs( struct pfs_process *p, int entering, INT64_T syscall, const INT64_T *args )
{
	if(entering) {
		struct pfs_statfs lbuf;

		if(syscall==SYSCALL64_statfs) {
			char path[PFS_PATH_MAX];
			tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
			p->syscall_result = pfs_statfs(path,&lbuf);
		} else if(syscall==SYSCALL64_fstatfs) {
			p->syscall_result = pfs_fstatfs(args[0],&lbuf);
		}

		if(p->syscall_result>=0) {
			struct pfs_kernel_statfs kbuf;
			p->io_channel_offset = 0;
			if(pfs_channel_alloc(0,sizeof(kbuf),&p->io_channel_offset)) {
				char *local_addr = pfs_channel_base() + p->io_channel_offset;
				COPY_STATFS(lbuf,kbuf);
				memcpy(local_addr,&kbuf,sizeof(kbuf));
				divert_to_channel(p,SYSCALL64_pread64,POINTER(args[1]),sizeof(kbuf),p->io_channel_offset);
			} else {
				divert_to_dummy(p,-ENOMEM);
			}
		} else {
			divert_to_dummy(p,-errno);
		}
	} else if (!p->syscall_dummy) {
		INT64_T actual;
		tracer_result_get(p->tracer,&actual);
		debug(D_DEBUG, "channel read %" PRId64, actual);
		pfs_channel_free(p->io_channel_offset);
		tracer_result_set(p->tracer, 0);
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

static int redirect_ldso( const char *exe, char *ldso_physical_name )
{
	extern char pfs_ldso_path[PFS_PATH_MAX];

	if (pfs_ldso_path[0] == '\0') {
		ldso_physical_name[0] = '\0';
		return 0;
	}

	debug(D_PROCESS, "redirect_ldso: called on %s", exe);

	if(pfs_get_local_name(pfs_ldso_path, ldso_physical_name, 0, 0) < 0) {
		debug(D_PROCESS, "redirect_ldso: cannot get physical name of %s", pfs_ldso_path);
		return errno = ENOENT, -1;
	}

	/* Unwise to check ldso recursively */
	if (strcmp(exe, ldso_physical_name) == 0) {
		ldso_physical_name[0] = '\0';
		return 0;
	}

	/* Test whether loading with ldso would work by running ldso --verify on
	 * the executable (may be static).
	 */

	pid_t child = fork();
	if (child == -1) {
		debug(D_PROCESS,"redirect_ldso: cannot fork: %s", strerror(errno));
		return errno = EIO, -1;
	} else if (child == 0) {
		int fd = open("/dev/null", O_RDWR);
		if (fd == -1)
			_exit(EXIT_FAILURE);
		dup2(fd, STDIN_FILENO);
		dup2(fd, STDOUT_FILENO);
		dup2(fd, STDERR_FILENO);
		execlp(ldso_physical_name, ldso_physical_name, "--verify", exe, NULL);
		_exit(EXIT_FAILURE);
	}

	int status;
	if (waitpid(child, &status, 0) == -1) {
		debug(D_PROCESS, "redirect_ldso: couldn't wait: %s", strerror(errno));
		kill(child, SIGKILL);
		return errno = EIO, -1;
	}
	if (!WIFEXITED(status)) {
		debug(D_PROCESS,"redirect_ldso: %s --verify %s didn't exit normally. status == %d", ldso_physical_name, exe, status);
		return errno = EIO, -1;
	}
	switch (WEXITSTATUS(status)) {
		case 0:
			debug(D_PROCESS, "redirect_ldso: will execute %s %s", ldso_physical_name, exe);
			return 0;
		case 1:
			debug(D_DEBUG, "redirect_ldso: %s is probably a static binary and will be executed directly", exe);
			ldso_physical_name[0] = '\0';
			return 0;
		default:
			debug(D_PROCESS,"redirect_ldso: %s --verify %s exited with status %d", ldso_physical_name, exe, WEXITSTATUS(status));
			return errno = EIO, -1;
	}
}

static int fix_execve ( struct pfs_process *p, uintptr_t old_user_argv, const char *physical_exe, const char *logical_exe, const char *replace_arg0, const char *arg1, const char *arg2 )
{
	uintptr_t scratch = pfs_process_scratch_address(p);

	/* XXX
	 *
	 * physical_exe is only used if we are doing execve directly without
	 * wrapping with ldso, it becomes the exe argument for:
	 *
	 * execve(exe, argv, envp)
	 *
	 * The reason for that is the *kernel* needs to know the physical name. On
	 * the other hand, ld-linux is a regular executable that will allow us to
	 * do another translation of logical_exe -> physical_exe. XXX This is
	 * important to get argv[0] right!!! XXX ld-linux replaces argv[0] with
	 * its first argument!
	 */

	char ldso[PFS_PATH_MAX] = "";
	if (redirect_ldso(physical_exe, ldso) == -1)
		return -1;

	/* "exe" + '\0' + [new "arg0" + '\0' + ... ] + padding + new argv array */
	buffer_t B;
	buffer_init(&B);
	buffer_abortonfailure(&B, 1);

	/* if we redirect ldso, then logical_exe becomes arg1 and ldso becomes exe */
	uintptr_t user_ldso = buffer_pos(&B)+scratch;
	if (ldso[0])
		buffer_putlstring(&B, ldso, strlen(ldso)+1);

	uintptr_t user_exe = buffer_pos(&B)+scratch;
	if (ldso[0]) {
		buffer_putlstring(&B, logical_exe, strlen(logical_exe)+1);
	} else {
		buffer_putlstring(&B, physical_exe, strlen(physical_exe)+1);
	}

	/* if we are changing arg0, then the user provided arg0 is replaced */
	uintptr_t user_arg0 = buffer_pos(&B)+scratch;
	if (replace_arg0)
		buffer_putlstring(&B, replace_arg0, strlen(replace_arg0)+1);

	uintptr_t user_arg1 = buffer_pos(&B)+scratch;
	if (arg1)
		buffer_putlstring(&B, arg1, strlen(arg1)+1);

	uintptr_t user_arg2 = buffer_pos(&B)+scratch;
	if (arg2)
		buffer_putlstring(&B, arg2, strlen(arg2)+1);

	/* align the upcoming argv array */
	{
		uint64_t dummy = 0;
		size_t padding = ALIGN(uint64_t, buffer_pos(&B))-buffer_pos(&B);
		assert(padding <= sizeof(dummy));
		buffer_putlstring(&B, (char *)&dummy, padding);
	}

	uintptr_t user_argv = buffer_pos(&B)+scratch;
	if (replace_arg0) {
		debug(D_DEBUG, "replacing argv0: `%s'", replace_arg0);
		buffer_putlstring(&B, (char *)&user_arg0, sizeof(user_arg0));
	} else {
		uintptr_t old_user_argv0;
		if (tracer_copy_in(p->tracer, &old_user_argv0, POINTER(old_user_argv), sizeof(old_user_argv0)) == -1) {
			buffer_free(&B);
			return errno = EFAULT, -1;
		}
		buffer_putlstring(&B, (char *)&old_user_argv0, sizeof(old_user_argv0));
	}
	if (ldso[0]) {
		debug(D_DEBUG, "wrapping execution with ldso, argv[1]: `%s'", logical_exe);
		buffer_putlstring(&B, (char *)&user_exe, sizeof(user_exe)); /* exe is arg1 when wrapped by ldso */
	}
	if (arg1) {
		debug(D_DEBUG, "argv[next]: `%s'", arg1);
		buffer_putlstring(&B, (char *)&user_arg1, sizeof(user_arg1));
	}
	if (arg2) {
		debug(D_DEBUG, "argv[next]: `%s'", arg2);
		buffer_putlstring(&B, (char *)&user_arg2, sizeof(user_arg2));
	}
	/* copy in the rest of the user argv array... */
	old_user_argv += sizeof(uintptr_t); /* skip user argv[0] */
	while (1) {
		size_t i;
		uintptr_t user_argva[1024];
		tracer_copy_in(p->tracer, user_argva, POINTER(old_user_argv), sizeof(user_argva));
		for (i = 0; i < sizeof(user_argva)/sizeof(uintptr_t) && user_argva[i]; i++, old_user_argv += sizeof(uintptr_t))
			buffer_putlstring(&B, (char *)&user_argva[i], sizeof(user_argva[i]));
		if (i < sizeof(user_argva)/sizeof(uintptr_t))
			break;
	}
	{
		uintptr_t sentinel = 0;
		buffer_putlstring(&B, (char *)&sentinel, sizeof(sentinel));
	}

#if 0
	for (size_t i = 0; i < buffer_pos(&B); i+=sizeof(uintptr_t))
		debug(D_DEBUG, "%" PRIx64 " %016" PRIx64, (uint64_t)i+scratch, *(uintptr_t *)(buffer_tostring(&B)+i));
#endif

	if (buffer_pos(&B) > PFS_SCRATCH_SPACE) {
		debug(D_NOTICE, "cannot handle too many arguments for `%s'", logical_exe);
		buffer_free(&B);
		return errno = E2BIG, -1;
	}

	pfs_process_scratch_set(p, buffer_tostring(&B), buffer_pos(&B));
	buffer_free(&B);

	/* change the registers to reflect argv */
	INT64_T nargs[] = {(INT64_T) (ldso[0] ? user_ldso : user_exe), (INT64_T)user_argv};
	tracer_args_set(p->tracer,p->syscall,nargs,sizeof(nargs)/sizeof(nargs[0]));
	p->syscall_args_changed = 1;
	return 0;
}

/*
Several things to note about exec.

An entry to execve looks like a normal syscall. An exit from execve indicates a
successful execve in progress.

Now, we cannot execute the path named by the execve directly.  It must be
resolved through PFS, because our idea of the current dir (or even the meaning
of the name) may be quite different.  We resolve the file name into a local
path, perhaps by pulling it into the cache.

In the simple (second) case, we copy the new local name into the address space
of the process and exec that instead.  If the exec fails, we must restore the
changed bytes afterwards.

In the complex (first) case, the program contains a pound-bang indicating an
interpreter.  We instead resolve the interpreter as the executable and fiddle
around with the job's argv to indicate that.  Then, we do much the same as the
first case.
*/

static void decode_execve( struct pfs_process *p, int entering, INT64_T syscall, const INT64_T *args )
{
	if(entering) {
		char logical_name[PFS_PATH_MAX] = "";
		char physical_name[PFS_PATH_MAX] = "";
		char firstline[PFS_PATH_MAX] = "";
		char *interp_exe = NULL, *interp_arg = NULL;
		const uintptr_t old_user_argv = args[1];

		tracer_copy_in_string(p->tracer,logical_name,POINTER(args[0]),sizeof(logical_name));
		strncpy(p->new_logical_name, logical_name, sizeof(p->new_logical_name)-1);

		if(!is_executable(logical_name))
			goto failure;

		if (pfs_get_local_name(logical_name,physical_name,firstline,sizeof(firstline))<0)
			goto failure;

		/* force to single line: */
		{
			char *c = strchr(firstline, '\n');
			if (c)
				*c = 0;
		}

		if(pattern_match(firstline, "^#!%s*(%S+)%s*(.-)%s*$", &interp_exe, &interp_arg) >= 0) {
			debug(D_PROCESS, "execve: %s (%s) is an interpreted executable", p->new_logical_name, physical_name);
			if (strlen(interp_arg))
				debug(D_PROCESS, "execve: instead do %s \"%s\" %s", interp_exe, interp_arg, logical_name);
			else
				debug(D_PROCESS, "execve: instead do %s %s", interp_exe, logical_name);

			/* make sure the new interp_exe is loaded */
			strcpy(p->new_logical_name, interp_exe);
			if (pfs_get_local_name(interp_exe, physical_name, 0, 0) < 0)
				goto failure;

			if (strlen(interp_arg)) {
				if (fix_execve(p, old_user_argv, physical_name, interp_exe, interp_exe, interp_arg, logical_name) == -1)
					goto failure;
			} else {
				if (fix_execve(p, old_user_argv, physical_name, interp_exe, interp_exe, logical_name, NULL) == -1)
					goto failure;
			}
		} else {
			debug(D_PROCESS, "execve: %s (%s) is an ordinary executable", p->new_logical_name, physical_name);
			if (fix_execve(p, old_user_argv, physical_name, logical_name, NULL, NULL, NULL) == -1)
				goto failure;
		}

		/* This forces the next call to return to decode_execve, see comment at top of decode_syscall */
		p->completing_execve = 1;

		debug(D_PROCESS,"execve: %s about to start", p->new_logical_name);
		goto done;
failure:
		divert_to_dummy(p, -errno);
done:
		free(interp_exe);
		free(interp_arg);
	} else if (p->syscall_dummy) {
		debug(D_PROCESS, "execve: failed: %s", strerror(-p->syscall_result));
	} else /* That is, we are not entering */ {
		INT64_T actual;
		tracer_result_get(p->tracer,&actual);

		p->completing_execve = 0;
		if (actual == 0) {
			debug(D_PROCESS, "execve: %s succeeded in 64-bit mode", p->new_logical_name);
			strcpy(p->name, p->new_logical_name);
			/* We do not need to restore the scratch space as the process image has been replaced. */
		} else {
			debug(D_PROCESS, "execve: failed: %s", strerror(-actual));
			pfs_process_scratch_restore(p);
		}
	}
}

/*
Memory mapped files are loaded into the channel,
the whole file regardless of what portion is actually
mapped.  The channel cache keeps a reference count.
*/

static void decode_mmap( struct pfs_process *p, int entering, const INT64_T *args )
{
	INT64_T addr = args[0];
	pfs_size_t length = args[1];
	INT64_T prot = args[2];
	INT64_T flags = args[3];
	int fd = args[4];
	pfs_size_t source_offset = args[5];

	if (entering)
		debug(D_SYSCALL,"mmap addr=0x%"PRIx64" len=0x%"PRIx64" prot=0x%"PRIx64" flags=0x%"PRIx64" fd=%d offset=0x%"PRIx64,addr,length,prot,flags,fd,source_offset);

	if(p->table->isnative(fd)) {
		if (entering)
			debug(D_DEBUG, "fallthrough mmap on native fd");
		return;
	} else if (flags&MAP_ANONYMOUS) {
		if (entering)
			debug(D_SYSCALL,"mmap skipped b/c anonymous");
		return;
	} else if(entering) {
		INT64_T nargs[] = {args[0], args[1], args[2], args[3], args[4], args[5]};

		pfs_size_t channel_offset = pfs_mmap_create(fd,source_offset,length,prot,flags);
		if(channel_offset<0) {
			divert_to_dummy(p,-errno);
			return;
		}

		nargs[3] = flags & ~MAP_DENYWRITE;
		nargs[4] = pfs_channel_fd();
		nargs[5] = channel_offset+source_offset;

		debug(D_SYSCALL,"channel_offset=0x%"PRIx64" source_offset=0x%"PRIx64" total=0x%"PRIx64,channel_offset,source_offset,nargs[5]);
		debug(D_SYSCALL,"mmap changed: fd=%"PRId64" offset=0x%"PRIx64,nargs[4],nargs[5]);

		tracer_args_set(p->tracer,p->syscall,nargs,6);
		p->syscall_args_changed = 1;
	} else if(!p->syscall_dummy) {
		/*
		On exit from the system call, retrieve the logical address of
		the mmap as returned to the application.  Then, update the
		mmap record that corresponds to the proper channel offset.
		On failure, we must unmap the object, which will have a logical
		address of zero because it was never set.
		*/

		tracer_result_get(p->tracer,&p->syscall_result);

		if(p->syscall_result!=-1) {
			pfs_mmap_update(p->syscall_result,0);
		} else {
			pfs_mmap_delete(0,0);
		}
	}
}

static void decode_syscall( struct pfs_process *p, int entering )
{
	const INT64_T *args;

	char path[PFS_PATH_MAX];
	char path2[PFS_PATH_MAX];

	/* SYSCALL_execve has a different value in 32 and 64 bit modes. When an
	 * execve forces a switch between execution modes, the old system call
	 * number is retained, even though the mode has changed.  So, we must
	 * explicitly check for this condition and fix up the system call number to
	 * end up in the right code.
	 */
	if(p->completing_execve) {
		if (p->syscall != SYSCALL64_execve) {
			debug(D_PROCESS, "Changing execve code number from 32 to 64 bit mode.\n");
			p->syscall = SYSCALL64_execve;
		}
		p->completing_execve = 0;
	}

	if(entering) {
		p->state = PFS_PROCESS_STATE_KERNEL;
		p->syscall_dummy = 0;
		tracer_args_get(p->tracer,&p->syscall,p->syscall_args);

		debug(D_SYSCALL,"%s",tracer_syscall_name(p->tracer,p->syscall));
		p->syscall_original = p->syscall;
		pfs_syscall_count++;

#if 0 /* enable for extreme debugging */
		{
			DIR *D;
			struct dirent *dirent;
			char fds[PATH_MAX];
			snprintf(fds, sizeof(fds), "/proc/%d/fd", p->pid);
			D = opendir(fds);
			if (D) {
				while ((dirent = readdir(D))) {
					if (!(strcmp(dirent->d_name, ".") == 0 || strcmp(dirent->d_name, "..") == 0)) {
						char resolved[PATH_MAX] = "";
						struct stat buf;
						readlinkat(dirfd(D), dirent->d_name, resolved, sizeof(resolved)-1);
						fstatat(dirfd(D), dirent->d_name, &buf, 0);
						debug(D_DEBUG, "%s -> %s (%d)", dirent->d_name, resolved, (int)buf.st_ino);
					}
				}
				closedir(D);
			}
		}
#endif

		if(pfs_syscall_totals64) {
			int s = p->syscall;
			if(s>=0 && s<SYSCALL64_MAX) {
				pfs_syscall_totals64[p->syscall]++;
			}
		}
	}

	args = p->syscall_args;

	/* To get syscalls not in this switch:
		(getsyscalls() { grep "$1" | grep -oE 'SYSCALL64_[[:alnum:]_]+' | grep -v MAX | sort | uniq; };  cat <(getsyscalls 'case SYSCALL64' < pfs_dispatch64.cc) <(getsyscalls '#define SYS' < tracer.table64.h)) | sort | uniq -c | grep '1 SYSCALL' | awk '{printf "\t\tcase %s:\n", $2}'
	*/
	switch(p->syscall) {
		/* A wide variety of calls have no relation to file access, so we
		 * simply send them along to the underlying OS.
		 */

		case SYSCALL64__sysctl:
		case SYSCALL64_adjtimex:
		case SYSCALL64_afs_syscall:
		case SYSCALL64_alarm:
		case SYSCALL64_arch_prctl:
		case SYSCALL64_brk:
		case SYSCALL64_capget:
		case SYSCALL64_capset:
		case SYSCALL64_clock_getres:
		case SYSCALL64_clock_gettime:
		case SYSCALL64_clock_nanosleep:
		case SYSCALL64_clock_settime:
		case SYSCALL64_create_module:
		case SYSCALL64_delete_module:
		case SYSCALL64_exit:
		case SYSCALL64_exit_group:
		case SYSCALL64_futex:
		case SYSCALL64_get_kernel_syms:
		case SYSCALL64_get_robust_list:
		case SYSCALL64_get_thread_area:
		case SYSCALL64_getcpu:
		case SYSCALL64_getgroups:
		case SYSCALL64_getitimer:
		case SYSCALL64_getpgid:
		case SYSCALL64_getpgrp:
		case SYSCALL64_getpid:
		case SYSCALL64_getppid:
		case SYSCALL64_getpriority:
		case SYSCALL64_getrandom:
		case SYSCALL64_getrlimit:
		case SYSCALL64_getrusage:
		case SYSCALL64_getsid:
		case SYSCALL64_gettid:
		case SYSCALL64_gettimeofday:
		case SYSCALL64_init_module:
		case SYSCALL64_ioperm:
		case SYSCALL64_iopl:
		case SYSCALL64_kcmp:
		case SYSCALL64_madvise:
		case SYSCALL64_migrate_pages:
		case SYSCALL64_mincore:
		case SYSCALL64_mlock:
		case SYSCALL64_mlockall:
		case SYSCALL64_modify_ldt:
		case SYSCALL64_move_pages:
		case SYSCALL64_mprotect:
		case SYSCALL64_mremap:
		case SYSCALL64_msync:
		case SYSCALL64_munlock:
		case SYSCALL64_munlockall:
		case SYSCALL64_nanosleep:
		case SYSCALL64_pause:
		case SYSCALL64_prctl:
		case SYSCALL64_prlimit64:
		case SYSCALL64_process_vm_readv:
		case SYSCALL64_process_vm_writev:
		case SYSCALL64_query_module:
		case SYSCALL64_quotactl:
		case SYSCALL64_reboot:
		case SYSCALL64_restart_syscall:
		case SYSCALL64_rt_sigaction:
		case SYSCALL64_rt_sigpending:
		case SYSCALL64_rt_sigprocmask:
		case SYSCALL64_rt_sigqueueinfo:
		case SYSCALL64_rt_sigreturn:
		case SYSCALL64_rt_sigsuspend:
		case SYSCALL64_rt_sigtimedwait:
		case SYSCALL64_sched_get_priority_max:
		case SYSCALL64_sched_get_priority_min:
		case SYSCALL64_sched_getaffinity:
		case SYSCALL64_sched_getattr:
		case SYSCALL64_sched_getparam:
		case SYSCALL64_sched_getscheduler:
		case SYSCALL64_sched_rr_get_interval:
		case SYSCALL64_sched_setaffinity:
		case SYSCALL64_sched_setattr:
		case SYSCALL64_sched_setparam:
		case SYSCALL64_sched_setscheduler:
		case SYSCALL64_sched_yield:
		case SYSCALL64_set_robust_list:
		case SYSCALL64_set_thread_area:
		case SYSCALL64_set_tid_address:
		case SYSCALL64_setdomainname:
		case SYSCALL64_setgroups:
		case SYSCALL64_sethostname:
		case SYSCALL64_setitimer:
		case SYSCALL64_setpgid:
		case SYSCALL64_setpriority:
		case SYSCALL64_setrlimit:
		case SYSCALL64_setsid:
		case SYSCALL64_settimeofday:
		case SYSCALL64_shmat:
		case SYSCALL64_shmctl:
		case SYSCALL64_shmdt:
		case SYSCALL64_shmget:
		case SYSCALL64_sigaltstack:
		case SYSCALL64_swapoff:
		case SYSCALL64_swapon:
		case SYSCALL64_sync:
		case SYSCALL64_sysinfo:
		case SYSCALL64_syslog:
		case SYSCALL64_time:
		case SYSCALL64_timer_create:
		case SYSCALL64_timer_delete:
		case SYSCALL64_timer_getoverrun:
		case SYSCALL64_timer_gettime:
		case SYSCALL64_timer_settime:
		case SYSCALL64_times:
		case SYSCALL64_ustat:
		case SYSCALL64_vhangup:
		case SYSCALL64_wait4:
		case SYSCALL64_waitid:
			break;

		case SYSCALL64_execve:
			decode_execve(p,entering,p->syscall,args);
			break;

		case SYSCALL64_vfork:
		case SYSCALL64_fork:
		case SYSCALL64_clone:
			if(entering) {
				/* Once a fork is started, we must trace only that pid so that
				 * we can determine the child pid before seeing any events from
				 * the child.
				 */
				trace_this_pid = p->pid;
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
						fatal("cannot execute program with personality %lu", persona);
				}
			}
			break;

		case SYSCALL64_kill:
		case SYSCALL64_tkill:
			if(entering) {
				debug(D_PROCESS, "%s(%d, %d)", tracer_syscall_name(p->tracer, p->syscall), (int)args[0], (int)args[1]);
				if (pfs_process_cankill(args[0]) == -1)
					divert_to_dummy(p, -errno);
			}
			break;

		case SYSCALL64_tgkill:
			if(entering) {
				debug(D_PROCESS, "tgkill(%d, %d, %d)", (int)args[0], (int)args[1], (int)args[2]);
				if (pfs_process_cankill(args[1]) == -1)
					divert_to_dummy(p, -errno);
			}
			break;

		case SYSCALL64_umask:
			/* We need to track the umask ourselves and use it in open. */
			if(entering)
				pfs_current->umask = args[0] & 0777;
			break;

		case SYSCALL64_geteuid:
		case SYSCALL64_getuid:
			/* Always return the dummy uids. */
			if (entering)
				divert_to_dummy(p,pfs_uid);
			break;

		case SYSCALL64_getegid:
		case SYSCALL64_getgid:
			if (entering)
				divert_to_dummy(p,pfs_gid);
			break;

		case SYSCALL64_getresuid:
			if (entering) {
				tracer_copy_out(p->tracer,&pfs_uid,POINTER(args[0]),sizeof(pfs_uid));
				tracer_copy_out(p->tracer,&pfs_uid,POINTER(args[1]),sizeof(pfs_uid));
				tracer_copy_out(p->tracer,&pfs_uid,POINTER(args[2]),sizeof(pfs_uid));
				divert_to_dummy(p,0);
			}
			break;

		case SYSCALL64_getresgid:
			if (entering) {
				tracer_copy_out(p->tracer,&pfs_gid,POINTER(args[0]),sizeof(pfs_uid));
				tracer_copy_out(p->tracer,&pfs_gid,POINTER(args[1]),sizeof(pfs_uid));
				tracer_copy_out(p->tracer,&pfs_gid,POINTER(args[2]),sizeof(pfs_uid));
				divert_to_dummy(p,0);
			}
			break;

		/* Changing the userid is not allow, but for completeness, you can
		 * always change to your own uid.
		 */

		case SYSCALL64_setfsgid:
		case SYSCALL64_setfsuid:
		case SYSCALL64_setgid:
		case SYSCALL64_setregid:
		case SYSCALL64_setresgid:
		case SYSCALL64_setresuid:
		case SYSCALL64_setreuid:
		case SYSCALL64_setuid:
			if (entering)
				divert_to_dummy(p,0);
			break;

		/* Here begin all of the I/O operations, given in the same order as in
		 * pfs_table.  Notice that most operations use the simple but slow
		 * tracer_copy_{in,out} routines. When performance is important
		 * (write,mmap), we resort to redirection I/O to the side channel.
		 */

		/* File descriptor creation */

		case SYSCALL64_open:
		case SYSCALL64_creat:
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				if(strlen(path) == 0) {
					divert_to_dummy(p, -ENOENT);
					break;
				}

				int flags;
				int mode;
				char native_path[PATH_MAX];
				if (p->syscall == SYSCALL64_creat) {
					flags = O_CREAT|O_WRONLY|O_TRUNC;
					mode = args[1];
				} else if (p->syscall == SYSCALL64_open) {
					flags = args[1];
					mode = args[2];
				} else assert(0);

				p->syscall_result = pfs_open(path,flags,mode,native_path,sizeof(native_path));

				if(p->syscall_result == -1) {
					divert_to_dummy(p, -errno);
				} else if(p->syscall_result == -2 /* canbenative */) {
					INT64_T nargs[] = {(INT64_T)pfs_process_scratch_set(p, native_path, strlen(native_path)+1), flags, mode};
					tracer_args_set(p->tracer,SYSCALL64_open,nargs,sizeof(nargs)/sizeof(nargs[0]));
					p->syscall_args_changed = 1;
				} else {
					divert_to_parrotfd(p,p->syscall_result,path,POINTER(args[0]),flags);
				}
			} else if (p->syscall_parrotfd >= 0) {
				handle_parrotfd(p);
			} else if (p->syscall_args_changed) {
				/* native fd */
				INT64_T actual;
				tracer_result_get(p->tracer, &actual);
				if (actual >= 0) {
					int fdflags = 0;
					if (p->syscall == SYSCALL64_open && args[1] & O_CLOEXEC) {
						fdflags |= FD_CLOEXEC;
					}
					p->table->setnative(actual, fdflags);
				}
				pfs_process_scratch_restore(p);
			}
			break;

		case SYSCALL64_dup3:
		case SYSCALL64_dup2:
			if (entering) {
				if (p->table->isspecial(args[1])) {
					divert_to_dummy(p, -EIO); /* best errno we can give */
				} else if (!p->table->isvalid(args[1])) {
					divert_to_dummy(p, -EBADF);
				}
			}
			/* fallthrough */
		case SYSCALL64_dup:
			if (!entering && !p->syscall_dummy) {
				INT64_T actual;
				tracer_result_get(p->tracer, &actual);
				if (actual >= 0 && actual != args[0]) {
					if (p->syscall == SYSCALL64_dup3 && (args[2]&O_CLOEXEC))
						p->table->dup2(args[0], actual, FD_CLOEXEC);
					else
						p->table->dup2(args[0], actual, 0);
				}
			}
			break;

		case SYSCALL64_accept:
		case SYSCALL64_epoll_create1:
		case SYSCALL64_epoll_create:
		case SYSCALL64_eventfd2:
		case SYSCALL64_eventfd:
		case SYSCALL64_memfd_create:
		case SYSCALL64_perf_event_open:
		case SYSCALL64_pipe2:
		case SYSCALL64_pipe:
		case SYSCALL64_signalfd:
		case SYSCALL64_signalfd4:
		case SYSCALL64_socket:
		case SYSCALL64_socketpair:
		case SYSCALL64_timerfd_create:
			if (entering) {
				debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else {
				INT64_T actual;
				tracer_result_get(p->tracer, &actual);
				if (actual >= 0) {
					if (p->syscall == SYSCALL64_socketpair || p->syscall == SYSCALL64_pipe || p->syscall == SYSCALL64_pipe2) {
						int fds[2];
						int fdflags = 0;
						if (p->syscall == SYSCALL64_socketpair)
							tracer_copy_in(p->tracer, fds, POINTER(args[3]), sizeof(fds));
						else if (p->syscall == SYSCALL64_pipe || p->syscall == SYSCALL64_pipe2)
							tracer_copy_in(p->tracer, fds, POINTER(args[0]), sizeof(fds));
						else assert(0);
						if (p->syscall == SYSCALL64_pipe2 && (args[1]&O_CLOEXEC)) {
							fdflags |= FD_CLOEXEC;
						} else if (p->syscall == SYSCALL64_socketpair && (args[1]&SOCK_CLOEXEC)) {
							fdflags |= FD_CLOEXEC;
						}
						assert(fds[0] >= 0);
						p->table->setnative(fds[0], fdflags);
						assert(fds[1] >= 0);
						p->table->setnative(fds[1], fdflags);
					} else if (p->syscall == SYSCALL64_epoll_create1 && (args[1]&EPOLL_CLOEXEC)) {
						p->table->setnative(actual, FD_CLOEXEC);
					} else if (p->syscall == SYSCALL64_eventfd2 && (args[1]&EFD_CLOEXEC)) {
						p->table->setnative(actual, FD_CLOEXEC);
					} else if (p->syscall == SYSCALL64_perf_event_open && (args[2]&PERF_FLAG_FD_CLOEXEC)) {
						p->table->setnative(actual, FD_CLOEXEC);
					} else if (p->syscall == SYSCALL64_signalfd4 && (args[2]&SFD_CLOEXEC)) {
						p->table->setnative(actual, FD_CLOEXEC);
					} else if (p->syscall == SYSCALL64_socket && (args[1]&SOCK_CLOEXEC)) {
						p->table->setnative(actual, FD_CLOEXEC);
					} else if (p->syscall == SYSCALL64_timerfd_create && (args[1]&TFD_CLOEXEC)) {
						p->table->setnative(actual, FD_CLOEXEC);
					} else {
						p->table->setnative(actual, 0);
					}
				}
			}
			break;

		/* operations on open files */

		/* Although pfs_table supports the high-level operations
		 * opendir/readdir/closedir, all we can get a hold of at this level is
		 * getdents, which works on an open file descriptor.  We copy dirents
		 * out one by one using fdreaddir, and transform them into the type
		 * expected by the kernel. If we overrun the available buffer,
		 * immediately seek the fd back to where it was before.
		 */

		case SYSCALL64_getdents:
		case SYSCALL64_getdents64:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
				INT64_T fd = args[0];
				uintptr_t uaddr = args[1];
				size_t length = args[2];

				BUFFER_STACK_ABORT(B, (1<<16)+1);
				length = MIN(length, 1<<16);

				struct dirent *d;
				errno = 0;
				while((d = pfs_fdreaddir(fd))) {
					uint64_t ino = d->d_ino;
					uint64_t off = d->d_off;
					uint16_t reclen;
					const char *name = d->d_name;
					uint8_t type = d->d_type;
					reclen = sizeof(ino) + sizeof(off) + sizeof(reclen) + strlen(name) + 1 /* NUL */ + /* padding + */ sizeof(type);
					size_t padding = ALIGN(uint64_t, reclen)-reclen;
					reclen += padding;

					if(reclen>length) {
						pfs_lseek(fd,d->d_off,SEEK_SET);
						errno = EINVAL;
						break;
					}

					int rc = 0;
					rc += buffer_putlstring(&B, (char *)&ino, sizeof(ino));
					rc += buffer_putlstring(&B, (char *)&off, sizeof(off));
					rc += buffer_putlstring(&B, (char *)&reclen, sizeof(reclen));
					rc += buffer_putstring(&B, name);
					rc += buffer_putliteral(&B, "\0"); /* NUL terminator for d_name */
					rc += buffer_putlstring(&B, "\0\0\0\0\0\0\0\0", padding); /* uint64_t alignment padding */
					rc += buffer_putlstring(&B, (char *)&type, sizeof(type));
					assert(rc == (int)reclen);
					length -= rc;
				}

				if (buffer_pos(&B)) {
					tracer_copy_out(p->tracer,buffer_tostring(&B),POINTER(uaddr),buffer_pos(&B));
					divert_to_dummy(p, buffer_pos(&B));
				} else {
					divert_to_dummy(p, -errno);
				}
			}
			break;

		case SYSCALL64_close:
			if (p->table->isnative(args[0])) {
				if (entering) {
					debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
					pfs_close(args[0]);
				}
				/* fall through so it closes the Parrot fd */
			} else {
				if (entering) {
					p->syscall_result = pfs_close(args[0]);
					if(p->syscall_result<0)
						divert_to_dummy(p, -errno);
					else
						p->syscall_dummy = 1; /* Fake a dummy "return" (so p->syscall_result is returned) but allow the kernel to close the Parrot fd. */
				}
			}
			break;

		case SYSCALL64_read:
		case SYSCALL64_pread64:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else {
				decode_read(p,entering,p->syscall,args);
			}
			break;

		case SYSCALL64_write:
		case SYSCALL64_pwrite64:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else {
				decode_write(p,entering,p->syscall,args);
			}
			break;

		case SYSCALL64_readv:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else {
				decode_readv(p,entering,p->syscall,args);
			}
			break;

		case SYSCALL64_writev:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else {
				decode_writev(p,entering,p->syscall,args);
			}
			break;


		/* bind and connect are symmetric... */
		case SYSCALL64_bind:
		case SYSCALL64_connect:
			if(entering) {
				p->syscall_result = 0;

				if (args[2] <= 0) {
					divert_to_dummy(p, -EINVAL);
					break;
				}

				/* Note that sockaddr is a class of structures. Linux requires
				 * the generic fields for any address family to be first and
				 * the same for all address families, although the name of the
				 * field will differ (e.g. addr.sun_family for AF_UNIX vs.
				 * addr.sin_family for AF_INET). We can use the common field of
				 * type sa_family_t named sun_family to check for AF_UNIX. Only
				 * AF_UNIX sockets can be bound to a filename.
				 */

				struct sockaddr_un addr;
				memset(&addr, 0, sizeof(addr));
				INT64_T len = tracer_copy_in(p->tracer, &addr, POINTER(args[1]), MIN(sizeof(addr),(size_t)args[2]));
				if (len <= (INT64_T)sizeof(addr.sun_family)) {
					divert_to_dummy(p, -EINVAL);
					break;
				}
				addr.sun_path[sizeof(addr.sun_path)-1] = '\0';

				if (addr.sun_family == AF_UNIX) {
					assert(sizeof(p->tmp) >= sizeof(addr));
					memcpy(p->tmp, &addr, sizeof(addr)); /* save a copy of original addr structure */

					p->syscall_result = p->table->bind(args[0], addr.sun_path, sizeof(addr.sun_path));
					if (p->syscall_result == -1) {
						divert_to_dummy(p, -errno);
						break;
					}

					p->syscall_result = 1;
					tracer_copy_out(p->tracer, &addr, POINTER(args[1]), sizeof(addr)); /* fix the path */
					/* let the kernel perform the bind/connect... */
				} else {
					/* We only care about AF_UNIX sockets. */
					debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
				}
			} else if (!p->syscall_dummy && p->syscall_result == 1) {
				/* We aren't changing/reading the *actual* result, we're just restoring the tracee's addr structure. */
				struct sockaddr_un addr;
				memcpy(&addr, p->tmp, sizeof(addr));
				tracer_copy_out(p->tracer, &addr, POINTER(args[1]), sizeof(addr)); /* restore the original path */
				p->syscall_result = 0; /* no actual effect... */
			}
			break;

		case SYSCALL64_recvmsg:
		case SYSCALL64_sendmsg:
			if (entering && !p->table->isnative(args[0])) {
				divert_to_dummy(p,-EBADF);
				break;
			} else if (p->syscall_dummy) {
				break;
			}

			/* There is a situation where a process sends a file descriptor and
			 * the receiver "discards" it unknowingly by doing a read instead
			 * of recvmsg. We can't account for that since the kernel
			 * completely hides that from us. So, it's possible an in-flight
			 * Parrot file descriptor causes a reference increase on a file
			 * pointer but never a decrease. It's also possible that a Parrot
			 * fd is sent to a process that is not a tracee. Not only can we
			 * not see the recvmsg and decrement the file pointer counter but
			 * that process receives a garbage "Parrot file" too.
			 */

			if (!entering)
				tracer_result_get(p->tracer, &p->syscall_result);

			/* We only care if the process is sending or has received an fd. */
			if ((entering && p->syscall == SYSCALL64_sendmsg) || (!entering && p->syscall == SYSCALL64_sendmsg && p->syscall_result < 0) || (!entering && p->syscall == SYSCALL64_recvmsg && p->syscall_result > 0)) {
				struct msghdr umsg;
				struct msghdr msg;

				/* Copy in parts of msghdr structure we want. */
				tracer_copy_in(p->tracer,&umsg,POINTER(args[1]),sizeof(umsg));

				if(umsg.msg_control && umsg.msg_controllen>0) {
					msg.msg_control = xxmalloc(umsg.msg_controllen);
					msg.msg_controllen = umsg.msg_controllen;
					tracer_copy_in(p->tracer,msg.msg_control,POINTER(umsg.msg_control),umsg.msg_controllen);
				} else {
					msg.msg_control = 0;
					msg.msg_controllen = 0;
				}

				/* FIXME handle MSG_CMSG_CLOEXEC */
				for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
					if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
						int *fd = (int *)CMSG_DATA(cmsg);
						do {
							if (p->syscall == SYSCALL64_recvmsg) {
								p->table->recvfd(p->pid, *fd);
							} else if (p->syscall == SYSCALL64_sendmsg) {
								if (entering) {
									p->table->sendfd(*fd, 0);
								} else if (p->syscall_result < 0) {
									p->table->sendfd(*fd, 1);
								} else assert(0);
							} else assert(0);
							fd += 1;
						} while (((socklen_t)((uintptr_t)fd - (uintptr_t)cmsg + sizeof(int))) <= cmsg->cmsg_len);
					} else if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_CREDENTIALS) {
						/* process id of sender */
					}
				}

				/* Delete the msghdr structure */
				free(msg.msg_control);
			}
			break;

		case SYSCALL64_epoll_ctl:
		case SYSCALL64_epoll_ctl_old:
		case SYSCALL64_epoll_wait:
		case SYSCALL64_epoll_wait_old:
		case SYSCALL64_epoll_pwait:
		case SYSCALL64_timerfd_gettime:
		case SYSCALL64_timerfd_settime:
			if (entering) {
				if (p->table->isparrot(args[0])) {
					divert_to_dummy(p,-EINVAL); /* You'd be suprised what you can live through... */
				} else if (!p->table->isnative(args[0])) {
					divert_to_dummy(p,-EBADF);
				}
			}
			break;

		case SYSCALL64_getpeername:
		case SYSCALL64_getsockname:
		case SYSCALL64_getsockopt:
		case SYSCALL64_listen:
		case SYSCALL64_recvfrom:
		case SYSCALL64_sendto:
		case SYSCALL64_setsockopt:
		case SYSCALL64_shutdown:
			if (entering) {
				if (p->table->isparrot(args[0])) {
					divert_to_dummy(p,-ENOTSOCK); /* You'd be suprised what you can live through... */
				} else if (!p->table->isnative(args[0])) {
					divert_to_dummy(p,-EBADF);
				}
			}
			break;

		case SYSCALL64_ioctl:
			if (entering) {
				if (p->table->isparrot(args[0])) {
					divert_to_dummy(p,-ENOTTY);
				} else if (!p->table->isnative(args[0])) {
					divert_to_dummy(p,-EBADF);
				}
			}
			break;

		case SYSCALL64_poll:
		case SYSCALL64_ppoll:
		case SYSCALL64_pselect6:
		case SYSCALL64_select:
			/* For select and poll, we don't need to hook these because
			 * sockets/pipes which the tracee might select/poll on are always
			 * native fds. Any "Parrot fd" is a regular file, so select/poll on
			 * those will always return "ready" for read/write/error. This is what
			 * we want anyway, so we don't need to do anything special.
			 *
			 * If however we *did* want to do something special, we could do
			 * something like sending one of a new socketpair to the tracee. We can
			 * perhaps send one of a new socketpair to the tracee. We can use the
			 * recvmsg system call to receive the socket via an existent socket we
			 * setup ahead of time.
			 */
			break;

		/* Now we have a series of standard file operations that only use the
		 * integer arguments, and are (mostly) easily passed back and forth.
		 */

		case SYSCALL64_lseek:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if(entering) {
				p->syscall_result = pfs_lseek(args[0],args[1],args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_ftruncate:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if(entering) {
				p->syscall_result = pfs_ftruncate(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fstat:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else {
				decode_stat(p,entering,SYSCALL64_fstat,args);
			}
			break;

		case SYSCALL64_fstatfs:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else {
				decode_statfs(p,entering,SYSCALL64_fstatfs,args);
			}
			break;

		case SYSCALL64_flock:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
				p->syscall_result = pfs_flock(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fsync:
		case SYSCALL64_fdatasync:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
				p->syscall_result = pfs_fsync(args[0]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchdir:
			if (p->table->isnative(args[0])) {
				divert_to_dummy(p,-EACCES); /* We do not need to allow this because all open directories are not native fds. */
			} else if (entering) {
				p->syscall_result = pfs_fchdir(args[0]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchown:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
				p->syscall_result = pfs_fchown(args[0],args[1],args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchmod:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
				p->syscall_result = pfs_fchmod(args[0],args[1]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/* See comment above SYSCALL64_getxattr. */

		case SYSCALL64_fgetxattr:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
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

		case SYSCALL64_flistxattr:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
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

		case SYSCALL64_fsetxattr:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
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

		case SYSCALL64_fremovexattr:
			if (p->table->isnative(args[0])) {
				if (entering) debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
			} else if (entering) {
				int fd = args[0]; /* args[0] */
				char name[4096]; /* args[1] */

				tracer_copy_in_string(p->tracer,name,POINTER(args[1]),sizeof(name));

				p->syscall_result = pfs_fremovexattr(fd,name);
				if(p->syscall_result<0)
					p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/* fcntl operations are rather generic and operate on the file table
		 * itself.  These things we can parse, understand, and pass along to
		 * the file table in most cases. We permit the user to set the O_ASYNC
		 * flag and thus receive activity notification via SIGIO.  However, we
		 * don't yet support extended signal information.
		 */

		case SYSCALL64_fcntl:
			/* special case for fcntl dup */
			if (args[1] == F_DUPFD || args[1] == F_DUP2FD || args[1] == F_DUPFD_CLOEXEC) {
				if (entering) {
					if (p->table->isspecial(args[2])) {
						divert_to_dummy(p, -EIO); /* best errno we can give */
					} else if (!p->table->isvalid(args[2])) {
						divert_to_dummy(p, -EBADF);
					}
				} else if (!p->syscall_dummy) {
					INT64_T actual;
					tracer_result_get(p->tracer, &actual);
					if (actual >= 0 && actual != args[0]) {
						if (args[1] == F_DUPFD_CLOEXEC) {
							p->table->dup2(args[0], actual, FD_CLOEXEC);
						} else {
							p->table->dup2(args[0], actual, 0);
						}
					}
				}
			} else if (p->table->isnative(args[0])) {
				if (entering) {
					debug(D_DEBUG, "fallthrough %s(%" PRId64 ", %" PRId64 ", %" PRId64 ")", tracer_syscall_name(p->tracer,p->syscall), args[0], args[1], args[2]);
				} else {
					/* Handle the application marking FD_CLOEXEC. */
					INT64_T actual;
					tracer_result_get(p->tracer, &actual);
					if (actual >= 0) {
						if (args[1] == F_SETFD) {
							debug(D_DEBUG, "updating native fd %d flags to %d", (int)args[0], (int)args[2]);
							p->table->setnative(args[0], args[2]);
						}
					}
				}
			} else if (entering) {
				pid_t pid;
				int fd = args[0];
				int cmd = args[1];
				void *uaddr = POINTER(args[2]);

				switch(cmd) {
					case F_GETFD:
					case F_SETFD:
						p->syscall_result = pfs_fcntl(fd,cmd,uaddr);
						if(p->syscall_result<0)
							divert_to_dummy(p,-errno);
						/* allow the kernel to also set fd flags (e.g. FD_CLOEXEC) */
						break;

					case F_GETFL:
					case F_SETFL:
						p->syscall_result = pfs_fcntl(fd,cmd,uaddr);
						if(p->syscall_result<0) p->syscall_result=-errno;
						divert_to_dummy(p,p->syscall_result);

						if(cmd==F_SETFL) {
							int flags = (int)args[2];
							if(flags&O_ASYNC) {
								debug(D_PROCESS,"pid %d requests O_ASYNC on fd %d",(int)pfs_current->pid,fd);
								p->flags |= PFS_PROCESS_FLAGS_ASYNC;
							}
						}
						break;

					case PFS_GETLK:
					case PFS_SETLK:
					case PFS_SETLKW: {
						struct flock fl;
						tracer_copy_in(p->tracer,&fl,uaddr,sizeof(fl));
						p->syscall_result = pfs_fcntl(fd,cmd,&fl);
						if(p->syscall_result<0) {
							p->syscall_result=-errno;
						} else {
							tracer_copy_out(p->tracer,&fl,uaddr,sizeof(fl));
						}
						divert_to_dummy(p,p->syscall_result);
						break;
					}

					/* Pretend that the caller is the signal recipient */
					case F_GETOWN:
						divert_to_dummy(p,p->pid);
						break;

					/* But we always get the signal. */
					case F_SETOWN:
						debug(D_PROCESS,"pid %d requests F_SETOWN on fd %d",pfs_current->pid,fd);
						p->flags |= PFS_PROCESS_FLAGS_ASYNC;
						pid = getpid();
						pfs_fcntl(fd,F_SETOWN,POINTER(pid));
						divert_to_dummy(p,0);
						break;

					default:
						divert_to_dummy(p,-ENOSYS);
						break;
				}
			}
			break;

		case SYSCALL64_mmap:
			decode_mmap(p,entering,args);
			break;

		/* For unmap, we update our internal records for what is unmapped,
		 * which may cause a flush of dirty data. However, we do not divert the
		 * system call, because we still want the real mapping undone in the
		 * process.
		 */

		case SYSCALL64_munmap:
			if(entering) {
				pfs_mmap_delete(args[0],args[1]);
			}
			break;

		/* Next, we have operations that do not modify any files in particular,
		 * but change the state of the file table within the process in
		 * question.
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

		/* Next we have all of the system calls that work on a file name,
		 * rather than an open file.  In most cases, we use the (fast)
		 * tracer_copy_in to fetch the file name, and then invoke the pfs_X.
		 *
		 * XXX We should have some sort of bounds checking on the path name.
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
					p->syscall_result = MIN(p->syscall_result, args[2]);
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

		/* *at system calls */

		case SYSCALL64_openat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				if(strlen(path) == 0) {
					divert_to_dummy(p, -ENOENT);
					break;
				}

				char native_path[PATH_MAX];
				p->syscall_result = pfs_openat(args[0],path,args[2],args[3],native_path,sizeof(native_path));

				if(p->syscall_result == -1) {
					divert_to_dummy(p, -errno);
				} else if(p->syscall_result == -2 /* canbenative */) {
					INT64_T nargs[] = {(INT64_T)pfs_process_scratch_set(p, native_path, strlen(native_path)+1), args[2], args[3]};
					tracer_args_set(p->tracer,SYSCALL64_open,nargs,sizeof(nargs)/sizeof(nargs[0]));
					p->syscall_args_changed = 1;
				} else {
					divert_to_parrotfd(p,p->syscall_result,path,POINTER(args[1]),args[2]);
				}
			} else if (p->syscall_parrotfd >= 0) {
				handle_parrotfd(p);
			} else if (p->syscall_args_changed) {
				/* native fd */
				INT64_T actual;
				tracer_result_get(p->tracer, &actual);
				if (actual >= 0) {
					int fdflags = 0;
					if (args[2] & O_CLOEXEC) {
						fdflags |= FD_CLOEXEC;
					}
					p->table->setnative(actual, fdflags);
				}
				pfs_process_scratch_restore(p);
			}
			break;

		case SYSCALL64_mkdirat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_mkdirat(args[0],path,args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_mknodat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_mknodat(args[0],path,args[2],args[3]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchownat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_fchownat(args[0],path,args[2],args[3],args[4]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_futimesat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
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
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
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
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_unlinkat(args[0],path,args[2]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_renameat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[3]),sizeof(path2));
				p->syscall_result = pfs_renameat(args[0],path,args[2],path2);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}

			break;
		case SYSCALL64_linkat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[3]),sizeof(path2));
				p->syscall_result = pfs_linkat(args[0],path,args[2],path2,args[4]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_symlinkat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				tracer_copy_in_string(p->tracer,path2,POINTER(args[2]),sizeof(path2));
				p->syscall_result = pfs_symlinkat(path,args[1],path2);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_readlinkat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_readlinkat(args[0],path,path2,sizeof(path2));
				if(p->syscall_result<0) {
					p->syscall_result = -errno;
				} else {
					p->syscall_result = MIN(p->syscall_result, args[3]);
					tracer_copy_out(p->tracer,path2,POINTER(args[2]),p->syscall_result);
				}
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_fchmodat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
			if(entering) {
				tracer_copy_in_string(p->tracer,path,POINTER(args[1]),sizeof(path));
				p->syscall_result = pfs_fchmodat(args[0],path,args[2],args[3]);
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_faccessat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
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

		case SYSCALL64_utimensat:
			if (entering && p->table->isnative(args[0])) {
				/* The only way a process has a native fd directory is it it
				 * receives it from an external process not being traced, via
				 * recvmsg. This is not allowed.
				 */
				divert_to_dummy(p, -ENOTDIR);
				break;
			}
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

		/* Parrot system calls. */

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

		case SYSCALL64_parrot_search:
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
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		case SYSCALL64_parrot_md5:
			if(entering) {
				char digest[16];
				tracer_copy_in_string(p->tracer,path,POINTER(args[0]),sizeof(path));
				p->syscall_result = pfs_md5(path,(unsigned char*)digest);
				if(p->syscall_result>=0)
					tracer_copy_out(p->tracer,digest,POINTER(args[1]),sizeof(digest));
				if(p->syscall_result<0) p->syscall_result = -errno;
				divert_to_dummy(p,p->syscall_result);
			}
			break;

		/* These things are not currently permitted.
		 */

		case SYSCALL64_chroot:
		case SYSCALL64_lookup_dcookie:
		case SYSCALL64_mount:
		case SYSCALL64_remap_file_pages:
		case SYSCALL64_sysfs:
		case SYSCALL64_umount2:
		case SYSCALL64_uselib:
			if (entering)
				divert_to_dummy(p,-EPERM);
			break;

		/* These system calls are historical artifacts or otherwise not
		 * necessary to support.
		 */

		case SYSCALL64_acct:
		case SYSCALL64_fadvise64:
			if (entering)
				divert_to_dummy(p,-ENOSYS);
			break;

		case SYSCALL64_getpmsg:
		case SYSCALL64_putpmsg:
		case SYSCALL64_readahead:
			if (entering && !p->table->isnative(args[0])) {
				divert_to_dummy(p,-ENOSYS);
			}
			break;

		/* These system calls could concievably be supported, but we haven't
		 * had the need or the time to attack them.  The user should know that
		 * we aren't handling them.
		 */

		case SYSCALL64_accept4:
		case SYSCALL64_add_key:
		case SYSCALL64_clock_adjtime:
		case SYSCALL64_fallocate:
		case SYSCALL64_fanotify_init:
		case SYSCALL64_fanotify_mark:
		case SYSCALL64_finit_module:
		case SYSCALL64_get_mempolicy:
		case SYSCALL64_inotify_add_watch:
		case SYSCALL64_inotify_init:
		case SYSCALL64_inotify_init1:
		case SYSCALL64_inotify_rm_watch:
		case SYSCALL64_io_cancel:
		case SYSCALL64_io_destroy:
		case SYSCALL64_io_getevents:
		case SYSCALL64_ioprio_get:
		case SYSCALL64_ioprio_set:
		case SYSCALL64_io_setup:
		case SYSCALL64_io_submit:
		case SYSCALL64_kexec_file_load:
		case SYSCALL64_kexec_load:
		case SYSCALL64_keyctl:
		case SYSCALL64_mbind:
		case SYSCALL64_mq_getsetattr:
		case SYSCALL64_mq_notify:
		case SYSCALL64_mq_open:
		case SYSCALL64_mq_timedreceive:
		case SYSCALL64_mq_timedsend:
		case SYSCALL64_mq_unlink:
		case SYSCALL64_msgctl:
		case SYSCALL64_msgget:
		case SYSCALL64_msgrcv:
		case SYSCALL64_msgsnd:
		case SYSCALL64_name_to_handle_at:
		case SYSCALL64_nfsservctl:
		case SYSCALL64_open_by_handle_at:
		case SYSCALL64_pivot_root:
		case SYSCALL64_preadv:
		case SYSCALL64_ptrace:
		case SYSCALL64_pwritev:
		case SYSCALL64_recvmmsg:
		case SYSCALL64_renameat2:
		case SYSCALL64_request_key:
		case SYSCALL64_rt_tgsigqueueinfo:
		case SYSCALL64_seccomp:
		case SYSCALL64_security:
		case SYSCALL64_semctl:
		case SYSCALL64_semget:
		case SYSCALL64_semop:
		case SYSCALL64_semtimedop:
		case SYSCALL64_sendfile:
		case SYSCALL64_sendmmsg:
		case SYSCALL64_set_mempolicy:
		case SYSCALL64_setns:
		case SYSCALL64_splice:
		case SYSCALL64_sync_file_range:
		case SYSCALL64_syncfs:
		case SYSCALL64_tee:
		case SYSCALL64_tuxcall:
		case SYSCALL64_unshare:
		case SYSCALL64_vmsplice:
		case SYSCALL64_vserver:
			/* fallthrough */

		/* If anything else escaped our attention, we must know about it in an
		 * obvious way.
		 */

		default:
			if(entering) {
				debug(D_NOTICE,"warning: system call %" PRId64 " (%s) not supported for program %s", p->syscall, tracer_syscall_name(p->tracer, p->syscall), p->name);
				divert_to_dummy(p,-ENOSYS);
			}
			break;
	}

	if(!entering && p->state==PFS_PROCESS_STATE_KERNEL) {
		p->state = PFS_PROCESS_STATE_USER;
		if(p->syscall_dummy) {
			tracer_args_set(p->tracer,p->syscall,p->syscall_args,TRACER_ARGS_MAX); /* restore original system call */
			tracer_result_set(p->tracer,p->syscall_result);
			p->syscall_dummy = 0;
		} else {
			tracer_result_get(p->tracer,&p->syscall_result);
			if(p->syscall_args_changed) {
				tracer_args_set(p->tracer,p->syscall,p->syscall_args,TRACER_ARGS_MAX); /* restore original system call */
				tracer_result_set(p->tracer,p->syscall_result);
				p->syscall_args_changed = 0;
			}
		}
		if (p->syscall_result >= 0)
			debug(D_SYSCALL, "= %" PRId64 " [%s]",p->syscall_result,tracer_syscall_name(p->tracer,p->syscall));
		else
			debug(D_SYSCALL, "= %" PRId64 " %s [%s]",p->syscall_result,strerror(-p->syscall_result),tracer_syscall_name(p->tracer,p->syscall));
	}
}

void pfs_dispatch64( struct pfs_process *p )
{
	struct pfs_process *oldcurrent = pfs_current;
	pfs_current = p;

	switch(p->state) {
		case PFS_PROCESS_STATE_KERNEL:
			decode_syscall(p,0);
			break;
		case PFS_PROCESS_STATE_USER:
			p->nsyscalls += 1;
			decode_syscall(p,1);
			break;
		default:
			assert(0);
	}

	switch(p->state) {
		case PFS_PROCESS_STATE_KERNEL:
		case PFS_PROCESS_STATE_USER:
			tracer_continue(p->tracer,0);
			break;
		default:
			assert(0);
	}

	pfs_current = oldcurrent;
}

#endif

/* vim: set noexpandtab tabstop=4: */
