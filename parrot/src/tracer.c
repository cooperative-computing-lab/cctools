/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* Included with Parrot... */
#include "linux-version.h"
#include "ptrace.h"
#include "tracer.h"

#include "buffer.h"
#include "debug.h"
#include "full_io.h"
#include "macros.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <fcntl.h>
#include <syscall.h>
#include <unistd.h>

#include <sys/wait.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define FATAL fatal("tracer: %d %s",t->pid,strerror(errno));

#define ERROR \
	do {\
		debug(D_DEBUG, "%s (%s:%d): ptrace error: %s", __func__, __FILE__, __LINE__, strerror(errno));\
		return -1;\
	} while (0)

#define VOID_MATH(p,op) ((void *)(((uintptr_t)p)op))

#ifndef PTRACE_OLDSETOPTIONS
#	define PTRACE_OLDSETOPTIONS 21
#endif

#include "tracer.table.c"
#include "tracer.table64.c"

/*
Note that we would normally get such register definitions
from the system header files.  However, we want this code
to be able to compile cleanly on either i386 OR x86_64 and
possibly even support both i386 and x86_64 binaries at the
same time from either platform.  So, we write our own definitions
so we can be independent of the system headers.
*/

struct i386_registers {
	INT32_T ebx, ecx, edx, esi, edi, ebp, eax;
	INT16_T ds, __ds, es, __es;
	INT16_T fs, __fs, gs, __gs;
	INT32_T orig_eax, eip;
	INT16_T cs, __cs;
	INT32_T eflags, esp;
	INT16_T ss, __ss;
};

struct x86_64_registers {
	INT64_T r15,r14,r13,r12,rbp,rbx,r11,r10;
	INT64_T r9,r8,rax,rcx,rdx,rsi,rdi,orig_rax;
	INT64_T rip,cs,eflags;
	INT64_T rsp,ss;
	INT64_T fs_base, gs_base;
	INT64_T ds,es,fs,gs;
};

struct tracer {
	pid_t pid;
	int gotregs;
	int setregs;
	union {
		struct i386_registers regs32;
		struct x86_64_registers regs64;
	} regs;
	int has_args5_bug;
};

int tracer_attach (pid_t pid)
{
	intptr_t options = PTRACE_O_TRACESYSGOOD|PTRACE_O_TRACEEXEC|PTRACE_O_TRACEEXIT|PTRACE_O_TRACECLONE|PTRACE_O_TRACEFORK|PTRACE_O_TRACEVFORK;

	if (linux_available(3,8,0))
		options |= PTRACE_O_EXITKILL;
	assert(linux_available(2,5,60));

	if (linux_available(3,4,0)) {
		/* So this is a really annoying situation, in order to correctly deal
		 * with group-stops with ptrace, we must use PTRACE_SEIZE.  For a full
		 * explanation, see pfs_main.cc where PTRACE_LISTEN is used.
		 */
		if (ptrace(PTRACE_SEIZE, pid, 0, (void *)options) == -1)
			ERROR;
	} else {
		int status;
		if (ptrace(PTRACE_ATTACH, pid, 0, 0) == -1)
			ERROR;
		/* wait for the signal-delivery-stop for the PTRACE_ATTACH bootstrap SIGSTOP */
		if (waitpid(pid, &status, WUNTRACED|__WALL) == -1)
			ERROR;
		assert(WIFSTOPPED(status) && WSTOPSIG(status) == SIGSTOP);
		/* The SIGSTOP will be supressed in the upcoming PTRACE_SYSCALL */
		if (linux_available(2,6,0)) {
			if (ptrace(PTRACE_SETOPTIONS, pid, 0, (void *)options) == -1)
				ERROR;
		} else {
			if (ptrace(PTRACE_OLDSETOPTIONS, pid, 0, (void *)options) == -1)
				ERROR;
		}
		/* suppress delivery */
		if (ptrace(PTRACE_SYSCALL, pid, NULL, (void *)0) == -1)
			ERROR;
	}

	return 0;
}

int tracer_listen (struct tracer *t)
{
	if(t->setregs) {
		if(ptrace(PTRACE_SETREGS,t->pid,0,&t->regs) == -1)
			return -1;
		t->setregs = 0;
	}
	if (linux_available(3,4,0)) {
		if (ptrace(PTRACE_LISTEN, t->pid, NULL, NULL) == -1)
			ERROR;
	} else {
		/* This version of Linux does not allow transparently listening for wake up from group-stop. We have no choice but to restart it... */
		if (ptrace(PTRACE_SYSCALL, t->pid, NULL, 0) == -1)
			ERROR;
	}
	return 0;
}

struct tracer *tracer_init (pid_t pid)
{
	struct tracer *t = malloc(sizeof(*t));
	if(!t) return 0;

	t->pid = pid;
	t->gotregs = 0;
	t->setregs = 0;
	t->has_args5_bug = 0;

	memset(&t->regs,0,sizeof(t->regs));

	return t;
}

int tracer_getevent( struct tracer *t, unsigned long *message )
{
	if (ptrace(PTRACE_GETEVENTMSG, t->pid, 0, message) == -1)
		ERROR;
	return 0;
}

int tracer_is_64bit( struct tracer *t )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs) == -1)
			return -1;
		t->gotregs = 1;
	}

	if(t->regs.regs64.cs==0x33) {
		return 1;
	} else {
		return 0;
	}
}

void tracer_detach( struct tracer *t )
{
	if(t->setregs) {
		ptrace(PTRACE_SETREGS,t->pid,0,&t->regs); /* ignore failure */
		t->setregs = 0;
	}
	ptrace(PTRACE_DETACH,t->pid,0,0); /* ignore failure */
	free(t);
}

int tracer_continue( struct tracer *t, int signum )
{
	t->gotregs = 0;
	if(t->setregs) {
		if(ptrace(PTRACE_SETREGS,t->pid,0,&t->regs) == -1)
			return -1;
		t->setregs = 0;
	}
	if (ptrace(PTRACE_SYSCALL,t->pid,0,signum) == -1)
		ERROR;
	return 0;
}

int tracer_args_get( struct tracer *t, INT64_T *syscall, INT64_T args[TRACER_ARGS_MAX] )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs) == -1)
			ERROR;
		t->gotregs = 1;
	}

#ifdef CCTOOLS_CPU_I386
	*syscall = t->regs.regs32.orig_eax;
	args[0] = t->regs.regs32.ebx;
	args[1] = t->regs.regs32.ecx;
	args[2] = t->regs.regs32.edx;
	args[3] = t->regs.regs32.esi;
	args[4] = t->regs.regs32.edi;
	args[5] = t->regs.regs32.ebp;
#else
	if(tracer_is_64bit(t)) {
		*syscall = t->regs.regs64.orig_rax;
#if 0 /* Enable this for extreme debugging... */
		debug(D_DEBUG, "rax = %d; -%d is -ENOSYS (rax == -ENOSYS indicates syscall-enter-stop)", (int)t->regs.regs64.rax, ENOSYS);
#endif
		args[0] = t->regs.regs64.rdi;
		args[1] = t->regs.regs64.rsi;
		args[2] = t->regs.regs64.rdx;
		args[3] = t->regs.regs64.r10;
		args[4] = t->regs.regs64.r8;
		args[5] = t->regs.regs64.r9;
	} else {
		*syscall = t->regs.regs64.orig_rax;
		args[0] = t->regs.regs64.rbx;
		args[1] = t->regs.regs64.rcx;
		args[2] = t->regs.regs64.rdx;
		args[3] = t->regs.regs64.rsi;
		args[4] = t->regs.regs64.rdi;
		if (t->has_args5_bug) args[5] = t->regs.regs64.r9;
		else                  args[5] = t->regs.regs64.rbp;
	}
#endif
#if 0 /* Enable this for extreme debugging... */
	debug(D_DEBUG, "GET args[] = {%"PRId64", %"PRId64", %"PRId64", %"PRId64", %"PRId64", %"PRId64"}", args[0], args[1], args[2], args[3], args[4], args[5]);
#endif

	return 1;
}

void tracer_has_args5_bug( struct tracer *t )
{
	// Due to a widely-deployed bug in Linux
	// ptrace, rbp is corrupted and r9 is incidentally correct,
	// when tracing a 32-bit program on a 64-bit machine.
	// See: http://lkml.org/lkml/2007/1/31/317

	t->has_args5_bug = 1;
}

int tracer_args_set( struct tracer *t, INT64_T syscall, const INT64_T *args, int nargs )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs) == -1)
			ERROR;
		t->gotregs = 1;
	}

#if 0 /* Enable this for extreme debugging... */
	{
		int i;
		buffer_t B;
		buffer_init(&B);
		buffer_putfstring(&B, "SET args[%zu] = {", (size_t)nargs);
		for (i = 0; i < nargs; i++)
			buffer_putfstring(&B, "%" PRId64 ", ", args[i]);
		buffer_putliteral(&B, "}");
		debug(D_DEBUG, "%s", buffer_tostring(&B, NULL));
		buffer_free(&B);
	}
#endif

#ifdef CCTOOLS_CPU_I386
	t->regs.regs32.orig_eax = syscall;
	if(nargs>=1) t->regs.regs32.ebx = args[0];
	if(nargs>=2) t->regs.regs32.ecx = args[1];
	if(nargs>=3) t->regs.regs32.edx = args[2];
	if(nargs>=4) t->regs.regs32.esi = args[3];
	if(nargs>=5) t->regs.regs32.edi = args[4];
	if(nargs>=6) t->regs.regs32.ebp = args[5];
#else
	if(tracer_is_64bit(t)) {
		t->regs.regs64.orig_rax = syscall;
		if(nargs>=1) t->regs.regs64.rdi = args[0];
		if(nargs>=2) t->regs.regs64.rsi = args[1];
		if(nargs>=3) t->regs.regs64.rdx = args[2];
		if(nargs>=4) t->regs.regs64.r10 = args[3];
		if(nargs>=5) t->regs.regs64.r8  = args[4];
		if(nargs>=6) t->regs.regs64.r9  = args[5];
	} else {
		t->regs.regs64.orig_rax = syscall;
		if(nargs>=1) t->regs.regs64.rbx = args[0];
		if(nargs>=2) t->regs.regs64.rcx = args[1];
		if(nargs>=3) t->regs.regs64.rdx = args[2];
		if(nargs>=4) t->regs.regs64.rsi = args[3];
		if(nargs>=5) t->regs.regs64.rdi  = args[4];
		if(nargs>=6) {
			if (t->has_args5_bug) t->regs.regs64.r9 = args[5];
			else                  t->regs.regs64.rbp = args[5];
		}
	}
#endif

	t->setregs = 1;

	return 1;
}

int tracer_result_get( struct tracer *t, INT64_T *result )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs) == -1)
			ERROR;
		t->gotregs = 1;
	}

#ifdef CCTOOLS_CPU_I386
	*result = t->regs.regs32.eax;
#else
	*result = t->regs.regs64.rax;
#endif

	return 1;
}

int tracer_result_set( struct tracer *t, INT64_T result )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs) == -1)
			ERROR;
		t->gotregs = 1;
	}

#ifdef CCTOOLS_CPU_I386
	t->regs.regs32.eax = result;
#else
	t->regs.regs64.rax = result;
#endif

	t->setregs = 1;

	return 1;
}

int tracer_stack_get( struct tracer *t, uintptr_t *ptr )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs) == -1)
			return -1;
		t->gotregs = 1;
	}

#ifdef CCTOOLS_CPU_I386
	*ptr = t->regs.regs32.esp;
#else
	*ptr = t->regs.regs64.rsp;
#endif

	return 0;
}

/*
Be careful here:
Note that the amount of data moved around in a PEEKDATA or POKEDATA
depends on the word size of the *caller*, not of the process
being traced.  Thus, a 64-bit Parrot will always move eight bytes
in and out of the target process.  We use a long here because
that represents the natural type of the target platform.
*/

static ssize_t tracer_copy_out_slow( struct tracer *t, const void *data, const void *uaddr, size_t length, int flags )
{
	const uint8_t *bdata = data;
	const uint8_t *buaddr = uaddr;
	size_t size = length;
	long word;

	/* first, copy whole words */

	while(size>=sizeof(word)) {
		word = *(long*)bdata;
		errno = 0;
		if (ptrace(PTRACE_POKEDATA,t->pid,buaddr,word) == -1 && errno) {
			size_t total = length-size;
			if (!(flags & TRACER_O_ATOMIC) && total) {
				return total;
			} else {
				ERROR;
			}
		}
		size -= sizeof(word);
		buaddr += sizeof(word);
		bdata += sizeof(word);
	}

	/* if necessary, copy the last few bytes */

	if(size>0) {
		errno = 0;
		if ((word = ptrace(PTRACE_PEEKDATA,t->pid,buaddr,0)) == -1 && errno) {
			size_t total = length-size;
			if (!(flags & TRACER_O_ATOMIC) && total) {
				return total;
			} else {
				ERROR;
			}
		}
		memcpy(&word,bdata,size);
		errno = 0;
		if (ptrace(PTRACE_POKEDATA,t->pid,buaddr,word) == -1 && errno) {
			size_t total = length-size;
			if (!(flags & TRACER_O_ATOMIC) && total) {
				return total;
			} else {
				ERROR;
			}
		}
	}

	return length;
}

static ssize_t copy_out_fast( struct tracer *t, const void *data, const void *uaddr, size_t length, int flags )
{
	int i;
	size_t pgsize = (size_t)getpagesize();
	struct iovec local;
	struct iovec remote[IOV_MAX];
	size_t rn;
	size_t count;
	size_t written = 0;

	if (!linux_available(3,2,0))
		return errno = ENOSYS, -1;

more:
	local.iov_base = (void *)data;
	local.iov_len = length;

	if (flags & TRACER_O_ATOMIC) {
		/* do the whole write */
		count = length;
	} else {
		/* first (partial) page */
		count = MIN(pgsize-(((uintptr_t)uaddr)&(pgsize-1)), length);
	}
	remote[0].iov_base = (void *)uaddr;
	remote[0].iov_len = count;
	uaddr = VOID_MATH(uaddr, +count);
	length -= count;
	rn = 1;

	for (i = 1; length && i < IOV_MAX; i++) {
		count = MIN(length, pgsize);
		remote[i].iov_base = (void *)uaddr;
		remote[i].iov_len = count;
		uaddr = VOID_MATH(uaddr, +count);
		length -= count;
		rn += 1;
	}

#ifdef CCTOOLS_CPU_I386
	ssize_t n = syscall(SYSCALL32_process_vm_writev, t->pid, &local, (int32_t)1, remote, rn, (int32_t)0);
#else
	ssize_t n = syscall(SYSCALL64_process_vm_writev, t->pid, &local, (int64_t)1, remote, rn, (int64_t)0);
#endif
	assert(n >= 0 || n == -1);

	/* There is a bug in the implementation, allowing a split remote iovec. The
	 * manual says this should not be possible:
	 *
	 *     "(Partial transfers apply at the granularity of iovec elements.
	 *     These system calls won't perform a partial transfer that splits a
	 *     single iovec element.)"
	 *
	 * So we need to check for this explicitly if doing an atomic read.
	 */
	if (n > 0 && (size_t)n != local.iov_len && flags & TRACER_O_ATOMIC) {
		return errno = EFAULT, -1;
	}

	if (n == -1) {
		if (errno == EFAULT && written) {
			return written;
		}
		return -1;
	}

	written += n;
	data = VOID_MATH(data, +n);

	/* we may need to copy out more, since remote is limited by IOV_MAX */
	if (length && n)
		goto more;

	return written;
}

ssize_t tracer_copy_out( struct tracer *t, const void *data, const void *uaddr, size_t length, int flags )
{
	if(length==0) return 0;

#if !defined(CCTOOLS_CPU_I386)
	if(!tracer_is_64bit(t)) {
		uaddr = VOID_MATH(uaddr, & 0xffffffff);
	}
#endif

	ssize_t rc = copy_out_fast(t,data,uaddr,length,flags);
	if (rc == -1 && errno == ENOSYS && !(flags & TRACER_O_FAST))
		rc = tracer_copy_out_slow(t,data,uaddr,length,flags);
	assert(!(flags & TRACER_O_ATOMIC) || (rc == -1 || (size_t)rc == length));
	return rc;
}

static ssize_t tracer_copy_in_slow( struct tracer *t, void *data, const void *uaddr, size_t length, int flags )
{
	uint8_t *bdata = data;
	const uint8_t *buaddr = uaddr;
	size_t total = 0;
	long word;

	/* first, copy whole words */

	while((length-total)>=sizeof(word)) {
		errno = 0;
		if ((*((long*)bdata) = ptrace(PTRACE_PEEKDATA,t->pid,buaddr,0)) == -1 && errno) {
			if (!(flags & TRACER_O_ATOMIC) && total)
				return total;
			else
				ERROR;
		}
		total += sizeof(word);
		buaddr += sizeof(word);
		bdata += sizeof(word);
	}

	/* if necessary, copy the last few bytes */

	if((length-total)>0) {
		errno = 0;
		if ((word = ptrace(PTRACE_PEEKDATA,t->pid,buaddr,0)) == -1 && errno) {
			if (!(flags & TRACER_O_ATOMIC) && total)
				return total;
			else
				ERROR;
		}
		memcpy(bdata,&word,(length-total));
		total = length;
	}

	return total;
}

ssize_t copy_in_fast( struct tracer *t, void *data, const void *uaddr, size_t length, int flags )
{
	int i;
	size_t pgsize = (size_t)getpagesize();
	struct iovec local;
	struct iovec remote[IOV_MAX];
	size_t rn;
	size_t count;
	size_t read = 0;

	if (!linux_available(3,2,0))
		return errno = ENOSYS, -1;

more:
	local.iov_base = data;
	local.iov_len = length;

	if (flags & TRACER_O_ATOMIC) {
		/* do the whole write */
		count = length;
	} else {
		/* first (partial) page */
		count = MIN(pgsize-(((uintptr_t)uaddr)&(pgsize-1)), length);
	}
	remote[0].iov_base = (void *)uaddr;
	remote[0].iov_len = count;
	uaddr = VOID_MATH(uaddr, +count);
	length -= count;
	rn = 1;

	for (i = 1; length && i < IOV_MAX; i++) {
		count = MIN(length, pgsize);
		remote[i].iov_base = (void *)uaddr;
		remote[i].iov_len = count;
		uaddr = VOID_MATH(uaddr, +count);
		length -= count;
		rn += 1;
	}

#ifdef CCTOOLS_CPU_I386
	ssize_t n = syscall(SYSCALL32_process_vm_readv, (int32_t)t->pid, &local, (int32_t)1, remote, rn, (int32_t)0);
#else
	ssize_t n = syscall(SYSCALL64_process_vm_readv, (int64_t)t->pid, &local, (int64_t)1, remote, rn, (int64_t)0);
#endif
	assert(n >= 0 || n == -1);

	/* There is a bug in the implementation, allowing a split remote iovec. The
	 * manual says this should not be possible:
	 *
	 *     "(Partial transfers apply at the granularity of iovec elements.
	 *     These system calls won't perform a partial transfer that splits a
	 *     single iovec element.)"
	 *
	 * So we need to check for this explicitly if doing an atomic read.
	 */
	if (n > 0 && (size_t)n != local.iov_len && flags & TRACER_O_ATOMIC) {
		return errno = EFAULT, -1;
	}

	if (n == -1) {
		if (errno == EFAULT && read) {
			return read;
		}
		return -1;
	}

	read += n;
	data = VOID_MATH(data, +n);

	/* we may need to copy in more, since remote is limited by IOV_MAX */
	if (length && n)
		goto more;

	return read;
}

ssize_t tracer_copy_in( struct tracer *t, void *data, const void *uaddr, size_t length, int flags )
{
	if(length==0) return 0;

#if !defined(CCTOOLS_CPU_I386)
	if(!tracer_is_64bit(t)) {
		uaddr = VOID_MATH(uaddr, & 0xffffffff);
	}
#endif

	ssize_t rc = copy_in_fast(t,data,uaddr,length,flags);
	if (rc == -1 && errno == ENOSYS && !(flags & TRACER_O_FAST))
		rc = tracer_copy_in_slow(t,data,uaddr,length,flags);
	assert(!(flags & TRACER_O_ATOMIC) || (rc == -1 || (size_t)rc == length));
	return rc;
}

static ssize_t copy_in_string_slow( struct tracer *t, char *str, const void *uaddr, size_t length, int flags )
{
	uint8_t *bdata = (uint8_t *)str;
	const uint8_t *buaddr = uaddr;
	ssize_t total = 0;

	while(length>0) {
		size_t i;
		long word;
		const uint8_t *worddata;
		errno = 0;
		if ((word = ptrace(PTRACE_PEEKDATA,t->pid,buaddr,0)) == -1 && errno)
			ERROR;
		worddata = (const uint8_t *)&word;
		for(i=0;i<sizeof(word);i++) {
			*bdata = worddata[i];
			total++;
			length--;
			if(!*bdata || length == 0) {
				return total;
			} else {
				bdata++;
			}
		}
		buaddr += sizeof(word);
	}

	return total;
}

ssize_t tracer_copy_in_string( struct tracer *t, char *str, const void *uaddr, size_t length, int flags )
{
	if(length==0) return 0;

#if !defined(CCTOOLS_CPU_I386)
	if(!tracer_is_64bit(t)) {
		uaddr = VOID_MATH(uaddr, & 0xffffffff);
	}
#endif

	ssize_t rc = copy_in_fast(t,str,uaddr,length,flags);
	if (rc == -1 && errno == ENOSYS && !(flags & TRACER_O_FAST))
		rc = copy_in_string_slow(t,str,uaddr,length,flags);
	/* check for NUL */
	if (rc > 0) {
		void *nul = memchr(str,'\0',length);
		if (nul) {
			rc = (ssize_t)((uintptr_t)nul-(uintptr_t)str);
		} else {
			*str = '\0';
			errno = EINVAL;
			rc = -1;
		}
	}
	return rc;
}

const char * tracer_syscall32_name( int syscall )
{
	if( syscall<0 || syscall>SYSCALL32_MAX ) {
		return "unknown";
	} else {
		return syscall32_names[syscall];
	}
}

const char * tracer_syscall64_name( int syscall )
{
	if( syscall<0 || syscall>SYSCALL64_MAX ) {
		return "unknown";
	} else {
		return syscall64_names[syscall];
	}
}

const char * tracer_syscall_name( struct tracer *t, int syscall )
{
	if(tracer_is_64bit(t)) {
		return tracer_syscall64_name(syscall);
	} else {
		return tracer_syscall32_name(syscall);
	}
}

/* vim: set noexpandtab tabstop=4: */
