/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "tracer.h"
#include "stringtools.h"
#include "full_io.h"
#include "xxmalloc.h"
#include "debug.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <limits.h>

#include <sys/wait.h>
#include <sys/ptrace.h>

#define FATAL fatal("tracer: %d %s",t->pid,strerror(errno));

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
	int memory_file;
	int gotregs;
	union {
		struct i386_registers regs32;
		struct x86_64_registers regs64;
	} regs;
	int has_args5_bug;
};

void tracer_prepare()
{
	ptrace(PTRACE_TRACEME,0,0,0);
	/* Ignore return value, as it is sometimes bogus. */
}

struct tracer * tracer_attach( pid_t pid )
{
	struct tracer *t;
	char path[PATH_MAX];

	t = malloc(sizeof(*t));
	if(!t) return 0;

	t->pid = pid;
	t->gotregs = 0;
	t->has_args5_bug = 0;

	sprintf(path,"/proc/%d/mem",pid);
	t->memory_file = open64(path,O_RDWR);
	if(t->memory_file<0) {
		free(t);
		return 0;
	}

	memset(&t->regs,0,sizeof(t->regs));

	return t;
}

int tracer_is_64bit( struct tracer *t )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs)!=0) FATAL;
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
	ptrace(PTRACE_DETACH,t->pid,0,0);
	close(t->memory_file);
	free(t);
}

void tracer_continue( struct tracer *t, int signum )
{
	ptrace(PTRACE_SYSCALL,t->pid,0,signum);
	t->gotregs = 0;
}

int tracer_args_get( struct tracer *t, INT64_T *syscall, INT64_T args[TRACER_ARGS_MAX] )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs)!=0) FATAL;
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

int tracer_args_set( struct tracer *t, INT64_T syscall, INT64_T args[TRACER_ARGS_MAX], int nargs )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs)!=0) FATAL;
		t->gotregs = 1;
	}

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

	if(ptrace(PTRACE_SETREGS,t->pid,0,&t->regs)!=0) FATAL;

	return 1;
}

int tracer_result_get( struct tracer *t, INT64_T *result )
{
	if(!t->gotregs) {
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs)!=0) FATAL;
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
		if(ptrace(PTRACE_GETREGS,t->pid,0,&t->regs)!=0) FATAL;
		t->gotregs = 1;
	}

#ifdef CCTOOLS_CPU_I386
	t->regs.regs32.eax = result;
#else
	t->regs.regs64.rax = result;
#endif

	if(ptrace(PTRACE_SETREGS,t->pid,0,&t->regs)!=0) FATAL;

	return 1;
}

/*
Be careful here:
Note that the amount of data moved around in a PEEKDATA or POKEDATA
depends on the word size of the *caller*, not of the process
being traced.  Thus, a 64-bit Parrot will always move eight bytes
in and out of the target process.  We use a long here because
that represents the natural type of the target platform.
*/

static int tracer_copy_out_slow( struct tracer *t, const void *data, const void *uaddr, int length )
{
	UINT8_T *bdata = (UINT8_T *)data;
	UINT8_T *buaddr = (UINT8_T *)uaddr;
	UINT32_T size = length;
	UINT32_T wordsize = sizeof(long);
	long word;

	/* first, copy whole words */ 

	while(size>=sizeof(long)) {
		word = *(long*)bdata;
		ptrace(PTRACE_POKEDATA,t->pid,buaddr,word);
		size -= wordsize;
		buaddr += wordsize;
		bdata += wordsize;
	}

	/* if necessary, copy the last few bytes */

	if(size>0) {
		word = ptrace(PTRACE_PEEKDATA,t->pid,buaddr,0);
		memcpy(&word,bdata,size);
		ptrace(PTRACE_POKEDATA,t->pid,buaddr,word);
	}

	return length;		
}

int tracer_copy_out( struct tracer *t, const void *data, const void *uaddr, int length )
{
	int result;
	static int has_fast_write=1;
	UPTRINT_T iuaddr = (UPTRINT_T)uaddr;

	if(length==0) return 0;

#if !defined(CCTOOLS_CPU_I386)
	if(!tracer_is_64bit(t)) iuaddr &= 0xffffffff;
#endif

	if(has_fast_write) {
		result = full_pwrite64(t->memory_file,data,length,iuaddr);
		if( result!=length ) {
			has_fast_write = 0;
			debug(D_SYSCALL,"writing to /proc/X/mem failed, falling back to slow ptrace write");
		} else {
			return result;
		}
	}

	result = tracer_copy_out_slow(t,data,(void*)iuaddr,length);

	return result;
}

static int tracer_copy_in_slow( struct tracer *t, const void *data, const void *uaddr, int length )
{
	UINT8_T *bdata = (UINT8_T *)data;
	UINT8_T *buaddr = (UINT8_T *)uaddr;
	UINT32_T size = length;
	UINT32_T wordsize = sizeof(long);
	long word;

	/* first, copy whole words */ 

	while(size>=sizeof(long)) {
		*((long*)bdata) = ptrace(PTRACE_PEEKDATA,t->pid,buaddr,0);
		size -= wordsize;
		buaddr += wordsize;
		bdata += wordsize;
	}

	/* if necessary, copy the last few bytes */

	if(size>0) {
		word = ptrace(PTRACE_PEEKDATA,t->pid,buaddr,0);
		memcpy(bdata,&word,size);
	}

	return length;		
}

int tracer_copy_in_string( struct tracer *t, char *str, const void *uaddr, int length )
{
	UINT8_T *bdata = (UINT8_T *)str;
	UINT8_T *buaddr = (UINT8_T *)uaddr;
	UINT32_T total = 0;
	UINT32_T wordsize = sizeof(long);
	long word;
	unsigned int i;

	while(length>0) {
		word = ptrace(PTRACE_PEEKDATA,t->pid,buaddr,0);
		UINT8_T *worddata = (void*)&word;
		for(i=0;i<wordsize;i++) {
			*bdata = worddata[i];
			total++;
			length--;
			if(!*bdata) {
				return total;
			} else {
				bdata++;
			}
		}
		buaddr += wordsize;
	}

	return total;
}

/*
Notes on tracer_copy_in:
1 - The various versions of the Linux kernel disagree on when and
where it is possible to read from /proc/pid/mem.  Some disallow
entirely, some allow only for certain address ranges, and some
allow it completely.  For example, on Ubuntu 12.02 with Linux 3.2,
we see successes intermixed with failures with no apparent pattern.

We don't want to retry a failing method unnecessarily, so we keep
track of the number of successes and failures.  If the fast read
has succeeded at any point, we keep trying it.  If we have 100 failures
with no successes, then we stop trying it.  In any case, if the fast
read fails, we fall back to the slow method.

2 - pread64 is necessary regardless of whether the target process
is 32 or 64 bit, since the 32-bit pread() cannot read above the 2GB
limit on a 32-bit process.
*/

int tracer_copy_in( struct tracer *t, void *data, const void *uaddr, int length )
{
	int result;
	static int fast_read_success = 0;
	static int fast_read_failure = 0;
	static int fast_read_attempts = 100;

	UPTRINT_T iuaddr = (UPTRINT_T)uaddr;

#if !defined(CCTOOLS_CPU_I386)
	if(!tracer_is_64bit(t)) iuaddr &= 0xffffffff;
#endif

	if(fast_read_success>0 || fast_read_failure<fast_read_attempts) {
		result = full_pread64(t->memory_file,data,length,iuaddr);
		if(result>0) {
			fast_read_success++;
			return result;
		} else {
			fast_read_failure++;
			// fall through to slow method, print message on the last attempt.
			if(fast_read_success==0 && fast_read_failure>=fast_read_attempts) {
				debug(D_SYSCALL,"reading from /proc/X/mem failed, falling back to slow ptrace read");
			}
		}
	}

	result = tracer_copy_in_slow(t,data,(void*)iuaddr,length);

	return result;
}

#include "tracer.table.c"
#include "tracer.table64.c"


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
