/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_channel.h"
#include "pfs_dispatch.h"
#include "pfs_paranoia.h"
#include "pfs_poll.h"
#include "pfs_process.h"

extern "C" {
#include "debug.h"
#include "itable.h"
#include "macros.h"
#include "stringtools.h"
#include "xxmalloc.h"
}

#include <fcntl.h>
#include <unistd.h>

#include <sys/wait.h>

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

struct pfs_process *pfs_current=0;

static struct itable * pfs_process_table = 0;
static int nprocs = 0;

struct pfs_process * pfs_process_lookup( pid_t pid )
{
	return (struct pfs_process *) itable_lookup(pfs_process_table,pid);
}

/* It would be nice if we could clean up everyone quietly and then some time
 * later, kill hard.  However, on Linux, if someone kills us before we have a
 * chance to clean up, then due to a "feature" of ptrace, all our children will
 * be left stuck in a debug-wait state.  So, rather than chance ourselves
 * getting killed, we will be very aggressive about cleaning up.  Upon
 * receiving any shutdown signal, we immediately blow away everyone involved,
 * and then kill ourselves.
 */
void pfs_process_kill_everyone( int sig )
{   
	debug(D_NOTICE,"received signal %d (%s), killing all my children...",sig,string_signal(sig));
	pfs_process_killall();
	debug(D_NOTICE,"sending myself %d (%s), goodbye!",sig,string_signal(sig));
	while(1) {
		signal(sig,SIG_DFL);
		sigsetmask(~sigmask(sig));
		kill(getpid(),sig);
		kill(getpid(),SIGKILL);
	}
}   

/* For every process interested in asynchronous events, send a SIGIO.  Note
 * that is is more coarse than it should be.  Most processes register interest
 * only on particular fds, however, we have limited mechanism for figuring out
 * which fds are ready.  Just signal everybody.
 */
void pfs_process_sigio()
{
	UINT64_T pid;
	struct pfs_process *p;

	debug(D_PROCESS,"SIGIO received");

	itable_firstkey(pfs_process_table);
	while(itable_nextkey(pfs_process_table,&pid,(void**)&p)) {
		if(p && p->flags&PFS_PROCESS_FLAGS_ASYNC) {
			debug(D_PROCESS,"SIGIO forwarded to pid %d",p->pid);
			pfs_process_raise(p->pid,SIGIO,1);
		}
	}
}

void pfs_process_wake( pid_t pid )
{
	struct pfs_process *p = pfs_process_lookup(pid);
	if(p && (p->state==PFS_PROCESS_STATE_WAITREAD || p->state==PFS_PROCESS_STATE_WAITWRITE) ) {
		debug(D_PROCESS,"pid %d woken from wait state",p->pid);
		pfs_dispatch(p,0);
	}
}

struct pfs_process * pfs_process_create( pid_t pid, pid_t ppid, int share_table )
{  
	struct pfs_process *actual_parent;
	struct pfs_process *child;

	if(!pfs_process_table) pfs_process_table = itable_create(0);

	child = (struct pfs_process *) xxmalloc(sizeof(*child));
	child->tracer = tracer_init(pid);
	if(!child->tracer) {
		free(child);
		return 0;
	}
	child->name[0] = 0;
	child->new_logical_name[0] = 0;
	child->new_physical_name[0] = 0;
	child->pid = pid;
	child->ppid = ppid;
	child->tgid = pid;
	child->state = PFS_PROCESS_STATE_KERNEL;
	child->flags = PFS_PROCESS_FLAGS_STARTUP;
	child->seltime.tv_sec = 0;
	child->syscall = SYSCALL32_fork;
	child->syscall_dummy = 0;
	child->syscall_result = 0;
	child->syscall_args_changed = 0;
	/* to prevent accidental copy out */
	child->interrupted = 0;
	child->did_stream_warning = 0;
	child->nsyscalls = 0;
	child->heap_address = 0;
	child->break_address = 0;
	child->completing_execve = 0;

	actual_parent = pfs_process_lookup(ppid);

	if(actual_parent) {
		child->flags |= actual_parent->flags;
		if(share_table) {
			child->table = actual_parent->table;
			child->table->addref();
		} else {
			child->table = actual_parent->table->fork();
		}
		strcpy(child->name,actual_parent->name);
		child->umask = actual_parent->umask;
		strcpy(child->tty,actual_parent->tty);
		memcpy(child->signal_interruptible,actual_parent->signal_interruptible,sizeof(child->signal_interruptible));
	} else {
		child->table = new pfs_table;

		/* The first child process must inherit file descriptors */

		int count = sysconf(_SC_OPEN_MAX);
		int *flags = (int*)malloc(sizeof(int)*count);
		int i;

		/* Scan through the known file descriptors */

		for(int i=0;i<count;i++) {
			flags[i] = fcntl(i,F_GETFL);
		}

		/* If valid, duplicate and attach them to the child process. */

		for(i=0;i<count;i++) {
			if(i==pfs_channel_fd()) continue;
			if(flags[i]>=0) {
				child->table->attach(i,dup(i),flags[i],0666,"fd");
				debug(D_PROCESS,"attaching to inherited fd %d with flags %d",i,flags[i]);
			} 
		}

		free(flags);

		child->umask = 000;
		strcpy(child->tty,"/dev/tty");
		memset(child->signal_interruptible,0,sizeof(child->signal_interruptible));
	}

	itable_insert(pfs_process_table,pid,child);
	pfs_paranoia_add_pid(pid);

	nprocs++;

	debug(D_PSTREE,"%d %s %d", ppid, share_table ? "newthread" : "fork", pid);
	
	return child;
}

static void pfs_process_delete( struct pfs_process *p )
{
	pfs_poll_clear(p->pid);
	if(p->table) {
		p->table->delref();
		if(!p->table->refs()) delete p->table;
		p->table = 0;
	}
	pfs_paranoia_delete_pid(p->pid);
	tracer_detach(p->tracer);
	itable_remove(pfs_process_table,p->pid);
	free(p);
}

void pfs_process_exit_group( struct pfs_process *child )
{
	struct pfs_process *p;
	UINT64_T pid;

	struct rusage usage;
	memset(&usage,0,sizeof(usage));

	itable_firstkey(pfs_process_table);
	while(itable_nextkey(pfs_process_table,&pid,(void**)&p)) {
		if(p && p!=child && p->tgid == child->tgid) {
			debug(D_PROCESS,"exiting process %d",p->pid);
			pfs_process_stop(p,0,&usage);
		}
	}
}

/* The given process has completed with this status and rusage.
 */
void pfs_process_stop( struct pfs_process *child, int status, struct rusage *usage )
{
	assert(WIFEXITED(status) || WIFSIGNALED(status));
	if(WIFEXITED(status)) {
		debug(D_PSTREE,"%d exit status %d",child->pid,WEXITSTATUS(status));
	} else {
		debug(D_PSTREE,"%d exit signal %d",child->pid,WTERMSIG(status));
	}
	pfs_process_delete(child);
	nprocs--;
}

int pfs_process_count()
{
	return nprocs;
}

extern "C" int pfs_process_getpid()
{
	if(pfs_current) {
		return pfs_current->pid;
	} else {
		return getpid();
	}
}

extern "C" char * pfs_process_name()
{
	if(pfs_current) {
		return pfs_current->name;
	} else {
		return (char *)"unknown";
	}
}

extern const char *pfs_username;

int pfs_process_raise( pid_t pid, int sig, int really_sendit )
{
	struct pfs_process *p;
	int result;

	if(pid==0) pid = pfs_process_getpid();

	pid = ABS(pid);

	p = pfs_process_lookup(pid);
	if(!p) {
		if(pfs_username) {
			result = -1;
			errno = EPERM;
		} else {
			debug(D_PROCESS,"sending signal %d (%s) to external pid %d",sig,string_signal(sig),pid);
			if(pid==getpid()) {
				debug(D_PROCESS,"ignoring attempt to send signal to parrot itself.");
				result = 0;
			} else {
				if (really_sendit) {
					result = kill(pid,sig);
				} else {
					result = 0;
				}
			}
		}
	} else {
		switch(p->state) {
			case PFS_PROCESS_STATE_WAITREAD:
			case PFS_PROCESS_STATE_WAITWRITE:
				if(p->signal_interruptible[sig]) {
					debug(D_PROCESS,"signal %d interrupts pid %d",sig,pid);
					p->interrupted = 1;
					pfs_dispatch(p,sig);
				} else {
					debug(D_PROCESS,"signal %d queued to pid %d",sig,pid);
					if(really_sendit) kill(pid,sig);
				}
				result = 0;
				break;
			default:
				if(really_sendit) {
					debug(D_PROCESS,"sending signal %d (%s) to pfs pid %d",sig,string_signal(sig),pid);
					result = kill(pid,sig);
				} else {
					debug(D_PROCESS,"prepared pfs pid %d to receive signal %d (%s)",pid,sig,string_signal(sig));
					result = 0;
				}
				break;
		}
	}

	return result;
}

void pfs_process_kill()
{
	if(pfs_current) {
		pfs_process_raise(pfs_current->pid,SIGKILL,1);
	} else {
		kill(getpid(),SIGKILL);
	}
}

void pfs_process_killall()
{
	UINT64_T pid;
	struct pfs_process *p;

	itable_firstkey(pfs_process_table);
	while(itable_nextkey(pfs_process_table,&pid,(void**)&p)) {
		debug(D_PROCESS,"killing pid %d",p->pid);
		kill(p->pid,SIGKILL);
	}
}

PTRINT_T pfs_process_scratch_address( struct pfs_process *p )
{
	if(p->break_address && pfs_process_verify_break_rw_address(p)) {
		return p->break_address - sysconf(_SC_PAGESIZE);
	} else {
		return pfs_process_heap_address(p);
	}
}

PTRINT_T pfs_process_heap_address( struct pfs_process *p )
{
	UPTRINT_T start, end, offset;
	int major, minor,inode;
	char flagstring[5];
	FILE *file;
	char line[1024];
	int fields;

	if(p->heap_address) return p->heap_address;

	sprintf(line,"/proc/%d/maps",p->pid);

	file  = fopen(line,"r");
	if(!file) {
		debug(D_PROCESS,"couldn't open %s: %s",line,strerror(errno));
		return 0;
	}

	while(fgets(line,sizeof(line),file)) {
		debug(D_PROCESS,"line: %s",line);

		fields = sscanf(line, "%" SCNxPTR "-%" SCNxPTR " %s %" SCNxPTR "%d:%d %d",
			&start,&end,flagstring,&offset,&major,&minor,&inode);

		if(fields==7 && inode==0 && flagstring[0]=='r' && flagstring[1]=='w' && flagstring[3]=='p') {
			fclose(file);
			p->heap_address = start;
			debug(D_PROCESS,"heap address is 0x%llx",(long long)start);
			return p->heap_address;
		}
	}

	debug(D_PROCESS,"couldn't find heap start address for pid %d",p->pid);

	fclose(file);
	return 0;
}


/* Verify that p->break_address falls into a writable address. 
 * 0 it does not, 1 it does */
int pfs_process_verify_break_rw_address( struct pfs_process *p )
{
	PTRINT_T start, end, offset;
	int major, minor,inode;
	char flagstring[5];
	FILE *file;
	char line[1024];
	int fields;

	if(!p->break_address) return p->heap_address;

	sprintf(line,"/proc/%d/maps",p->pid);

	file  = fopen(line,"r");
	if(!file) {
		debug(D_PROCESS,"couldn't open %s: %s",line,strerror(errno));
		return 0;
	}

	while(fgets(line,sizeof(line),file)) {
		fields = sscanf(line,PTR_FORMAT "-" PTR_FORMAT " %s " PTR_FORMAT "%d:%d %d",&start,&end,flagstring,&offset,&major,&minor,&inode);

		if( start <= p->break_address && p->break_address <= end ) {
			fclose(file);
			if(fields==7 && inode==0 && flagstring[0]=='r' && flagstring[1]=='w' && flagstring[3]=='p') {
				debug(D_DEBUG,"break address 0x%llx is valid.",(long long)p->break_address);
				return 1;
			} else {
				debug(D_PROCESS,"cannot read/write at break address, or break address is not private 0x%llx",(long long)p->break_address);
				return 0;
			}
		}
	}

	fclose(file);

	debug(D_PROCESS,"break address 0x%llx is invalid.", (long long)p->break_address);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
