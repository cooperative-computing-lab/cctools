/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_process.h"
#include "pfs_dispatch.h"
#include "pfs_poll.h"

extern "C" {
#include "debug.h"
#include "xmalloc.h"
#include "stringtools.h"
#include "macros.h"
#include "itable.h"
}

#include <sys/wait.h>
#include <signal.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

struct pfs_process *pfs_current=0;

static struct itable * pfs_process_table = 0;
static int nprocs = 0;

struct pfs_process * pfs_process_lookup( pid_t pid )
{
	return (struct pfs_process *) itable_lookup(pfs_process_table,pid);
}

/*
For every process interested in asynchronous events,
send a SIGIO.  Note that is is more coarse than it
should be.  Most processes register interest only
on particular fds, however, we have limited mechanism
for figuring out which fds are ready.  Just signal
everybody.
*/

void pfs_process_sigio()
{
	UINT64_T pid;
	struct pfs_process *p;

	debug(D_PROCESS,"SIGIO received");

	itable_firstkey(pfs_process_table);
	while(itable_nextkey(pfs_process_table,&pid,(void**)&p)) {
		if(p && p->flags&PFS_PROCESS_FLAGS_ASYNC) {
			switch(p->state) {
				case PFS_PROCESS_STATE_DONE:
					break;
				default:
					debug(D_PROCESS,"SIGIO forwarded to pid %d",p->pid);
					pfs_process_raise(p->pid,SIGIO,1);
					break;
			}
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

/*
Note that a new process really has two kinds of parents:
- The 'actual parent' process is the process that called fork or clone to create the child process.  From this parent, the child inherits the file table and other properties.
- If 'notify parent' is the parent process that should be notified when the process exits.  This is different than the actual parent if the child was created with the CLONE_PARENT flag to clone.
*/


struct pfs_process * pfs_process_create( pid_t pid, pid_t actual_ppid, pid_t notify_ppid, int share_table, int exit_signal )
{  
	struct pfs_process *actual_parent;
	struct pfs_process *child;

	if(!pfs_process_table) pfs_process_table = itable_create(0);

	child = (struct pfs_process *) xxmalloc(sizeof(*child));
	child->tracer = tracer_attach(pid);
	if(!child->tracer) {
		free(child);
		return 0;
	}
	child->name[0] = 0;
	child->new_logical_name[0] = 0;
	child->new_physical_name[0] = 0;
	child->pid = pid;
	child->ppid = notify_ppid;
	child->tgid = pid;
	child->state = PFS_PROCESS_STATE_KERNEL;
	child->flags = PFS_PROCESS_FLAGS_STARTUP;
	child->seltime.tv_sec = 0;
	child->syscall = SYSCALL32_fork;
	child->syscall_dummy = 0;
	child->syscall_result = 0;
	child->syscall_args_changed = 0;
	child->exit_status = 0;
	child->exit_signal = exit_signal;
	/* to prevent accidental copy out */
	child->wait_ustatus = 0;
	child->wait_urusage = 0;
	child->interrupted = 0;
	child->did_stream_warning = 0;
	child->nsyscalls = 0;
	child->heap_address = 0;
	child->break_address = 0;

	actual_parent = pfs_process_lookup(actual_ppid);

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
		child->table->attach(0,dup(0),O_RDONLY,0666,"stdin");
		child->table->attach(1,dup(1),O_WRONLY,0666,"stdout");
		child->table->attach(2,dup(2),O_WRONLY,0666,"stderr");
		child->umask = 000;
		strcpy(child->tty,"/dev/tty");
		memset(child->signal_interruptible,0,sizeof(child->signal_interruptible));
	}

	itable_insert(pfs_process_table,pid,child);

	nprocs++;

	debug(D_PSTREE,"%d %s %d",actual_ppid,share_table ? "newthread" : "fork", pid);
	
	return child;
}

void pfs_process_delete( struct pfs_process *p )
{
	/* The file table was deleted in pfs_process_stop */
	itable_remove(pfs_process_table,p->pid);
	tracer_detach(p->tracer);
	free(p);
}

/*
Cause this parent process to wake up, reporting
the status of the given child process.  If the
latter is complete, reap its status.
*/

static void pfs_process_do_wake( struct pfs_process *parent, struct pfs_process *child )
{
	parent->state = PFS_PROCESS_STATE_KERNEL;
	parent->syscall_result = child->pid;

	if(parent->wait_ustatus) {
		tracer_copy_out(
			parent->tracer,
			&child->exit_status,
			parent->wait_ustatus,
			sizeof(child->exit_status)
		);
	}
	if(parent->wait_urusage) {
		tracer_copy_out(
			parent->tracer,
			&child->exit_rusage,
			parent->wait_urusage,
			sizeof(child->exit_rusage)
		);
	}

	if(child->state==PFS_PROCESS_STATE_DONE) {
		pfs_process_delete(child);
	} else {
		child->state=PFS_PROCESS_STATE_USER;
	}

	/* to prevent accidental copy out */
	parent->wait_ustatus = 0;
	parent->wait_urusage = 0;
}

/*
Examine the state of this parent and child.
If the parent is waiting for the child, then wake it up.
Or, if the process is in a waitio state, then
kick it out with a signal.
*/

static int pfs_process_may_wake( struct pfs_process *parent, struct pfs_process *child )
{
	if(child->ppid == parent->pid ) {
		if( (child->state==PFS_PROCESS_STATE_DONE) ||
		    ( (child->state==PFS_PROCESS_STATE_WAITPID) && (parent->wait_options&WUNTRACED)) ) {
			if(parent->state==PFS_PROCESS_STATE_WAITPID) {
				if( (parent->wait_pid<=0) || (parent->wait_pid==child->pid) ) {
					pfs_process_do_wake(parent,child);
					return 1;
				} else {
					return 0;
				}
			} else {
				return 0;
			}
		} else {
			return 0;
		}
	} else {
		return 0;
	}
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
			pfs_process_stop(p,0,usage);
		}
	}
}

/*
The given process has stopped or completed with this status and rusage.
If this process has a parent, attempt to inform it, otherwise
wait for the parent to show up and claim it.  If we wake up a
parent ourselves, then cause it to be rescheduled.
*/

void pfs_process_stop( struct pfs_process *child, int status, struct rusage usage )
{
	struct pfs_process *parent;

	if(child->state==PFS_PROCESS_STATE_DONE) return;

	if(WIFEXITED(status) || WIFSIGNALED(status) ) {
		if(WIFEXITED(status)) {
			debug(D_PSTREE,"%d exit status %d",child->pid,WEXITSTATUS(status));
		} else {
			debug(D_PSTREE,"%d exit signal %d",child->pid,WTERMSIG(status));
		}
		child->state = PFS_PROCESS_STATE_DONE;
		pfs_poll_clear(child->pid);
		nprocs--;
		if(child->table) {
			child->table->delref();
			if(!child->table->refs()) delete child->table;
			child->table = 0;
		}
	} else {
		child->state = PFS_PROCESS_STATE_WAITPID;
	}

	child->exit_status = status;
	child->exit_rusage = usage;

	parent = pfs_process_lookup(child->ppid);

	if(parent) {
		int send_signal = 0;
		if(child->state==PFS_PROCESS_STATE_DONE) {
			if(child->exit_signal) {
				send_signal = child->exit_signal;
			}
		}

		/*
		XXX WARNING
		Do not refer to child after this point!
		It may have been deleted by pfs_process_may_wake!
		*/

		if(pfs_process_may_wake(parent,child)) {
			tracer_continue(parent->tracer,0);
		} else if(send_signal!=0) {
			pfs_process_raise(parent->pid,send_signal,1);
		}
	}
}

/*
This process is entering the waitpid state. Mark its structure as such,
then search to see if it can be immediately satisfied. If so,
return true.  Otherwise, if WNOHANG is given, get ready to return
from the kernel.
*/

int pfs_process_waitpid( struct pfs_process *p, pid_t wait_pid, int *wait_ustatus, int wait_options, struct rusage *wait_urusage )
{
	UINT64_T childpid;
	struct pfs_process *child;
	int nchildren = 0;

	p->state = PFS_PROCESS_STATE_WAITPID;
	p->wait_pid = wait_pid;
	p->wait_ustatus = wait_ustatus;
	p->wait_options = wait_options;
	p->wait_urusage = wait_urusage;
	p->syscall_result = -EINTR;

	itable_firstkey(pfs_process_table);
	while(itable_nextkey(pfs_process_table,&childpid,(void**)&child)) {
		if(child && child->ppid==p->pid) {
			nchildren++;
			if(pfs_process_may_wake(p,child)) return 1;
		}
	}

	if(nchildren==0) {
		p->state = PFS_PROCESS_STATE_KERNEL;
		p->syscall_result = -ECHILD;
	} else if(wait_options&WNOHANG) {
		p->state = PFS_PROCESS_STATE_KERNEL;
		p->syscall_result = 0;
	}

	return 0;
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

int  pfs_process_raise( pid_t pid, int sig, int really_sendit )
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
				result = kill(pid,sig);
			}
		}
	} else {
		debug(D_PROCESS,"sending signal %d (%s) to pfs pid %d",sig,string_signal(sig),pid);
		switch(p->state) {
			case PFS_PROCESS_STATE_WAITPID:
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
					result = kill(pid,sig);
				} else {
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
	if(p->break_address) {
		return p->break_address - 4096;
	} else {
		return pfs_process_heap_address(p);
	}
}

PTRINT_T pfs_process_heap_address( struct pfs_process *p )
{
	PTRINT_T start, end, offset;
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
		fields = sscanf(line,PTR_FORMAT "-" PTR_FORMAT " %s " PTR_FORMAT "%d:%d %d",
			&start,&end,flagstring,&offset,&major,&minor,&inode);

		if(fields==7 && !strcmp(flagstring,"rw-p") && inode==0) {
			fclose(file);
			p->heap_address = start;
			debug(D_PROCESS,"heap address is 0x%x",start);
			return p->heap_address;
		}
	}

	debug(D_PROCESS,"couldn't find heap start address for pid %d",p->pid);

	fclose(file);
	return 0;
}


