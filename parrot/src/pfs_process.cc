/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_channel.h"
#include "pfs_paranoia.h"
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
int parrot_dir_fd = -1;

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
extern "C" void pfs_process_sigio(int sig)
{
	UINT64_T pid;
	struct pfs_process *p;

	assert(sig == SIGIO);
	debug(D_PROCESS,"SIGIO received");

	itable_firstkey(pfs_process_table);
	while(itable_nextkey(pfs_process_table,&pid,(void**)&p)) {
		if(p && p->flags&PFS_PROCESS_FLAGS_ASYNC) {
			debug(D_PROCESS,"SIGIO forwarded to pid %d",p->pid);
			kill(p->pid, SIGIO);
		}
	}
}

int pfs_process_stat( pid_t pid, int fd, struct stat *buf )
{
	char path[4096];
	snprintf(path, sizeof(path), "/proc/%d/fd/%d", pid, fd);
	if (stat(path, buf) == -1)
		return -1;
	return 0;
}

static int bootnative( mode_t mode )
{
	return (S_ISSOCK(mode) || S_ISBLK(mode) || S_ISCHR(mode) || S_ISFIFO(mode));
}

static void initfd( pfs_process *p, int fd )
{
	if(fd == parrot_dir_fd || fd == pfs_channel_fd()) {
		p->table->setspecial(fd);
	} else {
		struct stat buf;
		int fdflags;
		if (fstat(fd, &buf) == 0 && !((fdflags = fcntl(fd, F_GETFD))&FD_CLOEXEC)) {
			debug(D_DEBUG, "found %d", fd);
			if (bootnative(buf.st_mode)) {
				p->table->setnative(fd, 0);
			} else {
				int flflags = fcntl(fd, F_GETFL);
				debug(D_PROCESS, "attaching to inherited native fd %d with flags %d", fd, flflags);

				/* create a duplicate because the tracee(s) might close the fd */
				int nfd = dup(fd);
				if (nfd == -1)
					fatal("could not dup %d: %s", fd, strerror(errno));
				/* So nfd closes on exec and is not attached again... */
				fcntl(nfd, F_SETFD, fcntl(nfd, F_GETFD)|FD_CLOEXEC);

				/* The fd was closed and opened as a "Parrot fd" by the root tracee, find its inode: */
				if (pfs_process_stat(p->pid, fd, &buf) == -1)
					fatal("could not stat root tracee: %s", strerror(errno));
				p->table->attach(fd, nfd, fdflags, S_IRUSR|S_IWUSR, "fd", &buf);
			}
		}
	}
}

void pfs_process_bootstrapfd( void )
{
	int count = sysconf(_SC_OPEN_MAX);
	for (int i = 0; i < count; i++) {
		if (!(i == parrot_dir_fd || i == pfs_channel_fd())) {
			struct stat buf;
			if (fstat(i, &buf) == 0 && !(fcntl(i, F_GETFD)&FD_CLOEXEC) && !bootnative(buf.st_mode)) {
				debug(D_DEBUG, "[root tracee] bootstrapping non-native fd as Parrot fd: %d", i);
				int fd = openat(parrot_dir_fd, "p", O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR);
				if (fd == -1)
					fatal("could not open Parrot fd: %s", strerror(errno));
				if (unlinkat(parrot_dir_fd, "p", 0) == -1)
					fatal("could not unlink Parrot fd file: %s", strerror(errno));
				if (dup2(fd, i) == -1)
					fatal("could not dup2 Parrot fd: %s", strerror(errno));
				if (close(fd) == -1)
					fatal("could not close Parrot fd: %s", strerror(errno));
			}
		}
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
	child->syscall_parrotfd = -1;
	child->syscall_result = 0;
	child->syscall_args_changed = 0;
	/* to prevent accidental copy out */
	child->did_stream_warning = 0;
	child->nsyscalls = 0;
	child->scratch_used = 0;
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
	} else {
		child->table = new pfs_table;

		/* The first child process must inherit file descriptors */
		/* If valid, duplicate and attach them to the child process. */
		int count = sysconf(_SC_OPEN_MAX);
		for(int i=0;i<count;i++) {
			initfd(child, i);
		}

		child->umask = 000;
	}

	itable_insert(pfs_process_table,pid,child);
	pfs_paranoia_add_pid(pid);

	nprocs++;

	debug(D_PSTREE,"%d %s %d", ppid, share_table ? "newthread" : "fork", pid);

	return child;
}

void pfs_process_exec( struct pfs_process *p )
{
	debug(D_PROCESS, "pid %d is completing exec", p->pid);
	p->table->close_on_exec();
}

static void pfs_process_delete( struct pfs_process *p )
{
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

/* The given process has completed with this status and rusage.
 */
void pfs_process_stop( struct pfs_process *p, int status, struct rusage *usage )
{
	assert(WIFEXITED(status) || WIFSIGNALED(status));
	if(WIFEXITED(status)) {
		debug(D_PSTREE,"%d exit status %d (%" PRIu64 " syscalls)",p->pid,WEXITSTATUS(status),p->nsyscalls);
	} else {
		debug(D_PSTREE,"%d exit signal %d (%" PRIu64 " syscalls)",p->pid,WTERMSIG(status),p->nsyscalls);
	}
	pfs_process_delete(p);
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

int pfs_process_cankill( pid_t pid )
{
	pid = ABS(pid);

	if (pid == 0) {
		return 0;
	} else if (pid == getpid()) {
		/* Parrot? naughty... */
		debug(D_PROCESS,"ignoring attempt to send signal to parrot itself.");
		return errno = EPERM, -1;
	} else if (pfs_process_lookup(pid)) {
		return 0;
	} else if (pfs_username) {
		return errno = EPERM, -1;
	} else {
		return 0;
	}
}

void pfs_process_killall()
{
	UINT64_T pid;
	struct pfs_process *p;

	if (pfs_process_table) {
		itable_firstkey(pfs_process_table);
		while(itable_nextkey(pfs_process_table,&pid,(void**)&p)) {
			debug(D_PROCESS,"killing pid %d",p->pid);
			kill(p->pid,SIGKILL);
		}
	}
}

uintptr_t pfs_process_scratch_address( struct pfs_process *p )
{
	uintptr_t stack;
	tracer_stack_get(p->tracer, &stack);
	stack -= sizeof(p->scratch_data);
	stack &= ~0x3; /* ensure it is aligned for most things */
	return stack;
}

void pfs_process_scratch_get( struct pfs_process *p, void *data, size_t len )
{
	assert(p->scratch_used <= len);
	uintptr_t scratch = pfs_process_scratch_address(p);
	if (tracer_copy_in(p->tracer, data, (const void *)scratch, p->scratch_used) == -1)
		fatal("could not copy in scratch: %s", strerror(errno));
	//debug(D_DEBUG, "%s: `%.*s':%zu", __func__, (int)len, (char *)data, p->scratch_used);
}

uintptr_t pfs_process_scratch_set( struct pfs_process *p, const void *data, size_t len )
{
	assert(len <= sizeof(p->scratch_data));
	uintptr_t scratch = pfs_process_scratch_address(p);
	if (tracer_copy_in(p->tracer, p->scratch_data, (const void *)scratch, len) == -1)
		fatal("could not copy in scratch: %s", strerror(errno));
	if (tracer_copy_out(p->tracer, data, (const void *)scratch, len) == -1)
		fatal("could not set scratch: %s", strerror(errno));
	//debug(D_DEBUG, "%s: `%.*s':%zu", __func__, (int)len, (char *)p->scratch_data, len);
	p->scratch_used = len;
	return scratch;
}

void pfs_process_scratch_restore( struct pfs_process *p )
{
	assert(p->scratch_used > 0);
	uintptr_t scratch = pfs_process_scratch_address(p);
	if (tracer_copy_out(p->tracer, p->scratch_data, (const void *)scratch, p->scratch_used) == -1)
		fatal("could not restore scratch: %s", strerror(errno));
	//debug(D_DEBUG, "%s: `%.*s':%zu", __func__, (int)p->scratch_used, (char *)p->scratch_data, p->scratch_used);
	p->scratch_used = 0;
}

/* vim: set noexpandtab tabstop=4: */
