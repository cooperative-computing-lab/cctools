/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_channel.h"
#include "pfs_paranoia.h"
#include "pfs_process.h"

extern "C" {
#include "debug.h"
#include "itable.h"
#include "linux-version.h"
#include "macros.h"
#include "pfs_resolve.h"
#include "stringtools.h"
#include "xxmalloc.h"
}

#include <fcntl.h>
#include <unistd.h>

#include <sys/resource.h>
#include <sys/wait.h>

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

pfs_pid_mode_t pfs_pid_mode = PFS_PID_MODE_NORMAL;

int emulated_pid = 12345;

struct pfs_process *pfs_current=0;
int parrot_dir_fd = -1;

static struct itable * pfs_process_table = 0;
static int nprocs = 0;

extern uid_t pfs_uid;
extern gid_t pfs_gid;
extern int pfs_fake_setuid;
extern int pfs_fake_setgid;


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

		sigset_t s;
		sigfillset(&s);
		sigdelset(&s, sig);
		sigprocmask(SIG_SETMASK, &s, NULL);\

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
				p->table->attach(fd, nfd, fdflags, S_IRUSR|S_IWUSR, NULL, &buf);
			}
		}
	}
}

/* The point of this function is to make a nice readable path for
 * /proc/self/fd/[0-9]+ to make debugging easier. We could just as easily use a
 * static name like "p".
 */

#define MAX_PATHTOFILENAME 32
void pfs_process_pathtofilename( char *path )
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

void pfs_process_bootstrapfd( void )
{
	extern int parrot_fd_max;
	extern int parrot_fd_start;

	struct rlimit rl;
	rl.rlim_cur = rl.rlim_max = parrot_fd_start;
	if (setrlimit(RLIMIT_NOFILE, &rl) == -1)
		fatal("setrlimit: %s", strerror(errno));
	if (getrlimit(RLIMIT_NOFILE, &rl) == -1)
		fatal("setrlimit: %s", strerror(errno));
	assert((int)rl.rlim_cur == parrot_fd_start && (int)rl.rlim_max == parrot_fd_start);
	debug(D_DEBUG, "lowered RLIMIT_NOFILE to %d.", parrot_fd_start);

	for (int i = 0; i < parrot_fd_max; i++) {
		if (!(i == parrot_dir_fd || i == pfs_channel_fd())) {
			struct stat buf;
			if (fstat(i, &buf) == 0 && !(fcntl(i, F_GETFD)&FD_CLOEXEC) && !bootnative(buf.st_mode)) {
				int fd;
				char path[PATH_MAX] = "";
				char fdlink[PATH_MAX];

				debug(D_DEBUG, "[root tracee] bootstrapping non-native fd as Parrot fd: %d", i);

				snprintf(fdlink, sizeof(fdlink), "/proc/self/fd/%d", i);
				if (readlink(fdlink, path, sizeof(path)-1) == -1)
					strcpy(path, "p"); /* dummy name */

				pfs_process_pathtofilename(path);

				if (linux_available(3,17,0)) {
#ifdef CCTOOLS_CPU_I386
					fd = syscall(SYSCALL32_memfd_create, path, 0);
#else
					fd = syscall(SYSCALL64_memfd_create, path, 0);
#endif
					if (fd == -1)
						fatal("could not create memfd: %s", strerror(errno));
				} else {
					fd = openat(parrot_dir_fd, path, O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR);
					if (fd == -1)
						fatal("could not open Parrot fd: %s", strerror(errno));
					if (unlinkat(parrot_dir_fd, path, 0) == -1)
						fatal("could not unlink Parrot fd file: %s", strerror(errno));
				}
				if (dup2(fd, i) == -1)
					fatal("could not dup2 Parrot fd: %s", strerror(errno));
				if (close(fd) == -1)
					fatal("could not close Parrot fd: %s", strerror(errno));
			}
		}
	}
}

struct pfs_process * pfs_process_create( pid_t pid, struct pfs_process *parent, int thread, int share_table )
{
	struct pfs_process *child;

	if(!pfs_process_table) pfs_process_table = itable_create(0);

	child = (struct pfs_process *) xxmalloc(sizeof(*child));
	child->tracer = tracer_init(pid);
	if(!child->tracer) {
		free(child);
		return 0;
	}
	memset(child->name, 0, sizeof(child->name));
	memset(child->new_logical_name, 0, sizeof(child->new_logical_name));
	child->pid = pid;
	child->tgid = thread ? parent->pid : pid;
	child->state = PFS_PROCESS_STATE_USER; /* a new process always begins in userspace */
	child->flags = PFS_PROCESS_FLAGS_STARTUP;
	child->syscall = SYSCALL32_fork;
	child->syscall_dummy = 0;
	child->syscall_parrotfd = -1;
	child->syscall_result = 0;
	child->syscall_args_changed = 0;
	/* to prevent accidental copy out */
	child->did_stream_warning = 0;
	child->nsyscalls = 0;
	child->completing_execve = 0;
	child->exefd = -1;
	child->ns = NULL;

	if(parent) {
		child->ppid = parent->pid;
		child->ruid = parent->ruid;
		child->euid = parent->euid;
		child->suid = parent->suid;
		child->rgid = parent->rgid;
		child->egid = parent->egid;
		child->sgid = parent->sgid;
		child->ngroups = parent->ngroups;
		memcpy(child->groups, parent->groups, child->ngroups * sizeof(gid_t));
		child->ns = pfs_resolve_share_ns(parent->ns);

		child->flags |= parent->flags;
		if(share_table) {
			child->table = parent->table;
			child->table->addref();
		} else {
			child->table = parent->table->fork();
		}
		strcpy(child->name,parent->name);
		child->umask = parent->umask;
	} else {
		child->ppid = getpid();
		child->ruid = child->euid = child->suid = pfs_uid;
		child->rgid = child->egid = child->sgid = pfs_gid;
		child->ngroups = getgroups(PFS_NGROUPS_MAX, child->groups);
		if (child->ngroups < 0) {
			fatal("Unable to get supplementary groups: %s", strerror(errno));
		}

		extern int parrot_fd_max;

		child->table = new pfs_table;

		/* The first child process must inherit file descriptors */
		/* If valid, duplicate and attach them to the child process. */
		for(int i=0;i<parrot_fd_max;i++) {
			initfd(child, i);
		}

		child->umask = 000;
	}

	itable_insert(pfs_process_table,pid,child);
	pfs_paranoia_add_pid(pid);

	nprocs++;

	debug(D_PSTREE,"%d %s %d", child->ppid, share_table ? "newthread" : "fork", pid);

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
	if(p->exefd >= 0)
		close(p->exefd);
	pfs_paranoia_delete_pid(p->pid);
	tracer_detach(p->tracer);
	itable_remove(pfs_process_table,p->pid);
	pfs_resolve_drop_ns(p->ns);
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
	switch(pfs_pid_mode) {
		case PFS_PID_MODE_NORMAL:
			if(pfs_current) {
				return pfs_current->pid;
			} else {
				return getpid();
			}
			break;
		case PFS_PID_MODE_FIXED:
			return emulated_pid;
			break;
		case PFS_PID_MODE_WARP:
			emulated_pid += 1;
			return emulated_pid;
			break;
	}

	errno = ENOSYS;
	return -1;
}

extern "C" char * pfs_process_name()
{
	if(pfs_current) {
		return pfs_current->name;
	} else {
		return (char *)"unknown";
	}
}

extern "C" struct pfs_mount_entry *pfs_process_current_ns(void) {
	return pfs_current ? pfs_current->ns : NULL;
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
	/* Add red zone... (x86 ABI mandates 128 bytes) */
	stack -= (PFS_SCRATCH_SPACE + 128);
	stack &= ~0xfff; /* align at page */
	return stack;
}

void pfs_process_scratch_get( struct pfs_process *p, void *data, size_t len )
{
	uintptr_t scratch = pfs_process_scratch_address(p);
	if (tracer_copy_in(p->tracer, data, (const void *)scratch, len, TRACER_O_ATOMIC) == -1)
		fatal("could not copy in scratch: %s", strerror(errno));
}

uintptr_t pfs_process_scratch_set( struct pfs_process *p, const void *data, size_t len )
{
	assert(len <= PFS_SCRATCH_SPACE);
	uintptr_t scratch = pfs_process_scratch_address(p);
	if (tracer_copy_out(p->tracer, data, (const void *)scratch, len, TRACER_O_ATOMIC) == -1)
		fatal("could not set scratch: %s", strerror(errno));
	return scratch;
}

void pfs_process_scratch_restore( struct pfs_process *p )
{
	/* do nothing */
}

static int allowed_uid(struct pfs_process *p, uid_t n) {
	return (n == (uid_t) -1) || (n == p->ruid) || (n == p->euid) || (n == p->suid);
}

static int privileged_uid(struct pfs_process *p) {
	return (p->ruid == 0) || (p->euid == 0) || (p->suid == 0);
}

static int check_setuid(struct pfs_process *p, uid_t ruid, uid_t euid, uid_t suid) {
	if (privileged_uid(p)) return 1;
	if (!allowed_uid(p, ruid)) return 0;
	if (!allowed_uid(p, euid)) return 0;
	if (!allowed_uid(p, suid)) return 0;
	return 1;
}

/*
 * As reported by @khurtado, ssh seems to try to drop privileges
 * regardless of the current user. Since an unprivileged user can only
 * drop to themself, this is a no-op in most cases.
 * Parrot silently ignores such no-op id changes, even without
 * `--fake-setuid`.
 */
static int noop_setuid(struct pfs_process *p, uid_t ruid, uid_t euid, uid_t suid) {
	if (ruid != (uid_t) -1 && ruid != p->ruid) return 0;
	if (euid != (uid_t) -1 && euid != p->euid) return 0;
	if (suid != (uid_t) -1 && suid != p->suid) return 0;
	return 1;
}

static int allowed_gid(struct pfs_process *p, gid_t n) {
	return (n == (gid_t) -1) || (n == p->rgid) || (n == p->egid) || (n == p->sgid);
}

static int privileged_gid(struct pfs_process *p) {
	return (p->rgid == 0) || (p->egid == 0) || (p->sgid == 0);
}

static int check_setgid(struct pfs_process *p, gid_t rgid, gid_t egid, gid_t sgid) {
	if (privileged_gid(p)) return 1;
	if (!allowed_gid(p, rgid)) return 0;
	if (!allowed_gid(p, egid)) return 0;
	if (!allowed_gid(p, sgid)) return 0;
	return 1;
}

static int noop_setgid(struct pfs_process *p, gid_t rgid, gid_t egid, gid_t sgid) {
	if (rgid != (gid_t) -1 && rgid != p->rgid) return 0;
	if (egid != (gid_t) -1 && egid != p->egid) return 0;
	if (sgid != (gid_t) -1 && sgid != p->sgid) return 0;
	return 1;
}

/* These checks end up being slightly more lax than the actual ones.
 * The various flavors of set*[ug]id require different combinations of
 * real, effective, and saved to match. Here, we just pretend they all
 * act like setres[ug]id.
 */

int pfs_process_setresuid(struct pfs_process *p, uid_t ruid, uid_t euid, uid_t suid) {
	if (noop_setuid(p, ruid, euid, suid)) return 0;
	if (!pfs_fake_setuid) return -EPERM;
	if (!check_setuid(p, ruid, euid, suid)) return -EPERM;

	if (ruid != (uid_t) -1) {
		p->ruid = ruid;
	}
	if (euid != (uid_t) -1) {
		p->euid = euid;
	}
	if (suid != (uid_t) -1) {
		p->suid = suid;
	}
	return 0;
}

int pfs_process_setreuid(struct pfs_process *p, uid_t ruid, uid_t euid) {
	if (noop_setuid(p, ruid, euid, -1)) return 0;
	if (!pfs_fake_setuid) return -EPERM;
	if (!check_setuid(p, ruid, euid, -1)) return -EPERM;

	if (euid != (uid_t) -1) {
		p->euid = euid;
		if (p->euid != p->ruid) {
			p->suid = p->euid;
		}
	}
	if (ruid != (uid_t) -1) {
		p->ruid = ruid;
		p->suid = p->euid;
	}
	return 0;
}

int pfs_process_setuid(struct pfs_process *p, uid_t uid) {
	if (noop_setuid(p, uid, uid, uid)) return 0;
	if (!pfs_fake_setuid) return -EPERM;
	if (!check_setuid(p, -1, uid, -1)) return -EPERM;

	if (privileged_uid(p)) {
		p->ruid = p->euid = p->suid = uid;
	} else {
		p->euid = uid;
	}
	return 0;
}

int pfs_process_setresgid(struct pfs_process *p, gid_t rgid, gid_t egid, gid_t sgid) {
	if (noop_setgid(p, rgid, egid, sgid)) return 0;
	if (!pfs_fake_setgid) return -EPERM;
	if (!check_setgid(p, rgid, egid, sgid)) return -EPERM;

	if (rgid != (gid_t) -1) {
		p->rgid = rgid;
	}
	if (egid != (gid_t) -1) {
		p->egid = egid;
	}
	if (sgid != (gid_t) -1) {
		p->sgid = sgid;
	}
	return 0;
}

int pfs_process_setregid(struct pfs_process *p, gid_t rgid, gid_t egid) {
	if (noop_setgid(p, rgid, egid, -1)) return 0;
	if (!pfs_fake_setgid) return -EPERM;
	if (!check_setgid(p, rgid, egid, -1)) return -EPERM;

	if (egid != (gid_t) -1) {
		p->egid = egid;
		if (p->egid != p->rgid) {
			p->sgid = p->egid;
		}
	}
	if (rgid != (gid_t) -1) {
		p->rgid = rgid;
		p->sgid = p->egid;
	}
	return 0;
}

int pfs_process_setgid(struct pfs_process *p, gid_t gid) {
	if (noop_setgid(p, gid, gid, gid)) return 0;
	if (!pfs_fake_setgid) return -EPERM;
	if (!check_setgid(p, -1, gid, -1)) return -EPERM;

	if (privileged_gid(p)) {
		p->rgid = p->egid = p->sgid = gid;
	} else {
		p->egid = gid;
	}
	return 0;
}

int pfs_process_getgroups(struct pfs_process *p, int size, gid_t list[]) {
	if (size == 0) return p->ngroups;
	if (p->ngroups > size) return -EINVAL;

	memcpy(list, p->groups, p->ngroups * sizeof(gid_t));
	return p->ngroups;
}

int pfs_process_setgroups(struct pfs_process *p, size_t size, const gid_t *list) {
	if (!pfs_fake_setgid) return -EPERM;
	if (size > PFS_NGROUPS_MAX) return -EINVAL;
	if (!privileged_uid(p)) return -EPERM;

	memcpy(p->groups, list, size * sizeof(gid_t));
	p->ngroups = size;
	return 0;
}

/* vim: set noexpandtab tabstop=8: */
