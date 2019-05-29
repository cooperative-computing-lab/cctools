/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "linux-version.h"
#include "pfs_channel.h"
#include "pfs_critical.h"
#include "pfs_dispatch.h"
#include "pfs_paranoia.h"
#include "pfs_process.h"
#include "pfs_service.h"
#include "pfs_table.h"
#include "pfs_time.h"
#include "ptrace.h"

extern "C" {
#include "parrot_client.h"
#include "pfs_resolve.h"
#include "pfs_mountfile.h"
}

#ifndef PTRACE_EVENT_STOP
#  define PTRACE_EVENT_STOP 128
#endif

extern "C" {
#include "auth_all.h"
#include "cctools.h"
#include "chirp_client.h"
#include "chirp_global.h"
#include "chirp_ticket.h"
#include "create_dir.h"
#include "debug.h"
#include "delete_dir.h"
#include "file_cache.h"
#include "ftp_lite.h"
#include "getopt.h"
#include "int_sizes.h"
#include "itable.h"
#include "macros.h"
#include "md5.h"
#include "password_cache.h"
#include "random.h"
#include "stringtools.h"
#include "string_array.h"
#include "tracer.h"
#include "xxmalloc.h"
#include "hash_table.h"
#include "jx.h"
#include "jx_pretty_print.h"
#include "stats.h"
}

#include <fcntl.h>
#include <termio.h>
#include <termios.h>
#include <unistd.h>

#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <sys/wait.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <vector>

#define SIG_ISSTOP(s) (s == SIGTTIN || s == SIGTTOU || s == SIGSTOP || s == SIGTSTP)

FILE *namelist_file;
struct hash_table *namelist_table;
int linux_major;
int linux_minor;
int linux_micro;

int wait_barrier = 0;

int pfs_master_timeout = 300;
struct file_cache *pfs_file_cache = 0;
struct password_cache *pfs_password_cache = 0;
struct hash_table *available_services;
int pfs_force_stream = 0;
int pfs_force_cache = 0;
int pfs_force_sync = 0;
int pfs_follow_symlinks = 1;
int pfs_session_cache = 0;
int pfs_use_helper = 0;
int pfs_checksum_files = 1;
int pfs_write_rval = 0;
int pfs_no_flock = 0;
int pfs_paranoid_mode = 0;
const char *pfs_write_rval_file = "parrot.rval";
int pfs_enable_small_file_optimizations = 1;
int set_foreground = 1;
int pfs_syscall_disable_debug = 0;
int pfs_allow_dynamic_mounts = 0;

char sys_temp_dir[PATH_MAX] = "/tmp";
char pfs_temp_dir[PATH_MAX];
char pfs_temp_per_instance_dir[PATH_MAX];

int *pfs_syscall_totals32 = 0;
int *pfs_syscall_totals64 = 0;

const char *pfs_root_checksum=0;
const char *pfs_initial_working_directory=0;

char *pfs_false_uname = 0;
char pfs_ldso_path[PATH_MAX];
uid_t pfs_uid = 0;
gid_t pfs_gid = 0;
const char * pfs_username = 0;
int pfs_fake_setuid = 0;
int pfs_fake_setgid = 0;

INT64_T pfs_syscall_count = 0;
INT64_T pfs_read_count = 0;
INT64_T pfs_write_count = 0;

const char * pfs_cvmfs_repo_arg = 0;
const char * pfs_cvmfs_config_arg = 0;
const char * pfs_cvmfs_http_proxy = 0;
bool pfs_cvmfs_repo_switching = false;
char pfs_cvmfs_alien_cache_dir[PATH_MAX];
char pfs_cvmfs_locks_dir[PATH_MAX];
bool pfs_cvmfs_enable_alien  = true;
char pfs_cvmfs_option_file[PATH_MAX];
struct jx *pfs_cvmfs_options = NULL;


int pfs_irods_debug_level = 0;
char *stats_file = NULL;

int parrot_fd_max = -1;
int parrot_fd_start = -1;

pfs_service *pfs_service_ext_init(const char *image, const char *mountpoint);

/*
This process at the very top of the traced tree
and its final exit status, which we use to determine
our own exit status
*/

static pid_t root_pid = -1;
static int root_exitstatus = 0;
static int channel_size = 10;

enum {
	LONG_OPT_CHECK_DRIVER = UCHAR_MAX+1,
	LONG_OPT_CVMFS_ALIEN_CACHE,
	LONG_OPT_CVMFS_CONFIG,
	LONG_OPT_CVMFS_DISABLE_ALIEN_CACHE,
	LONG_OPT_CVMFS_REPO_SWITCHING,
	LONG_OPT_CVMFS_OPTION,
	LONG_OPT_CVMFS_OPTION_FILE,
	LONG_OPT_HELPER,
	LONG_OPT_NO_SET_FOREGROUND,
	LONG_OPT_SYSCALL_DISABLE_DEBUG,
	LONG_OPT_VALGRIND,
	LONG_OPT_FAKE_SETUID,
	LONG_OPT_DYNAMIC_MOUNTS,
	LONG_OPT_IS_RUNNING,
	LONG_OPT_TIME_STOP,
	LONG_OPT_TIME_WARP,
	LONG_OPT_PARROT_PATH,
	LONG_OPT_PID_WARP,
	LONG_OPT_PID_FIXED,
	LONG_OPT_STATS_FILE,
	LONG_OPT_DISABLE_SERVICE,
	LONG_OPT_NO_FLOCK,
	LONG_OPT_EXT_IMAGE,
};

static void get_linux_version(const char *cmd)
{
	struct utsname name;
	int fields;

	uname(&name);

#ifdef CCTOOLS_CPU_I386
	if(!strcmp(name.machine,"x86_64")) {
		fatal("Sorry, you need to download a Parrot built specifically for an x86_64 CPU");
	}
#endif

	if(strcmp(name.sysname, "Linux") != 0)
		fatal("Sorry, parrot only operates on Linux");

	fields = sscanf(name.release,"%d.%d.%d",&linux_major,&linux_minor,&linux_micro);
	if(fields != 3)
		fatal("could not get linux version from: `%s'", name.release);

	debug(D_DEBUG,"kernel is %s %s",name.sysname,name.release);

	/* warning for latest untested version of Linux */
	if(linux_available(4,5,0))
		debug(D_NOTICE,"parrot_run %s has not been tested on %s %s yet, this may not work",CCTOOLS_VERSION,name.sysname,name.release);
	else if (!linux_available(2,5,60))
		fatal("this version of Parrot requires at least kernel version 2.5.60");
}

static void pfs_helper_init( void )
{
	char helper_path[PATH_MAX];

	debug(D_DEBUG,"locating helper library...");

	snprintf(helper_path, sizeof(helper_path),"%s/lib/libparrot_helper.so",INSTALL_PATH);

	char *s = getenv("PARROT_HELPER");
	if(s) {
		debug(D_DEBUG,"PARROT_HELPER=%s",s);
		snprintf(helper_path,sizeof(helper_path),"%s",s);
	} else {
		debug(D_DEBUG,"PARROT_HELPER is not set");
	}

	if(access(helper_path,R_OK)==0 || strstr(helper_path, "/$LIB/")) {
		debug(D_DEBUG,"found helper in %s",helper_path);
		setenv("LD_PRELOAD",helper_path,1);
	} else {
		debug(D_DEBUG,"couldn't find helper library %s but continuing anyway.",helper_path);
	}
}

static void show_help( const char *cmd )
{
	          /* 80-column text marker */
	         /******************************************************************************/
	printf("\n");
	printf( "Use: %s [options] <command> ...\n",cmd);
	printf("\n");
	printf( "Most common options are:\n");
	printf( " %-30s Mount (redirect) /foo to /bar.          (PARROT_MOUNT_STRING)\n", "-M,--mount=/foo=/bar");
	printf( " %-30s Use this file as a mountlist.             (PARROT_MOUNT_FILE)\n", "-m,--ftab-file=<file>");
	printf( " %-30s Where to store temporary files.             (PARROT_TEMP_DIR)\n", "-t,--tempdir=<dir>");
	printf( " %-30s Maximum amount of time to retry failures.    (PARROT_TIMEOUT)\n", "-T,--timeout=<time>");
	printf( " %-30s Enable debugging for this sub-system.    (PARROT_DEBUG_FLAGS)\n", "-d,--debug=<name>");
	printf( " %-30s Send debugging to this file.              (PARROT_DEBUG_FILE)\n", "-o,--debug-file=<file>");
	printf( " %-30s     (can also be :stderr, :stdout, :syslog, or :journal)\n", "");
	printf( " %-30s Rotate debug files of this size.     (PARROT_DEBUG_FILE_SIZE)\n", "-O,--debug-rotate-max=<bytes>");
	printf( " %-30s     (default 10M, 0 disables)\n","");
	printf( " %-30s Display version number.\n", "-v,--version");
	printf( " %-30s Test if Parrot is already running.\n", "   --is-running");
	printf( " %-30s Save runtime statistics to a file.\n", "   --stats-file");
	printf( " %-30s Show most commonly used options.\n", "-h,--help");
	printf("\n");
	printf("Virtualization options:\n");
	printf( " %-30s Print exit status information to <file>.\n", "-c,--status-file=<file>");
	printf( " %-30s Check if the given driver is enabled and exit.\n","   --check-driver");
	printf( " %-30s Enable dynamic mounting with parrot_mount.\n","   --dynamic-mounts");
	printf( " %-30s Record the environment variables at the starting point.\n", "-e,--env-list=<path>");
	printf( " %-30s Track changes from setuid and setgid.\n", "   --fake-setuid");
	printf( " %-30s Fake this gid; Real gid stays the same.          (PARROT_GID)\n", "-G,--gid=<num>");
	printf( " %-30s Enable use of helper library.\n", "   --helper");
	printf( " %-30s Path to ld.so to use.                      (PARROT_LDSO_PATH)\n", "-l,--ld-path=<path>");
	printf( " %-30s Record all the file names.\n", "-n,--name-list=<path>");
	printf( " %-30s Disable changing the foreground process group of the session.\n","   --no-set-foreground");
	printf( " %-30s Pretend that this is my hostname.          (PARROT_HOST_NAME)\n", "-N,--hostname=<name>");
	printf( " %-30s Enable paranoid mode for identity boxing mode.\n", "-P,--paranoid");
	printf( " %-30s Stop virtual time at midnight, Jan 1st, 2001 UTC.\n", "   --time-stop");
	printf( " %-30s Warp virtual time starting from midnight, Jan 1st, 2001 UTC.\n","   --time-warp");
	printf( " %-30s Fake this unix uid; Real uid stays the same.     (PARROT_UID)\n", "-U,--uid=<num>");
	printf( " %-30s Use this extended username.                 (PARROT_USERNAME)\n", "-u,--username=<name>");
	printf( " %-30s Enable valgrind support for Parrot.\n", "   --valgrind");
	printf( " %-30s Initial working directory.\n", "-w,--work-dir=<dir>");
	printf( " %-30s Display table of system calls trapped.\n", "-W,--syscall-table");
	printf("\n");
	printf("Performance and consistency options:\n");
	printf( " %-30s Set the I/O block size hint.              (PARROT_BLOCK_SIZE)\n", "-b,--block-size=<bytes>");
	printf( " %-30s Disable small file optimizations.\n", "-D,--no-optimize");
	printf( " %-30s Enable file snapshot caching for all protocols.\n", "-F,--with-snapshots");
	printf( " %-30s Disable following symlinks.\n", "-f,--no-follow-symlinks");
	printf( " %-30s Use streaming protocols without caching.(PARROT_FORCE_STREAM)\n", "-s,--stream-no-cache");
	printf( " %-30s Enable whole session caching for all protocols.\n", "-S,--session-caching");
	printf( " %-30s Force synchronous disk writes.            (PARROT_FORCE_SYNC)\n", "-Y,--sync-write");
	printf( " %-30s Enable automatic decompression on .gz files.\n", "-Z,--auto-decompress");
	printf( " %-30s Disable the given service.\n", "--disable-service");
	printf( " %-30s Make flock a no-op.\n", "--no-flock");
	printf("\n");
	printf("Filesystem Options:\n");
	printf( " %-30s Mount a read-only ext[234] disk image.\n", "--ext <image>=<mountpoint>");
	printf("FTP / GridFTP options:\n");
	printf( " %-30s Enable data channel authentication in GridFTP.\n", "-C,--channel-auth");
	printf("\n");
	printf("Chirp filesystem options:\n");
	printf( " %-30s Use these Chirp authentication methods.   (PARROT_CHIRP_AUTH)\n", "-a,--chirp-auth=<list>");
	printf( " %-30s Comma-delimited list of tickets to use for authentication.\n", "-i,--tickets=<files>");
	printf( " %-30s Inhibit catalog queries to list /chirp.\n", "-Q,--no-chirp-catalog");
	printf("\n");
	printf("iRODS filesystem options:\n");
	printf( " %-30s Set the debug level output for the iRODS driver.\n", "-I,--debug-level-irods=<num>");
	printf("\n");
	printf("GROW-FS filesystem options:\n");
	printf( " %-30s Use this checksum for the GROW-FS root.\n", "-R,--root-checksum=<cksum>");
	printf( " %-30s Use checksums to verify file integrity.\n", "-K,--with-checksums");
	printf( " %-30s Do not use checksums.\n", "-k,--no-checksums");
	printf( " %-30s Use this HTTP proxy server.                       (HTTP_PROXY)\n", "-p,--proxy=<hst:p>");
	printf( "\n");
	printf("CVMFS filesystem options:\n");
	printf( " %-30s Path to CVMFS options file.               (PARROT_CVMFS_OPTION_FILE)\n", "   --cvmfs-option-file=<config>");
	printf( " %-30s Set a CVMFS option.\n", "   --cvmfs-option CVMFS_XXX=yyy");
	printf( " %-30s (deprecated) CVMFS common configuration.               (PARROT_CVMFS_CONFIG)\n", "   --cvmfs-config=<config>");
	printf( " %-30s CVMFS repositories to enable.               (PARROT_CVMFS_REPO)\n", "-r,--cvmfs-repos=<repos>");
	printf( " %-30s Allow repository switching when using CVMFS.\n","   --cvmfs-repo-switching");
	printf( " %-30s (deprecated) Set CVMFS common cache directory.    (PARROT_CVMFS_ALIEN_CACHE)\n","   --cvmfs-alien-cache=<dir>");
	printf( " %-30s (deprecated) Disable CVMFS common cache directory.\n","   --cvmfs-disable-alien-cache");
	printf("\n");
	printf( "Debug flags are: ");
	debug_flags_print(stdout);
	printf( "\n\n");
	printf( "Enabled filesystems are:");

	{
		char *key;
		void *value;
		hash_table_firstkey(available_services);
		while(hash_table_nextkey(available_services, &key, &value)) {
			printf(" %s", key);
		}
	}

	printf("\n");

	if(pfs_service_lookup("cvmfs")) {
		printf( "\ncvmfs compilation flags: " CCTOOLS_CVMFS_BUILD_FLAGS);
	}

	printf("\n");
	exit(1);
}

/*
For all of the signals that we handle, we want to
run the handler without interruption from other signals.
*/

void install_handler( int sig, void (*handler)(int sig))
{
	struct sigaction s;

	s.sa_handler = handler;
	sigfillset(&s.sa_mask);
	s.sa_flags = 0;

	sigaction(sig,&s,0);
}

static void ignore_signal( int sig )
{
}

void pfs_abort()
{
	kill(getpid(),SIGTERM);
	exit(1);
}

/*
Other less deadly signals we simply pass through to the root
of our children for its consideration.
*/

static void pass_through( int sig )
{
	kill(root_pid, sig);
}

/*
Here is the meat and potatoes.  We have discovered that
something interesting has happened to this pid. Decode
the event and take the appropriate action.
*/

static void handle_event( pid_t pid, int status, struct rusage *usage )
{
	struct pfs_process *p;
	unsigned long message;

	p = pfs_process_lookup(pid);
	if(!p) {
		debug(D_PROCESS,"ignoring event %d for unknown pid %d",status,pid);
		return;
	}

	if (WIFSTOPPED(status) && WSTOPSIG(status) == (SIGTRAP|0x80)) {
		/* The common case, a syscall delivery stop. */
		pfs_dispatch(p);
	} else if (status>>8 == (SIGTRAP | (PTRACE_EVENT_CLONE<<8)) || status>>8 == (SIGTRAP | (PTRACE_EVENT_FORK<<8)) || status>>8 == (SIGTRAP | (PTRACE_EVENT_VFORK<<8))) {
		pid_t cpid;
		struct pfs_process *child;
		INT64_T clone_files;

		if (tracer_getevent(p->tracer, &message) == -1)
			return;
		cpid = message;
		debug(D_PROCESS, "pid %d cloned %d",pid,cpid);
		assert(p->nsyscalls > 0);

		if(status>>8 == (SIGTRAP | (PTRACE_EVENT_FORK<<8)) || status>>8 == (SIGTRAP | (PTRACE_EVENT_VFORK<<8))) {
			clone_files = 0;
		} else {
			clone_files = p->syscall_args[0]&CLONE_FILES;
		}
		child = pfs_process_create(cpid,p,p->syscall_args[0]&CLONE_THREAD,clone_files);
		child->syscall_result = 0;
		if (tracer_continue(p->tracer,0) == -1) /* child starts stopped. */
			return;
	} else if (status>>8 == (SIGTRAP | (PTRACE_EVENT_EXEC<<8))) {
		pfs_process_exec(p);
		if (tracer_continue(p->tracer,0) == -1)
			return;
	} else if (status>>8 == (SIGTRAP | (PTRACE_EVENT_EXIT<<8)) || WIFEXITED(status) || WIFSIGNALED(status)) {
		/* In my own testing, if we use PTRACE_O_TRACEEXIT then we never get
		 * WIFEXITED(status) || WIFSIGNALED(status).  There may be corner cases
		 * (maybe in the future) where we do receive e.g. WIFSIGNALED(status),
		 * possibly due to SIGKILL. The ptrace manual says as much. So, I'm
		 * leaving that check in to handle the possibility. I also changed the
		 * above check if pfs_process_lookup(pid) fails to simply ignore the
		 * failure. Conceivably, we could receive PTRACE_EVENT_EXIT (due to
		 * normal exit) followed by an event due to SIGKILL. By the time we
		 * process SIGKILL, we have already destroyed our process data
		 * structures.
		 */
		struct rusage _usage;
		if (status>>8 == (SIGTRAP | (PTRACE_EVENT_EXIT<<8))) {
			debug(D_DEBUG, "pid %d received PTRACE_EVENT_EXIT",pid);
			if (tracer_getevent(p->tracer, &message) == -1)
				return;
			status = message;
		}
		if (WIFEXITED(status))
			debug(D_PROCESS,"pid %d exited normally with code %d",pid,WEXITSTATUS(status));
		else if (WIFSIGNALED(status))
			debug(D_PROCESS,"pid %d exited abnormally with signal %d (%s)",pid,WTERMSIG(status),string_signal(WTERMSIG(status)));
		else
			debug(D_PROCESS,"pid %d is exiting with status: %d",pid,status);
		pfs_process_stop(p,status,&_usage);
		if (pid == root_pid)
			root_exitstatus = status;
	} else if (WIFSTOPPED(status)) {
		int signum = WSTOPSIG(status);
		siginfo_t info;
		if(linux_available(3,4,0) && ((status>>16) == PTRACE_EVENT_STOP) && (signum == SIGTRAP || signum == 0)) {
			/* This is generic PTRACE_EVENT_STOP (but not group-stop):
			 *
			 *     Stop induced by PTRACE_INTERRUPT command, or group-stop, or
			 *     initial ptrace-stop when a new child is attached (only if
			 *     attached  using  PTRACE_SEIZE), or PTRACE_EVENT_STOP if
			 *     PTRACE_SEIZE was used.
			 */
			debug(D_DEBUG, "%d received PTRACE_EVENT_STOP, continuing...", (int)pid);
			if (tracer_continue(p->tracer, 0) == -1)
				return;
		} else if((linux_available(3,4,0) && ((status>>16) == PTRACE_EVENT_STOP)) || (!linux_available(3,4,0) && SIG_ISSTOP(signum) && ptrace(PTRACE_GETSIGINFO, pid, 0, &info) == -1 && errno == EINVAL)) {
			/* group-stop, `man ptrace` for more information */
			debug(D_PROCESS, "process %d has group-stopped due to signal %d (%s) (state %d)",pid,signum,string_signal(signum),p->state);
			assert(SIG_ISSTOP(signum));
			if (!linux_available(3,4,0)) {
				static int notified = 0;
				if (!notified) {
					debug(D_NOTICE, "The ptrace interface cannot handle group-stop for this Linux version. This may not work...");
					notified = 1;
				}
			}
			if (tracer_listen(p->tracer) == -1)
				return;
		} else {
			/* signal-delivery-stop */
			debug(D_PROCESS,"pid %d received signal %d (%s) (state %d)",pid,signum,string_signal(signum),p->state);
			switch(signum) {
				/* There are 4 process stop signals: SIGTTIN, SIGTTOU, SIGSTOP, and SIGTSTP.
				 * The Open Group Base Specifications Issue 6
				 * IEEE Std 1003.1, 2004 Edition
				 * Also mentioned in `man ptrace`.
				 */
				case SIGSTOP:
					/* Black magic to get threads working on old Linux kernels... */

					if(p->nsyscalls == 0) { /* stop before we begin running the process */
						debug(D_DEBUG, "suppressing bootstrap SIGSTOP for %d",pid);
						signum = 0; /* suppress delivery */
					}
					break;
				case SIGTSTP:
					break;
				case SIGSEGV: {
					buffer_t B[1];
					buffer_init(B);
					if (ptrace(PTRACE_GETSIGINFO, pid, 0, &info) == 0) {
						if (info.si_code == SEGV_MAPERR) {
							debug(D_PROCESS, "pid %d faulted on address %p (unmapped)", pid, info.si_addr);
						} else if (info.si_code == SEGV_ACCERR) {
							debug(D_PROCESS, "pid %d faulted on address %p (permissions)", pid, info.si_addr);
						} else {
							debug(D_PROCESS, "pid %d faulted on address %p", pid, info.si_addr);
						}
					} else {
						debug(D_DEBUG, "couldn't get signal info: %s", strerror(errno));
					}
					pfs_table::mmap_proc(pid, B);
					debug(D_DEBUG, "%d maps:\n%s", pid, buffer_tostring(B));
					buffer_free(B);
					break;
				}
			}
			if (tracer_continue(p->tracer,signum) == -1) /* deliver (or not) the signal */
				return;
		}
	} else {
		fatal("pid %d stopped with strange status %d",pid,status);
	}
}

struct timeval clock_to_timeval( clock_t c )
{
	struct timeval result;
	result.tv_sec = c/CLOCKS_PER_SEC;
	result.tv_usec = (c - result.tv_sec)*1000000/CLOCKS_PER_SEC;
	return result;
}

static volatile sig_atomic_t attached_and_ready = 0;
static void set_attached_and_ready (int sig)
{
	assert(sig == SIGUSR1);
	attached_and_ready = 1;
}

void write_rval(const char* message, int status) {
	FILE *file = fopen(pfs_write_rval_file, "w+");
	if(file) {
		fprintf(file, "%s\n%d\n", message, status);
		fclose(file);
	}

}

static int get_maxfd (void)
{
	struct rlimit rl;
	if (getrlimit(RLIMIT_NOFILE, &rl) == -1)
		fatal("getrlimit: %s", strerror(errno));
	if (rl.rlim_max == RLIM_INFINITY)
		rl.rlim_max = 1<<20; /* 2^20 fd should be enough for anyone! */
	debug(D_DEBUG, "RLIMIT_NOFILE: %" PRId64, (int64_t)rl.rlim_max);
	rl.rlim_cur = rl.rlim_max;
	if (setrlimit(RLIMIT_NOFILE, &rl) == -1)
		fatal("setrlimit: %s", strerror(errno));
	return rl.rlim_max;
}

struct pfswait {
	pid_t pid;
	int status;
	struct rusage usage;
};

static int pfswait (struct pfswait *p, pid_t pid, int block)
{
	int flags = WUNTRACED|__WALL;
	if (!block)
		flags |= WNOHANG;
	if (pid > 0)
		debug(D_PROCESS, "waiting for blocking event from process %d\n", pid);
	p->pid = wait4(pid, &p->status, flags, &p->usage);
#if 0 /* Enable this for extreme debugging... */
	debug(D_DEBUG, "%d = wait4(%d, %p, %d, %p)", (int)p->pid, pid, &p->status, flags, &p->usage);
#endif
	if (p->pid == -1) {
		debug(D_DEBUG, "wait4: %s", strerror(errno));
		if (errno == ECHILD) {
			debug(D_FATAL, "No children to wait for? Cleaning up...");
			pfs_process_kill_everyone(SIGKILL);
			abort();
		}
		return 0;
	} else if (p->pid == 0) {
		return 0;
	}
	return 1;
}

int main( int argc, char *argv[] )
{
	int c;
	int chose_auth = 0;
	char *tickets = NULL;
	pid_t pid;
	struct pfs_process *p;
	char envlist[PATH_MAX] = "";
	int valgrind = 0;
	int envdebug = 0;
	int envauth = 0;
	std::vector<pfs_service *> service_instances;

	random_init();
	pfs_resolve_init();

	debug_config(argv[0]);
	debug_config_file_size(0); /* do not rotate debug file by default */
	debug_config_fatal(pfs_process_killall);
	debug_config_getpid(pfs_process_getpid);

	/* Special file descriptors (currently the channel and the Parrot
	 * directory) are allocated from the top of our file descriptor pool. After
	 * setting up all special file descriptors, the root tracee will lower its
	 * RLIMIT_NOFILE so that special file descriptors are outside of its
	 * allocation/visibility. We're segmenting the file descriptor table.
	*/
	parrot_fd_start = parrot_fd_max = get_maxfd();

	install_handler(SIGQUIT,pfs_process_kill_everyone);
	install_handler(SIGILL,pfs_process_kill_everyone);
	install_handler(SIGABRT,pfs_process_kill_everyone);
	install_handler(SIGIOT,pfs_process_kill_everyone);
	install_handler(SIGBUS,pfs_process_kill_everyone);
	install_handler(SIGFPE,pfs_process_kill_everyone);
	install_handler(SIGSEGV,pfs_process_kill_everyone);
	install_handler(SIGTERM,pfs_process_kill_everyone);
	install_handler(SIGHUP,pass_through);
	install_handler(SIGINT,pass_through);
	install_handler(SIGIO,pfs_process_sigio);
	install_handler(SIGXFSZ,ignore_signal);

	/* For terminal stop signals, we ignore all.
	 *
	 * Parrot sometimes writes to the terminal using the debug library, we want
	 * to *never* be the foreground process group so we do not call tcsetpgrp
	 * to gain control. Instead we just ignore the terminal stop signals. If
	 * the terminal is configured with "-tostop" (see stty(1)), then the writes
	 * succeed even if we are not the foreground process group. Otherwise, the
	 * writes will return with EIO which is fine also. Reading from a terminal
	 * requires being the foreground process group (so far which we have never
	 * needed to do), we will receive EIO for all reads since we ignore
	 * SIGTTIN.
	 *
	 * SIGTSTOP occurs because the user suspends the foreground process group
	 * using ^Z. If this occurs, we also ignore it. We should probably never
	 * receive this signal since Parrot becomes a background process group as
	 * soon as its root tracee starts.
	 */

	install_handler(SIGTSTP,SIG_IGN);
	install_handler(SIGTTIN,SIG_IGN);
	install_handler(SIGTTOU,SIG_IGN);

	if(!isatty(0)) {
		pfs_master_timeout = 3600;
	}

	pfs_uid = getuid();
	pfs_gid = getgid();

	available_services = hash_table_create(0, 0);
	extern pfs_service *pfs_service_chirp;
	hash_table_insert(available_services, "chirp", pfs_service_chirp);
	extern pfs_service *pfs_service_multi;
	hash_table_insert(available_services, "multi", pfs_service_multi);
	extern pfs_service *pfs_service_anonftp;
	hash_table_insert(available_services, "anonftp", pfs_service_anonftp);
	extern pfs_service *pfs_service_ftp;
	hash_table_insert(available_services, "ftp", pfs_service_ftp);
	extern pfs_service *pfs_service_http;
	hash_table_insert(available_services, "http", pfs_service_http);
	extern pfs_service *pfs_service_grow;
	hash_table_insert(available_services, "grow", pfs_service_grow);
	extern pfs_service *pfs_service_hdfs;
	hash_table_insert(available_services, "hdfs", pfs_service_hdfs);
#ifdef HAS_GLOBUS_GSS
	extern pfs_service *pfs_service_gsiftp;
	hash_table_insert(available_services, "gsiftp", pfs_service_gsiftp);
	hash_table_insert(available_services, "gridftp", pfs_service_gsiftp);
#endif
#ifdef HAS_IRODS
	extern pfs_service *pfs_service_irods;
	hash_table_insert(available_services, "irods", pfs_service_irods);
#endif
#ifdef HAS_BXGRID
	extern pfs_service *pfs_service_bxgrid;
	hash_table_insert(available_services, "bxgrid", pfs_service_bxgrid);
#endif
#ifdef HAS_XROOTD
	extern pfs_service *pfs_service_xrootd;
	hash_table_insert(available_services, "xrootd", pfs_service_xrootd);
#endif
#ifdef HAS_CVMFS
	extern pfs_service *pfs_service_cvmfs;
	hash_table_insert(available_services, "cvmfs", pfs_service_cvmfs);
#endif

	const char *s;
	char *key;
	char *value = NULL;

	s = getenv("PARROT_BLOCK_SIZE");
	if(s) pfs_service_set_block_size(string_metric_parse(s));

	s = getenv("PARROT_MOUNT_FILE");
	if(s) pfs_mountfile_parse_file(s);

	s = getenv("PARROT_MOUNT_STRING");
	if(s) pfs_mountfile_parse_string(s);

	s = getenv("PARROT_FORCE_STREAM");
	if(s) pfs_force_stream = 1;

	s = getenv("PARROT_FORCE_CACHE");
	if(s) pfs_force_cache = 1;

	s = getenv("PARROT_FOLLOW_SYMLINKS");
	if(s) pfs_follow_symlinks = atoi(s);

	s = getenv("PARROT_SESSION_CACHE");
	if(s) pfs_session_cache = 1;

	s = getenv("PARROT_HOST_NAME");
	if(s) pfs_false_uname = xxstrdup(pfs_false_uname);

	s = getenv("PARROT_UID");
	if(s) pfs_uid = atoi(s);

	s = getenv("PARROT_GID");
	if(s) pfs_gid = atoi(s);

	s = getenv("PARROT_TIMEOUT");
	if(s) pfs_master_timeout = string_time_parse(s);

	s = getenv("PARROT_FORCE_SYNC");
	if(s) pfs_force_sync = 1;

	s = getenv("PARROT_LDSO_PATH");
	if(s) snprintf(pfs_ldso_path, sizeof(pfs_ldso_path), "%s", s);

	s = getenv("PARROT_DEBUG_FLAGS");
	if(s) {
		char *x = xxstrdup(s);
		int nargs;
		char **args;
		if(string_split(x,&nargs,&args)) {
			for(int i=0;i<nargs;i++) {
				debug_flags_set(args[i]);
			}
		}
		free(x);
		envdebug = 1;
	}

	s = getenv("PARROT_CHIRP_AUTH");
	if(s) {
		char *x = xxstrdup(s);
		int nargs;
		char **args;
		if(string_split(x,&nargs,&args)) {
			for(int i=0;i<nargs;i++) {
				if (!auth_register_byname(optarg))
					fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
				chose_auth = 1;
			}
		}
		free(x);
		envauth = 1;
	}

	s = getenv("PARROT_USER_PASS");
	if(s) {
		char *x = xxstrdup(s);
		int nargs;
		char **args;
		if(string_split(x,&nargs,&args)) {
			pfs_password_cache = password_cache_init(args[0], args[1]);
		}
	}

	s = getenv("TMPDIR");
	if(s) {
		snprintf(sys_temp_dir, sizeof(sys_temp_dir), "%s", s);
	}

	s = getenv("PARROT_TEMP_DIR");
	if(s) {
		snprintf(pfs_temp_dir, sizeof(pfs_temp_dir), "%s", s);
	} else {
		assert(sys_temp_dir[0]);
		string_nformat(pfs_temp_dir, sizeof(pfs_temp_dir), "%s/parrot.%d", sys_temp_dir, getuid());
	}

	s = getenv("PARROT_CVMFS_ALIEN_CACHE");
	if(s) {
		snprintf(pfs_cvmfs_alien_cache_dir, sizeof(pfs_cvmfs_alien_cache_dir), "%s", s);
	} else {
		assert(pfs_temp_dir[0]);
		string_nformat(pfs_cvmfs_alien_cache_dir, sizeof(pfs_cvmfs_alien_cache_dir), "%s/cvmfs", pfs_temp_dir);
	}

	s = getenv("PARROT_CVMFS_OPTION_FILE");
	if (s) {
		string_nformat(pfs_cvmfs_option_file, sizeof(pfs_cvmfs_option_file), "%s", s);
	} else {
		memset(pfs_cvmfs_option_file, 0, sizeof(pfs_cvmfs_option_file));
	}

	static const struct option long_options[] = {
		{"auto-decompress", no_argument, 0, 'Z'},
		{"block-size", required_argument, 0, 'b'},
		{"channel-auth", no_argument, 0, 'C'},
		{"check-driver", required_argument, 0, LONG_OPT_CHECK_DRIVER },
		{"chirp-auth",  required_argument, 0, 'a'},
		{"cvmfs-repos", required_argument, 0, 'r'},
		{"cvmfs-alien-cache", required_argument, 0, LONG_OPT_CVMFS_ALIEN_CACHE},
		{"cvmfs-disable-alien-cache", no_argument, 0, LONG_OPT_CVMFS_DISABLE_ALIEN_CACHE},
		{"cvmfs-repo-switching", no_argument, 0, LONG_OPT_CVMFS_REPO_SWITCHING},
		{"cvmfs-repos", required_argument, 0, 'r'},
		{"cvmfs-option", required_argument, 0, LONG_OPT_CVMFS_OPTION},
		{"cvmfs-option-file", required_argument, 0, LONG_OPT_CVMFS_OPTION_FILE},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-level-irods", required_argument, 0, 'I'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"disable-service", required_argument, 0, LONG_OPT_DISABLE_SERVICE},
		{"dynamic-mounts", no_argument, 0, LONG_OPT_DYNAMIC_MOUNTS },
		{"env-list", required_argument, 0, 'e'},
		{"ext-image", required_argument, 0, LONG_OPT_EXT_IMAGE},
		{"fake-setuid", no_argument, 0, LONG_OPT_FAKE_SETUID},
		{"gid", required_argument, 0, 'G'},
		{"help", no_argument, 0, 'h'},
		{"helper", no_argument, 0, LONG_OPT_HELPER},
		{"hostname", required_argument, 0, 'N'},
		{"ld-path", required_argument, 0, 'l'},
		{"mount", required_argument, 0, 'M'},
		{"name-list", required_argument, 0, 'n'},
		{"no-checksums", no_argument, 0, 'k'},
		{"no-chirp-catalog", no_argument, 0, 'Q'},
		{"no-follow-symlinks", no_argument, 0, 'f'},
		{"no-helper", no_argument, 0, 'H'},
		{"no-optimize", no_argument, 0, 'D'},
		{"no-flock", no_argument, 0, LONG_OPT_NO_FLOCK},
		{"no-set-foreground", no_argument, 0, LONG_OPT_NO_SET_FOREGROUND},
		{"paranoid", no_argument, 0, 'P'},
		{"parrot-path", required_argument, 0, LONG_OPT_PARROT_PATH},
		{"pid-fixed", no_argument, 0, LONG_OPT_PID_FIXED},
		{"pid-warp", no_argument, 0, LONG_OPT_PID_WARP},
		{"proxy", required_argument, 0, 'p'},
		{"root-checksum", required_argument, 0, 'R'},
		{"session-caching", no_argument, 0, 'S'},
		{"stats-file", required_argument, 0, LONG_OPT_STATS_FILE},
		{"status-file", required_argument, 0, 'c'},
		{"stream-no-cache", no_argument, 0, 's'},
		{"sync-write", no_argument, 0, 'Y'},
		{"syscall-disable-debug", no_argument, 0, LONG_OPT_SYSCALL_DISABLE_DEBUG},
		{"syscall-table", no_argument, 0, 'W'},
		{"tab-file", required_argument, 0, 'm'},
		{"tempdir", required_argument, 0, 't'},
		{"tickets", required_argument, 0, 'i'},
		{"time-stop", no_argument, 0, LONG_OPT_TIME_STOP},
		{"time-warp", no_argument, 0, LONG_OPT_TIME_WARP},
		{"timeout", required_argument, 0, 'T'},
		{"uid", required_argument, 0, 'U'},
		{"username", required_argument, 0, 'u'},
		{"valgrind", no_argument, 0, LONG_OPT_VALGRIND},
		{"version", no_argument, 0, 'v'},
		{"is-running", no_argument, 0, LONG_OPT_IS_RUNNING},
		{"with-checksums", no_argument, 0, 'K'},
		{"with-snapshots", no_argument, 0, 'F'},
		{"work-dir", required_argument, 0, 'w'},
		{0,0,0,0}
	};

	while((c=getopt_long(argc,argv,"+ha:b:B:c:Cd:DFfG:e:Hi:I:kKl:m:n:M:N:o:O:p:PQr:R:sSt:T:U:u:vw:WY", long_options, NULL)) > -1) {
		switch(c) {
		case 'a':
			if(envauth) {
				auth_clear();
				envauth = 0;
			}
			if(!auth_register_byname(optarg)) {
				fprintf(stderr,"unknown auth type: %s\n",optarg);
				return 1;
			}
			chose_auth = 1;
			break;
		case 'b':
			pfs_service_set_block_size(string_metric_parse(optarg));
			break;
		case 'B':
			pfs_service_set_block_size(string_metric_parse(optarg));
			break;
		case 'c':
			pfs_write_rval = 1;
			pfs_write_rval_file = optarg;
			break;
		case 'C':
			ftp_lite_data_channel_authentication = 1;
			break;
		case 'd':
			if(envdebug) {
				debug_flags_clear();
				envdebug = 0;
			}
			if(!debug_flags_set(optarg)) show_help(argv[0]);
			break;
		case 'D':
			pfs_enable_small_file_optimizations = 0;
			break;
		case 'e':
			snprintf(envlist, sizeof(envlist), "%s", optarg);
			break;
		case 'F':
			pfs_force_cache = 1;
			break;
		case 'f':
			pfs_follow_symlinks = 0;
			break;
		case 'G':
			pfs_gid = atoi(optarg);
			break;
		case 'H':
			/* deprecated */
			break;
		case 'I':
			pfs_irods_debug_level = atoi(optarg);
			break;
		case 'i':
			tickets = strdup(optarg);
			break;
		case 'k':
			pfs_checksum_files = 0;
			break;
		case 'K':
			pfs_checksum_files = 1;
			break;
		case 'l':
			snprintf(pfs_ldso_path, sizeof(pfs_ldso_path), "%s", optarg);
			break;
		case 'm':
			pfs_mountfile_parse_file(optarg);
			break;
		case 'M':
			pfs_mountfile_parse_string(optarg);
			break;
		case 'n':
			if(access(optarg, F_OK) != -1) {
				fprintf(stderr, "The namelist file (%s) has already existed. Please delete it first or refer to another namelist file!!\n", optarg);
				return 1;
			}
			namelist_file = fopen(optarg, "a");
			if(!namelist_file) {
				debug(D_DEBUG, "Can not open namelist file: %s", optarg);
				return 1;
			}
			namelist_table = hash_table_create(0, 0);
			if(!namelist_table) {
				debug(D_DEBUG, "Failed to create hash table for namelist!\n");
				return 1;
			}
			char cmd[PATH_MAX];
			if(snprintf(cmd, sizeof(cmd), "find /lib*/ -name ld-linux*>>%s 2>/dev/null", optarg) >= 0)
				system(cmd);
			else {
				debug(D_DEBUG, "writing ld-linux* into namelist file failed.");
				return 1;
			}
			fprintf(namelist_file, "/bin/sh\n");
			break;
		case 'N':
			pfs_false_uname = xxstrdup(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'p':
			pfs_cvmfs_http_proxy = xxstrdup(optarg);
			break;
		case 'P':
			pfs_paranoid_mode = 1;
			break;
		case 'Q':
			chirp_global_inhibit_catalog(1);
			break;
		case LONG_OPT_CVMFS_CONFIG:
			pfs_cvmfs_config_arg = optarg;
			break;
		case 'r':
			pfs_cvmfs_repo_arg = optarg;
			break;
		case LONG_OPT_CVMFS_REPO_SWITCHING:
			pfs_cvmfs_repo_switching = true;
			break;
		case LONG_OPT_CVMFS_ALIEN_CACHE:
			snprintf(pfs_cvmfs_alien_cache_dir,sizeof(pfs_cvmfs_alien_cache_dir),"%s",optarg);
			break;
		case LONG_OPT_CVMFS_DISABLE_ALIEN_CACHE:
			pfs_cvmfs_enable_alien = false;
			break;
		case LONG_OPT_CVMFS_OPTION_FILE:
			snprintf(pfs_cvmfs_option_file, sizeof(pfs_cvmfs_option_file), "%s", optarg);
			break;
		case LONG_OPT_CVMFS_OPTION:
			if (!pfs_cvmfs_options) {
				pfs_cvmfs_options = jx_object(NULL);
				assert(pfs_cvmfs_options);
			}
			s = xxstrdup(optarg);
			// Somewhat lazy option parsing: just split on the first =
			// No whitespace stripping, so don't write CVMFS_OPTION = x
			key = strtok_r((char *) s, "=", &value);
			if (!(key && value)) {
				fprintf(stderr, "Malformed CVMFS option\n");
				exit(EXIT_FAILURE);
			}
			jx_insert(pfs_cvmfs_options, jx_string(key), jx_string(value));
			break;
		case 'R':
			pfs_root_checksum = optarg;
			pfs_checksum_files = 1;
			break;
		case 's':
			pfs_force_stream = 1;
			break;
		case 'S':
			pfs_force_cache = 1;
			pfs_session_cache = 1;
			break;
		case 't':
			snprintf(pfs_temp_dir,sizeof(pfs_temp_dir),"%s",optarg);
			break;
		case 'T':
			pfs_master_timeout = string_time_parse(optarg);
			break;
		case 'U':
			pfs_uid = atoi(optarg);
			break;
		case 'u':
			pfs_username = optarg;
			break;
		case 'Y':
			pfs_force_sync = 1;
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
			break;
		case 'w':
			pfs_initial_working_directory = optarg;
			break;
		case 'W':
			pfs_syscall_totals32 = (int*) calloc(SYSCALL32_MAX,sizeof(int));
			pfs_syscall_totals64 = (int*) calloc(SYSCALL64_MAX,sizeof(int));
			break;
		case LONG_OPT_NO_SET_FOREGROUND:
			set_foreground = 0;
			break;
		case LONG_OPT_HELPER:
			pfs_use_helper = 1;
			break;
		case LONG_OPT_VALGRIND:
			valgrind = 1;
			break;
		case LONG_OPT_CHECK_DRIVER:
			if(pfs_service_lookup(optarg)) {
				printf("%s is enabled\n",optarg);
				return 0;
			} else {
				printf("%s is not enabled\n",optarg);
				return 1;
			}
		case LONG_OPT_SYSCALL_DISABLE_DEBUG:
			pfs_syscall_disable_debug = 1;
			break;
		case LONG_OPT_FAKE_SETUID:
			pfs_fake_setuid = 1;
			pfs_fake_setgid = 1;
			break;
		case LONG_OPT_DYNAMIC_MOUNTS:
			pfs_allow_dynamic_mounts = 1;
			break;
		case LONG_OPT_IS_RUNNING: {
			char buf[4096];
			if (parrot_version(buf, sizeof(buf)) >= 0) {
				printf("%s\n", buf);
				exit(EXIT_SUCCESS);
			} else {
				exit(EXIT_FAILURE);
			}
			break;
		}
		case LONG_OPT_TIME_STOP:
			pfs_time_mode = PFS_TIME_MODE_STOP;
			pfs_use_helper = 1;
			break;
		case LONG_OPT_TIME_WARP:
			pfs_time_mode = PFS_TIME_MODE_WARP;
			pfs_use_helper = 1;
			break;
		case LONG_OPT_PARROT_PATH:
			// compatibility option for parrot_namespace
			break;
		case LONG_OPT_PID_FIXED:
			pfs_pid_mode = PFS_PID_MODE_FIXED;
			pfs_use_helper = 1;
			break;
		case LONG_OPT_PID_WARP:
			pfs_pid_mode = PFS_PID_MODE_WARP;
			pfs_use_helper = 1;
			break;
		case LONG_OPT_STATS_FILE:
			free(stats_file);
			stats_file = xxstrdup(optarg);
			break;
		case LONG_OPT_DISABLE_SERVICE:
			if (!hash_table_remove(available_services, optarg)) {
				fprintf(stderr, "warning: unknown service %s\n", optarg);
			}
			break;
		case LONG_OPT_NO_FLOCK:
			pfs_no_flock = 1;
			break;
		case LONG_OPT_EXT_IMAGE: {
			char service[128];
			char image[PATH_MAX] = {0};
			char mountpoint[PATH_MAX] = {0};
			static unsigned ext_no = 0;

			char *split = strchr(optarg, '=');
			if (!split) fatal("--ext must be specified as IMAGE=MOUNTPOINT");
			strncpy(image, optarg, MIN((size_t) (split - optarg), sizeof(image) - 1));
			strncpy(mountpoint, split + 1, sizeof(mountpoint) - 1);
			if (mountpoint[0] != '/') fatal("mountpoint for ext image %s must be an absolute path", image);
			struct pfs_service *s = pfs_service_ext_init(image, mountpoint);
			if (!s) fatal("failed to load ext image %s", image);

			service_instances.push_back(s);
			snprintf(service, sizeof(service), "/ext_%u", ext_no++);
			hash_table_insert(available_services, &service[1], s);
			pfs_resolve_add_entry(mountpoint, service, R_OK|W_OK|X_OK);

			break;
		}
		default:
			show_help(argv[0]);
			break;
		}
	}

	if(optind>=argc) show_help(argv[0]);

	FILE *stats_out;
	if (stats_file) {
		stats_enable();
		stats_out = fopen(stats_file, "w");
		if (!stats_out)
			fatal("could not open stats file %s: %s", stats_file, strerror(errno));
	}

	{
		char buf[4096];
		if (parrot_version(buf, sizeof(buf)) >= 0) {
			fprintf(stderr, "sorry, parrot_run cannot be run inside of itself.\n");
			fprintf(stderr, "version already running is %s.\n",buf);
			exit(EXIT_FAILURE);
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if (!pfs_allow_dynamic_mounts) {
		pfs_resolve_seal_ns();
	}

	debug(D_PROCESS, "I am process %d in group %d in session %d",(int)getpid(),(int)getpgrp(),(int)getsid(0));
	{
		extern char **environ;
		buffer_t B;
		buffer_init(&B);
		debug(D_DEBUG, "command:");
		buffer_putfstring(&B, " - %s", argv[0]);
		for (int i = 1; argv[i]; i++)
			buffer_putfstring(&B, " \"%s\"", argv[i]);
		debug(D_DEBUG, "%s", buffer_tostring(&B));
		debug(D_DEBUG, "environment:");
		for (int i = 0; environ[i]; i++)
			debug(D_DEBUG, " - %s", environ[i]);
		buffer_free(&B);
	}

	get_linux_version(argv[0]);

	if (envlist[0]) {
		extern char **environ;
		if(access(envlist, F_OK) == 0)
			fatal("The envlist file (%s) has already existed. Please delete it first or refer to another envlist file!!\n", envlist);
		FILE *fp = fopen(envlist, "w");
		if(!fp)
			fatal("Can not open envlist file: %s", envlist);
		for (int i = 0; environ[i]; i++)
			fprintf(fp, "%s%c", environ[i], '\0');
		char working_dir[PATH_MAX];
		::getcwd(working_dir,sizeof(working_dir));
		if(working_dir == NULL)
			fatal("Can not obtain the current working directory!");
		fprintf(fp, "PWD=%s\n", working_dir);
		fclose(fp);
	}

	// if -p not given, check if HTTP_PROXY is set.
	if(!pfs_cvmfs_http_proxy && getenv("HTTP_PROXY")) {
		pfs_cvmfs_http_proxy = xxstrdup(getenv("HTTP_PROXY"));
	}

	if (!create_dir(pfs_temp_dir, S_IRWXU))
		fatal("could not create directory '%s': %s", pfs_temp_dir, strerror(errno));

	string_nformat(pfs_temp_per_instance_dir, sizeof(pfs_temp_per_instance_dir), "%s/parrot-instance.XXXXXX", pfs_temp_dir);

	if (mkdtemp(pfs_temp_per_instance_dir) == NULL)
		fatal("could not create directory '%s': %s", pfs_temp_per_instance_dir, strerror(errno));

	pfs_file_cache = file_cache_init(pfs_temp_dir);
	if(!pfs_file_cache) fatal("couldn't setup cache in %s: %s\n",pfs_temp_dir,strerror(errno));
	file_cache_cleanup(pfs_file_cache);

	string_nformat(pfs_cvmfs_locks_dir, sizeof(pfs_cvmfs_locks_dir), "%s/cvmfs_locks_XXXXXX", pfs_temp_per_instance_dir);

	if(mkdtemp(pfs_cvmfs_locks_dir) == NULL)
		fatal("could not create a cvmfs locks temporary directory: %s", strerror(errno));

	if(!chose_auth) auth_register_all();

	if(tickets) {
		auth_ticket_load(tickets);
		tickets = (char *)realloc(tickets, 0);
	} else if(getenv(CHIRP_CLIENT_TICKETS)) {
		auth_ticket_load(getenv(CHIRP_CLIENT_TICKETS));
	} else {
		auth_ticket_load(NULL);
	}

	if(!pfs_channel_init(channel_size*1024*1024)) fatal("couldn't establish I/O channel");

	{
		int fd;

		char buf[PATH_MAX];
		string_nformat(buf, sizeof(buf), "%s/parrot-fd.XXXXXX", pfs_temp_per_instance_dir);

		if (mkdtemp(buf) == NULL)
			fatal("could not create parrot-fd temporary directory: %s", strerror(errno));
		fd = open(buf, O_RDONLY|O_DIRECTORY);
		if (fd == -1)
			fatal("could not open tempdir: %s", strerror(errno));
		parrot_dir_fd = --parrot_fd_start;
		if (dup2(fd, parrot_dir_fd) == -1) {
			fatal("could not dup2(%d, parrot_dir_fd = %d): %s", fd, parrot_dir_fd, strerror(errno));
		}
		close(fd);
	}

	pid_t pfs_watchdog_pid = -2;
	if (pfs_paranoid_mode) {
		pfs_watchdog_pid = pfs_paranoia_setup();
		if (pfs_watchdog_pid < 0) {
			fatal("couldn't initialize paranoid mode.");
		} else {
			debug(D_PROCESS,"watchdog PID %d",pfs_watchdog_pid);
		}
	}

	/* XXX Notes on strange code ahead:
	 *
	 * Previously we had a really simple synchronization mechanism whereby the
	 * child would raise(SIGSTOP) and wait for the parent to attach. Apparently
	 * this does not work on obscure Linux flavors (Cray Linux 2.6.32) so we
	 * need to be more fancy. The exact problem appears to be that we cannot
	 * PTRACE_ATTACH a stopped process and then do PTRACE_SETOPTIONS.
	 *
	 * So the solution is: only attach when the child is spinning.
	 *
	 * This requires awkward signal gymnastics:
	 */

	pid = fork();
	if(pid>0) {
		pid_t wpid;
		int status;
		debug(D_PROCESS,"pid %d started",pid);
		do {
			wpid = waitpid(pid, &status, WUNTRACED);
		} while (wpid != pid);
		if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGSTOP)
			fatal("child did not stop as expected!");
		kill(pid, SIGCONT);
		do {
			wpid = waitpid(pid, &status, WCONTINUED);
		} while (wpid != pid);
		if (!WIFCONTINUED(status))
			fatal("child did not continue as expected!");
	} else if(pid==0) {
		setenv("PARROT_ENABLED", "TRUE", 1);
		if (pfs_use_helper)
			pfs_helper_init();
		pfs_paranoia_payload();
		pfs_process_bootstrapfd();
		if(set_foreground) {
			setpgrp();
			int fd = open("/dev/tty", O_RDWR);
			if (fd >= 0) {
				tcsetpgrp(fd, getpgrp());
				close(fd);
			}
		}
		if (valgrind) {
			int i;
			char **nargv = string_array_new();
			nargv = string_array_append(nargv, "sh");
			nargv = string_array_append(nargv, "-c");
			nargv = string_array_append(nargv, "trap 'exec \"$@\"' USR1; kill -STOP $$; while true; do true; done;");
			nargv = string_array_append(nargv, "--");
			for (i = optind; argv[i]; i++)
				nargv = string_array_append(nargv, argv[i]);
			debug(D_DEBUG, "execvp(\"sh\", [\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", ...])", nargv[0], nargv[1], nargv[2], nargv[3], nargv[4]);
			execvp("sh", (char *const *)nargv);
		} else {
			signal(SIGUSR1, set_attached_and_ready);
			raise(SIGSTOP); /* synchronize with parent, above */
			while (!attached_and_ready) ; /* spin waiting to be traced (NO SLEEPING/STOPPING) */
			execvp(argv[optind],&argv[optind]);
		}
		fprintf(stderr, "unable to execute %s: %s\n", argv[optind], strerror(errno));
		fflush(stderr);
		if(pfs_write_rval) {
			write_rval("noexec", 0);
		}
		_exit(1);
	} else {
		debug(D_NOTICE,"unable to fork %s: %s",argv[optind],strerror(errno));
		if(pfs_write_rval) {
			write_rval("nofork", 0);
		}
		exit(1);
	}

	CRITICAL_BEGIN

	root_pid = pid;
	debug(D_PROCESS,"attaching to pid %d",pid);
	if (tracer_attach(pid) == -1) {
		if (errno == EPERM) {
			fprintf(stderr,
				"The `ptrace` system call appears to be disabled.\n"
				"Some possible causes:\n"
				" - Syscall filtering (e.g. seccomp) is in place. Some versions of Docker do\n   this inside containers.\n"
				" - The program that launched Parrot used `PR_SET_DUMPABLE` to disable debugging\n   for this process.\n"
				" - Your system's security framework (SELinux, Yama, etc.) disables ptrace.\n");
		}
		fatal("could not trace child");
	}
	kill(pid, SIGUSR1);
	p = pfs_process_create(pid,NULL,0,0);
	if(!p) {
		if(pfs_write_rval) {
			write_rval("noattach", 0);
		}
		kill(pid,SIGKILL);
		fatal("unable to attach to pid %d: %s",pid,strerror(errno));
	}

	snprintf(p->name,sizeof(p->name),"%s",argv[optind]);

	/* We perform wait4 until there are no tracees left to wait for.
	 * Previously, we would wait for a process, handle the event, then repeat.
	 * This caused problems with Java where threads would get stuck in a race
	 * condition with sched_yield/futex.
	 *
	 * The reason I discovered this solution is due to strace(1). I've long
	 * wondered why it chooses to wait for all tracees before processing events
	 * (you can see this if you strace strace).  After again seeing the
	 * sched_yield infinite loop again in #927, I recalled this peculiarity and
	 * decided to give the strace approach a try.  It apparently fixes the
	 * problem. I couldn't find any documentation on why strace does this.
	 */

	while(pfs_process_count()>0) {
		std::vector<struct pfswait> pevents;
		struct pfswait p;

		while (pfswait(&p, -1, !pevents.size())) {
			pevents.push_back(p);
		}
		if (pevents.size() == 0)
			break;

		for (std::vector<struct pfswait>::iterator it = pevents.begin(); it != pevents.end(); ++it) {
			if(it->pid == pfs_watchdog_pid) {
				if (WIFEXITED(it->status) || WIFSIGNALED(it->status)) {
					debug(D_NOTICE,"watchdog died unexpectedly; killing everyone");
					pfs_process_kill_everyone(SIGKILL);
					break;
				}
			} else {
				p = *it;
				do {
					wait_barrier = 0; /* reinitialize */
					handle_event(p.pid, p.status, &p.usage);
				} while (wait_barrier && pfswait(&p, it->pid, 1));
			}
		}
	}

	for (std::vector<pfs_service *>::iterator it = service_instances.begin(); it != service_instances.end(); ++it) {
		delete *it;
	}

	if(pfs_syscall_totals32) {
		printf("\nParrot System Call Summary:\n");
		printf("%" PRId64 " syscalls\n",pfs_syscall_count);
		printf("%" PRId64 " bytes read\n",pfs_read_count);
		printf("%" PRId64 " bytes written\n",pfs_write_count);

		printf("\n32-bit System Calls:\n");
		for(int i=0;i<SYSCALL32_MAX;i++) {
			if(pfs_syscall_totals32[i]) {
				printf("%-20s %d\n",tracer_syscall32_name(i),pfs_syscall_totals32[i]);
			}
		}

		#ifdef CCTOOLS_CPU_X86_64

		printf("\n64-bit System Calls:\n");
		for(int i=0;i<SYSCALL64_MAX;i++) {
			if(pfs_syscall_totals64[i]) {
				printf("%-20s %d\n",tracer_syscall64_name(i),pfs_syscall_totals64[i]);
			}
		}

		#endif
	}

	if(pfs_paranoid_mode) pfs_paranoia_cleanup();

	delete_dir(pfs_temp_per_instance_dir);

	if(namelist_table && namelist_file) {
		char *key;
		void *value;
		hash_table_firstkey(namelist_table);
		while(hash_table_nextkey(namelist_table, &key, &value)) {
			fprintf(namelist_file, "%s|%s\n", key, (char *)value);
		}
		hash_table_delete(namelist_table);
		fclose(namelist_file);
	}

	if (stats_file) {
		jx_pretty_print_stream(stats_get(), stats_out);
		fprintf(stats_out, "\n");
		fclose(stats_out);
	}

	if(WIFEXITED(root_exitstatus)) {
		int status = WEXITSTATUS(root_exitstatus);
		debug(D_PROCESS,"%s exited normally with status %d",argv[optind],status);
		if(pfs_write_rval) {
			write_rval("normal", status);
		}
		return status;
	} else {
		int signum = WTERMSIG(root_exitstatus);
		debug(D_PROCESS,"%s exited abnormally with signal %d (%s)",argv[optind],signum,string_signal(signum));
		if(pfs_write_rval) {
			write_rval("abnormal", signum);
		}
		return 1;
	}
}

/* vim: set noexpandtab tabstop=4: */
