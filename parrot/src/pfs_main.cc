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
#include "pfs_poll.h"
#include "pfs_process.h"
#include "pfs_service.h"
#include "ptrace.h"

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
#include "md5.h"
#include "password_cache.h"
#include "pfs_resolve.h"
#include "stringtools.h"
#include "tracer.h"
#include "xxmalloc.h"
#include "hash_table.h"
}

#include <fcntl.h>
#include <termio.h>
#include <termios.h>
#include <unistd.h>

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

#define SIG_ISSTOP(s) (s == SIGTTIN || s == SIGTTOU || s == SIGSTOP || s == SIGTSTP)

extern char **environ;
FILE *namelist_file;
struct hash_table *namelist_table;
int linux_major;
int linux_minor;
int linux_micro;

pid_t trace_this_pid = -1;

int pfs_master_timeout = 300;
struct file_cache *pfs_file_cache = 0;
struct password_cache *pfs_password_cache = 0;
int pfs_force_stream = 0;
int pfs_force_cache = 0;
int pfs_force_sync = 0;
int pfs_follow_symlinks = 1;
int pfs_session_cache = 0;
int pfs_use_helper = 0;
int pfs_checksum_files = 1;
int pfs_write_rval = 0;
int pfs_paranoid_mode = 0;
const char *pfs_write_rval_file = "parrot.rval";
int pfs_enable_small_file_optimizations = 1;

char sys_temp_dir[PFS_PATH_MAX] = "/tmp";
char pfs_temp_dir[PFS_PATH_MAX];

int *pfs_syscall_totals32 = 0;
int *pfs_syscall_totals64 = 0;

const char *pfs_root_checksum=0;
const char *pfs_initial_working_directory=0;

char *pfs_false_uname = 0;
char *pfs_ldso_path = 0;
uid_t pfs_uid = 0;
gid_t pfs_gid = 0;
const char * pfs_username = 0;

INT64_T pfs_syscall_count = 0;
INT64_T pfs_read_count = 0;
INT64_T pfs_write_count = 0;

const char * pfs_cvmfs_repo_arg = 0;
const char * pfs_cvmfs_config_arg = 0;
bool pfs_cvmfs_repo_switching = false;
char pfs_cvmfs_alien_cache_dir[PFS_PATH_MAX];
char pfs_cvmfs_locks_dir[PFS_PATH_MAX];
bool pfs_cvmfs_enable_alien  = true;

int pfs_irods_debug_level = 0;

/*
This process at the very top of the traced tree
and its final exit status, which we use to determine
our own exit status
*/

static pid_t root_pid = -1;
static int root_exitstatus = 0;
static int channel_size = 10;

enum {
	LONG_OPT_CVMFS_REPO_SWITCHING=UCHAR_MAX+1,
	LONG_OPT_CVMFS_CONFIG,
	LONG_OPT_CVMFS_DISABLE_ALIEN_CACHE,
	LONG_OPT_CVMFS_ALIEN_CACHE,
	LONG_OPT_HELPER,
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
	if(linux_available(3,15,3))
		debug(D_NOTICE,"parrot_run %d.%d.%s has not been tested on %s %s yet, this may not work",CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,name.sysname,name.release);
	else if (!linux_available(2,5,60))
		fatal("this version of Parrot requires at least kernel version 2.5.60");
}

static void pfs_helper_init( void )
{
	char helper_path[PFS_PATH_MAX];

	debug(D_DEBUG,"locating helper library...");

	sprintf(helper_path,"%s/lib/libparrot_helper.so",INSTALL_PATH);

	char * s = getenv("PARROT_HELPER");
	if(s) {
		debug(D_DEBUG,"PARROT_HELPER=%s",s);
		strcpy(helper_path,s);
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
	fprintf(stdout, "Use: %s [options] <command> ...\n",cmd);
	fprintf(stdout, "Where options and environment variables are:\n");
	fprintf(stdout, " %-30s Use these Chirp authentication methods.   (PARROT_CHIRP_AUTH)\n", "-a,--chirp-auth=<list>");
	fprintf(stdout, " %-30s Set the I/O block size hint.              (PARROT_BLOCK_SIZE)\n", "-b,--block-size=<bytes>");
	fprintf(stdout, " %-30s Print exit status information to <file>.\n", "-c,--status-file=<file>");
	fprintf(stdout, " %-30s Enable data channel authentication in GridFTP.\n", "-C,--channel-auth");
	fprintf(stdout, " %-30s Enable debugging for this sub-system.    (PARROT_DEBUG_FLAGS)\n", "-d,--debug=<name>");
	fprintf(stdout, " %-30s Disable small file optimizations.\n", "-D,--no-optimize");
	fprintf(stdout, " %-30s Record the environment variables at the starting point.\n", "-e,--env-list=<path>");
	fprintf(stdout, " %-30s Enable file snapshot caching for all protocols.\n", "-F,--with-snapshots");
	fprintf(stdout, " %-30s Disable following symlinks.\n", "-f,--no-follow-symlinks");
	fprintf(stdout, " %-30s Fake this gid; Real gid stays the same.          (PARROT_GID)\n", "-G,--gid=<num>");
	fprintf(stdout, " %-30s Show this screen.\n", "-h,--help");
	fprintf(stdout, " %-30s Enable use of helper library.\n", "--helper");
	fprintf(stdout, " %-30s Comma-delimited list of tickets to use for authentication.\n", "-i,--tickets=<files>");
	fprintf(stdout, " %-30s Set the debug level output for the iRODS driver.\n", "-I,--debug-level-irods=<num>");
	fprintf(stdout, " %-30s Checksum files where available.\n", "-K,--with-checksums");
	fprintf(stdout, " %-30s Do not checksum files.\n", "-k,--no-checksums");
	fprintf(stdout, " %-30s Path to ld.so to use.                      (PARROT_LDSO_PATH)\n", "-l,--ld-path=<path>");
	fprintf(stdout, " %-30s Record all the file names.\n", "-n,--name-list=<path>");
	fprintf(stdout, " %-30s Use this file as a mountlist.             (PARROT_MOUNT_FILE)\n", "-m,--ftab-file=<file>");
	fprintf(stdout, " %-30s Mount (redirect) /foo to /bar.          (PARROT_MOUNT_STRING)\n", "-M,--mount=/foo=/bar");
	fprintf(stdout, " %-30s Pretend that this is my hostname.          (PARROT_HOST_NAME)\n", "-N,--hostname=<name>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal) (PARROT_DEBUG_FILE)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate debug files of this size. (default 10M, 0 disables) (PARROT_DEBUG_FILE_SIZE)\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Use this proxy server for HTTP requests.         (HTTP_PROXY)\n", "-p,--proxy=<hst:p>");
	fprintf(stdout, " %-30s Enable paranoid mode for identity boxing mode.\n", "-P,--paranoid");
	fprintf(stdout, " %-30s Inhibit catalog queries to list /chirp.\n", "-Q,--no-chirp-catalog");
	fprintf(stdout, " %-30s CVMFS common configuration.               (PARROT_CVMFS_CONFIG)\n", "   --cvmfs-config=<config>");
	fprintf(stdout, " %-30s CVMFS repositories to enable.             (PARROT_CVMFS_REPO)\n", "-r,--cvmfs-repos=<repos>");
	fprintf(stdout, " %-30s Allow repository switching when using CVMFS.\n","   --cvmfs-repo-switching");
	fprintf(stdout, " %-30s Set CVMFS common cache directory.         (PARROT_CVMFS_ALIEN_CACHE)\n","   --cvmfs-alien-cache=<dir>");
	fprintf(stdout, " %-30s Disable CVMFS common cache directory.\n","   --cvmfs-disable-alien-cache");
	fprintf(stdout, " %-30s Enforce this root filesystem checksum, where available.\n", "-R,--root-checksum=<cksum>");
	fprintf(stdout, " %-30s Use streaming protocols without caching.(PARROT_FORCE_STREAM)\n", "-s,--stream-no-cache");
	fprintf(stdout, " %-30s Enable whole session caching for all protocols.\n", "-S,--session-caching");
	fprintf(stdout, " %-30s Where to store temporary files.             (PARROT_TEMP_DIR)\n", "-t,--tempdir=<dir>");
	fprintf(stdout, " %-30s Maximum amount of time to retry failures.    (PARROT_TIMEOUT)\n", "-T,--timeout=<time>");
	fprintf(stdout, " %-30s Fake this unix uid; Real uid stays the same.     (PARROT_UID)\n", "-U,--uid=<num>");
	fprintf(stdout, " %-30s Use this extended username.                 (PARROT_USERNAME)\n", "-u,--username=<name>");
	fprintf(stdout, " %-30s Display version number.\n", "-v,--version");
	fprintf(stdout, " %-30s Initial working directory.\n", "-w,--work-dir=<dir>");
	fprintf(stdout, " %-30s Display table of system calls trapped.\n", "-W,--syscall-table");
	fprintf(stdout, " %-30s Force synchronous disk writes.            (PARROT_FORCE_SYNC)\n", "-Y,--sync-write");
	fprintf(stdout, " %-30s Enable automatic decompression on .gz files.\n", "-Z,--auto-decompress");
	fprintf(stdout, "\n");
	fprintf(stdout, "Known debugging sub-systems are: ");
	debug_flags_print(stdout);
	fprintf(stdout, "\n");
	fprintf(stdout, "Enabled filesystems are:");
	if(pfs_service_lookup("http"))		fprintf(stdout, " http");
	if(pfs_service_lookup("grow"))		fprintf(stdout, " grow");
	if(pfs_service_lookup("ftp"))		fprintf(stdout, " ftp");
	if(pfs_service_lookup("anonftp"))	fprintf(stdout, " anonftp");
	if(pfs_service_lookup("gsiftp"))	fprintf(stdout, " gsiftp");
	if(pfs_service_lookup("nest"))		fprintf(stdout, " nest");
	if(pfs_service_lookup("chirp"))		fprintf(stdout, " chirp");
	if(pfs_service_lookup("gfal"))		fprintf(stdout, " gfal lfn guid srm");
	if(pfs_service_lookup("rfio"))		fprintf(stdout, " rfio");
	if(pfs_service_lookup("dcap"))		fprintf(stdout, " dcap");
	if(pfs_service_lookup("glite"))		fprintf(stdout, " glite");
	if(pfs_service_lookup("lfc"))		fprintf(stdout, " lfc");
	if(pfs_service_lookup("irods"))		fprintf(stdout, " irods");
	if(pfs_service_lookup("hdfs"))		fprintf(stdout, " hdfs");
	if(pfs_service_lookup("bxgrid"))	fprintf(stdout, " bxgrid");
	if(pfs_service_lookup("s3"))		fprintf(stdout, " s3");
	if(pfs_service_lookup("root"))      fprintf(stdout, " root");
	if(pfs_service_lookup("xrootd"))    fprintf(stdout, " xrootd");
	if(pfs_service_lookup("cvmfs"))		fprintf(stdout, " cvmfs");
	fprintf(stdout, "\n");
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
	pfs_process_raise(root_pid, sig, 1 /* really send it */);
}

/*
We will be fighting with our child processes for control of 
the terminal.  Whenever a we want to write to the terminal,
we will get a SIGTTOU or SIGTTIN instructing us that its
not our turn.  But, we are in charge!  So, upon receipt
of these signals, grab control of the terminal.
*/

static void control_terminal( int sig )
{
	if(sig==SIGTTOU) {
		tcsetpgrp(1,getpgrp());
		tcsetpgrp(2,getpgrp());
	} else {
		tcsetpgrp(0,getpgrp());
	}
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
		p->nsyscalls++;
		pfs_dispatch(p, 0);
	} else if (status>>8 == (SIGTRAP | (PTRACE_EVENT_CLONE<<8)) || status>>8 == (SIGTRAP | (PTRACE_EVENT_FORK<<8)) || status>>8 == (SIGTRAP | (PTRACE_EVENT_VFORK<<8))) {
		pid_t cpid;
		struct pfs_process *child;
		INT64_T clone_files;

		if (tracer_getevent(p->tracer, &message) == -1)
			return;
		cpid = message;
		debug(D_PROCESS, "pid %d cloned %d",pid,cpid);

		if(status>>8 == (SIGTRAP | (PTRACE_EVENT_FORK<<8)) || status>>8 == (SIGTRAP | (PTRACE_EVENT_VFORK<<8))) {
			clone_files = 0;
		} else {
			clone_files = p->syscall_args[0]&CLONE_FILES;
		}
		child = pfs_process_create(cpid,pid,clone_files);
		child->syscall_result = 0;
		if(p->syscall_args[0]&CLONE_THREAD)
			child->tgid = p->tgid;
		child->state = PFS_PROCESS_STATE_USER;
		tracer_continue(p->tracer,0); /* child starts stopped. */

		trace_this_pid = -1; /* now trace any process at all */
	} else if (status>>8 == (SIGTRAP | (PTRACE_EVENT_EXEC<<8))) {
		debug(D_PROCESS, "pid %d is completing exec", (int)pid);
		tracer_continue(p->tracer, 0);
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
			tracer_continue(p->tracer, 0);
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
			tracer_listen(p->tracer);
		} else {
			/* signal-delivery-stop */
			debug(D_PROCESS,"pid %d received signal %d (%s) (state %d)",pid,signum,string_signal(signum),p->state);
			switch(signum) {
				/* There are 4 process stop signals: SIGTTIN, SIGTTOU, SIGSTOP, and SIGTSTP.
				 * The Open Group Base Specifications Issue 6
				 * IEEE Std 1003.1, 2004 Edition
				 * Also mentioned in `man ptrace`.
				 */
				case SIGTTIN:
					tcsetpgrp(STDIN_FILENO,pid);
					signum = 0; /* suppress delivery */
					break;
				case SIGTTOU:
					tcsetpgrp(STDOUT_FILENO,pid);
					tcsetpgrp(STDERR_FILENO,pid);
					signum = 0; /* suppress delivery */
					break;
				case SIGSTOP:
					/* Black magic to get threads working on old Linux kernels... */

					if(p->nsyscalls == 0) { /* stop before we begin running the process */
						debug(D_DEBUG, "suppressing bootstrap SIGSTOP for %d",pid);
						signum = 0; /* suppress delivery */
						kill(p->pid,SIGCONT);
					}
					break;
				case SIGTSTP:
					break;
			}
			tracer_continue(p->tracer,signum); /* deliver (or not) the signal */
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

void handle_sigchld( int sig  )
{
	pfs_poll_abort();
}

static void handle_sigio( int sig )
{
	pfs_process_sigio();
}

void write_rval(const char* message, int status) {
	FILE *file = fopen(pfs_write_rval_file, "w+");
	if(file) {
		fprintf(file, "%s\n%d\n", message, status);
		fclose(file);
	}

}

int main( int argc, char *argv[] )
{
	int c;
	int chose_auth = 0;
	const char *s;
	char *tickets = NULL;
	char *http_proxy = NULL;
	pid_t pid;
	struct pfs_process *p;

	if(getenv("PARROT_ENABLED")) {
		fprintf(stderr,"sorry, parrot_run cannot be run inside of itself.\n");
		exit(EXIT_FAILURE);
	}

	random_init();

	debug_config(argv[0]);
	debug_config_file_size(0); /* do not rotate debug file by default */
	debug_config_fatal(pfs_process_killall);
	debug_config_getpid(pfs_process_getpid);

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
	install_handler(SIGTTIN,control_terminal);
	install_handler(SIGTTOU,control_terminal);
	install_handler(SIGCHLD,handle_sigchld);
	install_handler(SIGIO,handle_sigio);
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

	static const struct option long_options[] = {
		{"auto-decompress", no_argument, 0, 'Z'},
		{"block-size", required_argument, 0, 'b'},
		{"channel-auth", no_argument, 0, 'C'},
		{"chirp-auth",  required_argument, 0, 'a'},
		{"cvmfs-repos", required_argument, 0, 'r'},
		{"cvmfs-alien-cache", required_argument, 0, LONG_OPT_CVMFS_ALIEN_CACHE},
		{"cvmfs-disable-alien-cache", no_argument, 0, LONG_OPT_CVMFS_DISABLE_ALIEN_CACHE},
		{"cvmfs-repo-switching", no_argument, 0, LONG_OPT_CVMFS_REPO_SWITCHING},
		{"cvmfs-repos", required_argument, 0, 'r'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-level-irods", required_argument, 0, 'I'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"env-list", required_argument, 0, 'e'},
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
		{"paranoid", no_argument, 0, 'P'},
		{"proxy", required_argument, 0, 'p'},
		{"root-checksum", required_argument, 0, 'R'},
		{"session-caching", no_argument, 0, 'S'},
		{"status-file", required_argument, 0, 'c'},
		{"stream-no-cache", no_argument, 0, 's'},
		{"sync-write", no_argument, 0, 'Y'},
		{"syscall-table", no_argument, 0, 'W'},
		{"tab-file", required_argument, 0, 'm'},
		{"tempdir", required_argument, 0, 't'},
		{"tickets", required_argument, 0, 'i'},
		{"timeout", required_argument, 0, 'T'},
		{"uid", required_argument, 0, 'U'},
		{"username", required_argument, 0, 'u'},
		{"version", no_argument, 0, 'v'},
		{"with-checksums", no_argument, 0, 'K'},
		{"with-snapshots", no_argument, 0, 'F'},
		{"work-dir", required_argument, 0, 'w'},
		{0,0,0,0}
	};

	while((c=getopt_long(argc,argv,"+ha:b:B:c:Cd:DFfG:e:Hi:I:kKl:m:n:M:N:o:O:p:PQr:R:sSt:T:U:u:vw:WY", long_options, NULL)) > -1) {
		switch(c) {
		case 'a':
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
			if(!debug_flags_set(optarg)) show_help(argv[0]);
			break;
		case 'D':
			pfs_enable_small_file_optimizations = 0;
			break;
		case 'e':
			if(access(optarg, F_OK) != -1) {
				fprintf(stderr, "The envlist file (%s) has already existed. Please delete it first or refer to another envlist file!!\n", optarg);
				return 1;
			}
			int count;
			count = 0;
			FILE *fp;
			fp = fopen(optarg, "w");
			if(!fp) {
				debug(D_DEBUG, "Can not open envlist file: %s", optarg);
				return 1;
			}
			while(environ[count] != NULL) {
				fprintf(fp, "%s\n", environ[count]);
				count++;
			}
			char working_dir[PFS_PATH_MAX];
			::getcwd(working_dir,sizeof(working_dir));
			if(working_dir == NULL) {
				debug(D_DEBUG, "Can not obtain the current working directory!");
				return 1;
			}
			fprintf(fp, "PWD=%s\n", working_dir);
			fclose(fp);
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
			pfs_ldso_path = optarg;
			break;
		case 'm':
			pfs_resolve_file_config(optarg);
			break;
		case 'M':
			pfs_resolve_manual_config(optarg);
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
			char cmd[PFS_PATH_MAX];
			if(snprintf(cmd, PFS_PATH_MAX, "find /lib*/ -name ld-linux*>>%s 2>/dev/null", optarg) >= 0)
				system(cmd);
			else {
				debug(D_DEBUG, "writing ld-linux* into namelist file failed.");
				return 1;
			}
			fprintf(namelist_file, "/bin/sh\n");
			break;
		case 'N':
			pfs_false_uname = optarg;
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'p':
			http_proxy = xxstrdup(optarg);
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
			strncpy(pfs_cvmfs_alien_cache_dir,optarg,PFS_PATH_MAX);
			break;
		case LONG_OPT_CVMFS_DISABLE_ALIEN_CACHE:
			pfs_cvmfs_enable_alien = false;
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
			strncpy(pfs_temp_dir,optarg,sizeof(pfs_temp_dir)-1);
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
		case LONG_OPT_HELPER:
			pfs_use_helper = 1;
			break;
		default:
			show_help(argv[0]);
			break;
		}
	}

	if(optind>=argc) show_help(argv[0]);

	cctools_version_debug(D_DEBUG, argv[0]);

	debug(D_PROCESS, "I am process %d in group %d in session %d",(int)getpid(),(int)getpgrp(),(int)getsid(0));
	{
		extern char **environ;
		int i;
		buffer_t B;
		buffer_init(&B);
		buffer_putfstring(&B, "command: %s", argv[0]);
		for (i = 1; argv[i]; i++)
			buffer_putfstring(&B, " \"%s\"", argv[i]);
		debug(D_DEBUG, "%s", buffer_tostring(&B));
		debug(D_DEBUG, "environment:");
		for (i = 0; environ[i]; i++)
			debug(D_DEBUG, "%s", environ[i]);
		buffer_free(&B);
	}

	get_linux_version(argv[0]);

	if(isatty(0)) {
		pfs_master_timeout = 300;
	} else {
		pfs_master_timeout = 3600;
	}

	pfs_uid = getuid();
	pfs_gid = getgid();

	if (http_proxy[0])
		setenv("HTTP_PROXY", http_proxy, 1);
	http_proxy = realloc(http_proxy, 0);

	s = getenv("PARROT_BLOCK_SIZE");
	if(s) pfs_service_set_block_size(string_metric_parse(s));

	s = getenv("PARROT_MOUNT_FILE");
	if(s) pfs_resolve_file_config(s);

	s = getenv("PARROT_MOUNT_STRING");
	if(s) pfs_resolve_manual_config(s);

	s = getenv("PARROT_FORCE_STREAM");
	if(s) pfs_force_stream = 1;

	s = getenv("PARROT_FORCE_CACHE");
	if(s) pfs_force_cache = 1;

	s = getenv("PARROT_FOLLOW_SYMLINKS");
	if(s) pfs_follow_symlinks = atoi(s);

	s = getenv("PARROT_SESSION_CACHE");
	if(s) pfs_session_cache = 1;

	s = getenv("PARROT_HOST_NAME");
	if(s) pfs_false_uname = s;

	s = getenv("PARROT_UID");
	if(s) pfs_uid = atoi(s);

	s = getenv("PARROT_GID");
	if(s) pfs_gid = atoi(s);

	s = getenv("PARROT_TIMEOUT");
	if(s) pfs_master_timeout = string_time_parse(s);

	s = getenv("PARROT_FORCE_SYNC");
	if(s) pfs_force_sync = 1;

	s = getenv("PARROT_LDSO_PATH");
	if(s) strncpy(pfs_ldso_path, s, sizeof(pfs_ldso_path)-1);

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

	if (getenv("TMPDIR"))
		strncpy(sys_temp_dir, getenv("TMPDIR"), sizeof(sys_temp_dir)-1);

	if (!pfs_temp_dir[0]) {
		char *t = getenv("PARROT_TEMP_DIR");
		if(t && t[0]) {
			strncpy(pfs_temp_dir, t, sizeof(pfs_temp_dir)-1);
		} else {
			snprintf(pfs_temp_dir, sizeof(pfs_temp_dir), "%s/parrot.%d", sys_temp_dir, getuid());
		}
	}

	pfs_cvmfs_alien_cache_dir[0]                = '\0';
	pfs_cvmfs_alien_cache_dir[PFS_PATH_MAX - 1] = '\0';
	s = getenv("PARROT_CVMFS_ALIEN_CACHE");
	if(s && strlen(s) > 0)
		strncpy(pfs_cvmfs_alien_cache_dir, s, PFS_PATH_MAX);

	if(pfs_temp_dir[PFS_PATH_MAX - 1] != '\0')
		fatal("temporary files directory pathname larger than %d characters\n", PFS_PATH_MAX - 1);

	//if alien cache dir has not been set, use default based on final value of pfs_temp_dir.
	if(strlen(pfs_cvmfs_alien_cache_dir) < 1)
	{
		sprintf(pfs_cvmfs_alien_cache_dir,"%s/cvmfs", pfs_temp_dir);
	}

	pfs_file_cache = file_cache_init(pfs_temp_dir);
	if(!pfs_file_cache) fatal("couldn't setup cache in %s: %s\n",pfs_temp_dir,strerror(errno));
	file_cache_cleanup(pfs_file_cache);

	sprintf(pfs_cvmfs_locks_dir, "%s/cvmfs_locks_XXXXXX", pfs_temp_dir);
	mkdtemp(pfs_cvmfs_locks_dir);

	if(!chose_auth) auth_register_all();

	if(tickets) {
		auth_ticket_load(tickets);
		tickets = realloc(tickets, 0);
	} else if(getenv(CHIRP_CLIENT_TICKETS)) {
		auth_ticket_load(getenv(CHIRP_CLIENT_TICKETS));
	} else {
		auth_ticket_load(NULL);
	}

	if(!pfs_channel_init(channel_size*1024*1024)) fatal("couldn't establish I/O channel");

	{
		char buf[PATH_MAX];
		snprintf(buf, sizeof(buf), "%s/parrot-fd.XXXXXX", pfs_temp_dir);
		if (mkdtemp(buf) == NULL)
			fatal("could not create parrot-fd temporary directory: %s", strerror(errno));
		parrot_dir_fd = open(buf, O_RDONLY|O_DIRECTORY);
		if (parrot_dir_fd == -1)
			fatal("could not open tempdir: %s", strerror(errno));
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

	pfs_poll_init();

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
		setpgrp();
		{
			int fd = open("/dev/tty", O_RDWR);
			if (fd >= 0) {
				tcsetpgrp(fd, getpgrp());
				close(fd);
			}
			_exit(1);
		} else {
			debug(D_NOTICE,"unable to fork %s: %s",argv[optind],strerror(errno));
			if(pfs_write_rval) {
				write_rval("nofork", 0);
			}
			exit(1);
		}
	}

	CRITICAL_BEGIN

	root_pid = pid;
	debug(D_PROCESS,"attaching to pid %d",pid);
	if (tracer_attach(pid) == -1)
		fatal("could not trace child");
	p = pfs_process_create(pid,getpid(),0);
	if(!p) {
		if(pfs_write_rval) {
			write_rval("noattach", 0);
		}
		kill(pid,SIGKILL);
		fatal("unable to attach to pid %d: %s",pid,strerror(errno));
	}

	p->state = PFS_PROCESS_STATE_USER;
	strcpy(p->name,argv[optind]);

	while(pfs_process_count()>0) {
		while(1) {
			int status, flags;
			struct rusage usage;

			flags = WUNTRACED|__WALL|WNOHANG;
			pid = wait4(trace_this_pid,&status,flags,&usage);
#if 0 /* Enable this for extreme debugging... */
			debug(D_DEBUG, "%d = wait4(%d, %p, %d, %p)", (int)pid, (int)trace_this_pid, &status, flags, &usage);
#endif

			if(pid == pfs_watchdog_pid) {
				if (WIFEXITED(status) || WIFSIGNALED(status)) {
					debug(D_NOTICE,"watchdog died unexpectedly; killing everyone");
					pfs_process_kill_everyone(SIGKILL);
					break;
				}
			} else if(pid>0) {
				handle_event(pid,status,&usage);
			} else if(trace_this_pid > 0) {
				debug(D_PROCESS, "Waiting for process %d\n", trace_this_pid);
				usleep(100);       //Avoid busy waiting while the process gives signs of live.
			} else {
				break;
			}
		}

		if(pid==-1 && errno==ECHILD) break;
		if(pfs_process_count()>0) pfs_poll_sleep();
	}

	if(pfs_syscall_totals32) {
		printf("\nParrot System Call Summary:\n");
		printf("%" PRId64 " syscalls\n",pfs_syscall_count);
		printf("%" PRId64 " bytes read\n",pfs_read_count);
		printf("%" PRId64 " bytes written\n",pfs_write_count);

		printf("\n32-bit System Calls:\n");
		for(i=0;i<SYSCALL32_MAX;i++) {
			if(pfs_syscall_totals32[i]) {
				printf("%-20s %d\n",tracer_syscall32_name(i),pfs_syscall_totals32[i]);
			}
		}

		#ifdef CCTOOLS_CPU_X86_64

		printf("\n64-bit System Calls:\n");
		for(i=0;i<SYSCALL64_MAX;i++) {
			if(pfs_syscall_totals64[i]) {
				printf("%-20s %d\n",tracer_syscall64_name(i),pfs_syscall_totals64[i]);
			}
		}

		#endif
	}

	if(pfs_paranoid_mode) pfs_paranoia_cleanup();

	delete_dir(pfs_cvmfs_locks_dir);

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
