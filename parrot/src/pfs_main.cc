/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_channel.h"
#include "pfs_process.h"
#include "pfs_dispatch.h"
#include "pfs_poll.h"
#include "pfs_service.h"
#include "pfs_critical.h"
#include "pfs_paranoia.h"

extern "C" {
#include "cctools.h"
#include "tracer.h"
#include "stringtools.h"
#include "auth_all.h"
#include "xxmalloc.h"
#include "create_dir.h"
#include "file_cache.h"
#include "md5.h"
#include "sort_dir.h"
#include "password_cache.h"
#include "debug.h"
#include "getopt.h"
#include "pfs_resolve.h"
#include "chirp_client.h"
#include "chirp_global.h"
#include "chirp_ticket.h"
#include "ftp_lite.h"
#include "int_sizes.h"
}

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <limits.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/utsname.h>
#include <termio.h>
#include <termios.h>

pid_t trace_this_pid = -1;

int pfs_master_timeout = 300;
struct file_cache *pfs_file_cache = 0;
struct password_cache *pfs_password_cache = 0;
int pfs_trap_after_fork = 0;
int pfs_force_stream = 0;
int pfs_force_cache = 0;
int pfs_force_sync = 0;
int pfs_follow_symlinks = 1;
int pfs_session_cache = 0;
int pfs_use_helper = 1;
int pfs_checksum_files = 1;
int pfs_write_rval = 0;
int pfs_paranoid_mode = 0;
const char *pfs_write_rval_file = "parrot.rval";
int pfs_enable_small_file_optimizations = 1;
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
bool pfs_cvmfs_repo_switching = false;

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
	LONG_OPT_CVMFS_REPO_SWITCHING=500,
};

static void get_linux_version(const char *cmd)
{
	struct utsname name;
	int major,minor,micro,fields;

	uname(&name);

#ifdef CCTOOLS_CPU_I386
        if(!strcmp(name.machine,"x86_64")) {
                fatal("Sorry, you need to download a Parrot built specifically for an x86_64 CPU");
        }
#endif

	if(!strcmp(name.sysname,"Linux")) {
		debug(D_DEBUG,"kernel is %s %s",name.sysname,name.release);
		fields = sscanf(name.release,"%d.%d.%d",&major,&minor,&micro);
		if(fields==3) {
			if(major==2) {
				if(minor==4) {
					pfs_trap_after_fork = 1;
					return;
				} else if(minor==6) {
					pfs_trap_after_fork = 0;
					return;
				}
			} else if(major==3) {
				if(minor<=2) {
					pfs_trap_after_fork = 0;
					return;
				}
			}
		}
	}

	debug(D_NOTICE,"parrot_run %d.%d.%d has not been tested on %s %s yet, this may not work",CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,name.sysname,name.release);
}

static void pfs_helper_init( const char *argv0 ) 
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
	fprintf(stdout, " %-30s Enable file snapshot caching for all protocols.\n", "-F,--with-snapshots");
	fprintf(stdout, " %-30s Disable following symlinks.\n", "-f,--no-follow-symlinks");
	fprintf(stdout, " %-30s Fake this gid; Real gid stays the same.          (PARROT_GID)\n", "-G,--gid=<num>");
	fprintf(stdout, " %-30s Disable use of helper library.\n", "-H,--no-helper");
	fprintf(stdout, " %-30s Show this screen.\n", "-h,--help");
	fprintf(stdout, " %-30s Comma-delimited list of tickets to use for authentication.\n", "-i,--tickets=<files>");
	fprintf(stdout, " %-30s Set the debug level output for the iRODS driver.\n", "-I,--debug-level-irods=<num>");
	fprintf(stdout, " %-30s Checksum files where available.\n", "-K,--with-checksums");
	fprintf(stdout, " %-30s Do not checksum files.\n", "-k,--no-checksums");
	fprintf(stdout, " %-30s Path to ld.so to use.                      (PARROT_LDSO_PATH)\n", "-l,--ld-path=<path>");
	fprintf(stdout, " %-30s Use this file as a mountlist.             (PARROT_MOUNT_FILE)\n", "-m,--ftab-file=<file>");
	fprintf(stdout, " %-30s Mount (redirect) /foo to /bar.         (PARROT_MOUNT_STRING)\n", "-M,--mount=/foo=/bar");
	fprintf(stdout, " %-30s Pretend that this is my hostname.          (PARROT_HOST_NAME)\n", "-N,--hostname=<name>");
	fprintf(stdout, " %-30s Send debugging messages to this file.     (PARROT_DEBUG_FILE)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate debug files of this size.     (PARROT_DEBUG_FILE_SIZE)\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Use this proxy server for HTTP requests.         (HTTP_PROXY)\n", "-p,--proxy=<hst:p>");
	fprintf(stdout, " %-30s Enable paranoid mode for identity boxing mode.\n", "-P,--paranoid");
	fprintf(stdout, " %-30s Inhibit catalog queries to list /chirp.\n", "-Q,--no-chirp-catalog");
	fprintf(stdout, " %-30s CVMFS repositories to enable.             (PARROT_CVMFS_REPO)\n", "-r,--cvmfs-repos=<repos>");
	fprintf(stdout, " %-30s Allow repository switching when using CVMFS.\n","   --cvmfs-repo-switching");
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

static void handle_event( pid_t pid, int status, struct rusage usage )
{
	struct pfs_process *p;
	int signum;

	p = pfs_process_lookup(pid);
	if(!p) {
		debug(D_PROCESS,"killing unexpected pid %d",pid);
		kill(pid,SIGKILL);
		return;
	}

	if(WIFEXITED(status)) {
		debug(D_PROCESS,"pid %d exited normally with code %d",pid,WEXITSTATUS(status));
		pfs_process_stop(p,status,usage);
		if(pid==root_pid) root_exitstatus = status;
	} else if(WIFSIGNALED(status)) {
		signum = WTERMSIG(status);
		debug(D_PROCESS,"pid %d exited abnormally with signal %d (%s)",pid,signum,string_signal(signum));
		pfs_process_stop(p,status,usage);
		if(pid==root_pid) root_exitstatus = status;
	} else if(WIFSTOPPED(status)) {
		signum = WSTOPSIG(status);
		if(signum==SIGTRAP) {
			p->nsyscalls++;
			pfs_dispatch(p,0);
		} else {
			debug(D_PROCESS,"pid %d received signal %d (%s) (state %d)",pid,signum,string_signal(signum),p->state);
			if(signum==SIGTTIN) {
				tcsetpgrp(0,pid);
				tracer_continue(p->tracer,SIGCONT);
			} else if(signum==SIGTTOU) {
				tcsetpgrp(1,pid);
				tcsetpgrp(2,pid);
				tracer_continue(p->tracer,SIGCONT);
			} else {
				tracer_continue(p->tracer,signum);
				if(signum==SIGSTOP && p->nsyscalls==0) {
					kill(p->pid,SIGCONT);
				}
			}
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
	pid_t pid=0;
	int signum;
	int status;
	struct pfs_process *p;
	char *s;
	int i;
	int chose_auth=0;
	struct rusage usage;
	int c;
	char *tickets = NULL;

	srand(time(0)*(getpid()+getuid()));

	debug_config(argv[0]);
	debug_config_file_size(0);

	if(getenv("PARROT_ENABLED")) {
		fprintf(stderr,"sorry, parrot_run cannot be run inside of itself.\n");
		exit(1);
	}

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

	if(isatty(0)) {
		pfs_master_timeout = 300;
	} else {
		pfs_master_timeout = 3600;
	}
	
	pfs_uid = getuid();
	pfs_gid = getgid();

	putenv((char *)"PARROT_ENABLED=TRUE");

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
	if(s) pfs_ldso_path = s;

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
				auth_register_byname(args[i]);
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

	sprintf(pfs_temp_dir,"/tmp/parrot.%d",getuid());

	struct option long_options[] = {
		{"chirp-auth",  required_argument, 0, 'a'},
		{"block-size", required_argument, 0, 'b'},
		{"status-file", required_argument, 0, 'c'},
		{"channel-auth", no_argument, 0, 'C'},
		{"debug", required_argument, 0, 'd'},
		{"no-optimize", no_argument, 0, 'D'},
		{"with-snapshots", no_argument, 0, 'F'},
		{"no-follow-symlinks", no_argument, 0, 'f'},
		{"gid", required_argument, 0, 'G'},
		{"no-helper", no_argument, 0, 'H'},
		{"help", no_argument, 0, 'h'},
		{"tickets", required_argument, 0, 'i'},
		{"debug-level-irods", required_argument, 0, 'I'},
		{"with-checksums", no_argument, 0, 'K'},
		{"no-checksums", no_argument, 0, 'k'},
		{"ld-path", required_argument, 0, 'l'},
		{"tab-file", required_argument, 0, 'm'},
		{"mount", required_argument, 0, 'M'},
		{"hostname", required_argument, 0, 'N'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"proxy", required_argument, 0, 'p'},
		{"paranoid", no_argument, 0, 'P'},
		{"no-chirp-catalog", no_argument, 0, 'Q'},
		{"cvmfs-repos", required_argument, 0, 'r'},
		{"cvmfs-repo-switching", no_argument, 0, LONG_OPT_CVMFS_REPO_SWITCHING},
		{"root-checksum", required_argument, 0, 'R'},
		{"stream-no-cache", no_argument, 0, 's'},
		{"session-caching", no_argument, 0, 'S'},
		{"tempdir", required_argument, 0, 't'},
		{"timeout", required_argument, 0, 'T'},
		{"uid", required_argument, 0, 'U'},
		{"username", required_argument, 0, 'u'},
		{"version", no_argument, 0, 'v'},
		{"work-dir", required_argument, 0, 'w'},
		{"syscall-table", no_argument, 0, 'W'},
		{"sync-write", no_argument, 0, 'Y'},
		{"auto-decompress", no_argument, 0, 'Z'},
        {0,0,0,0}
	};

	while((c=getopt_long(argc,argv,"+ha:b:B:c:Cd:DFfG:Hi:I:kKl:m:M:N:o:O:p:PQr:R:sSt:T:U:u:vw:WY", long_options, NULL)) > -1) {
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
			pfs_use_helper = 0;
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
			setenv("HTTP_PROXY",optarg,1);
			break;
		case 'P':
			pfs_paranoid_mode = 1;
			break;
		case 'Q':
			chirp_global_inhibit_catalog(1);
			break;
		case 'r':
			pfs_cvmfs_repo_arg = optarg;
			break;
		case LONG_OPT_CVMFS_REPO_SWITCHING:
			pfs_cvmfs_repo_switching = true;
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
			strcpy(pfs_temp_dir,optarg);
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
		default:
			show_help(argv[0]);
			break;
	}
	}

	if(optind>=argc) show_help(argv[0]);

	cctools_version_debug(D_DEBUG, argv[0]);
	get_linux_version(argv[0]);

	pfs_file_cache = file_cache_init(pfs_temp_dir);
	if(!pfs_file_cache) fatal("couldn't setup cache in %s: %s\n",pfs_temp_dir,strerror(errno));
	file_cache_cleanup(pfs_file_cache);

	if(!chose_auth) auth_register_all();

	if(tickets) {
		auth_ticket_load(tickets);
		free(tickets);
	} else if(getenv(CHIRP_CLIENT_TICKETS)) {
		auth_ticket_load(getenv(CHIRP_CLIENT_TICKETS));
	} else {
		auth_ticket_load(NULL);
	}


	if(!pfs_channel_init(channel_size*1024*1024)) fatal("couldn't establish I/O channel");	

	if(pfs_use_helper) pfs_helper_init(argv[0]);

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

	/*
	For reasons I don't understand yet, parrot gets very confused when
	it is the session leader.  This happens when it is run as the first
	process in a pty, for example in an xterm or when run from a server.
	So, when we are the session leader, disconnect from the terminal .
	This disables features such as line editing and job control, but
	prevents triggering some ugly bugs.
	*/

	if(getsid(0)==getpid()) {
		debug(D_PROCESS, "disconnecting from terminal");
		::ioctl(0,TIOCNOTTY,0);
	}

	setpgrp();
	debug(D_PROCESS, "I am process %d in group %d in session %d",(int)getpid(),(int)getpgrp(),(int)getsid(0));

	if(pid==0) {
		pid = fork();
		if(pid>0) {
			debug(D_PROCESS,"pid %d started",pid);
		} else if(pid==0) {
			pfs_paranoia_payload();
			setpgrp();
			tracer_prepare();
			kill(getpid(),SIGSTOP);
			getpid();
			// This call is necessary to force the kernel to report the current heap
			// size, so that Parrot can observe it in order to rewrite the following exec.
			sbrk(4096);
			execvp(argv[optind],&argv[optind]);
			debug(D_NOTICE,"unable to execute %s: %s",argv[optind],strerror(errno));
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
	}

	CRITICAL_BEGIN

	root_pid = pid;
	debug(D_PROCESS,"attaching to pid %d",pid);
	p = pfs_process_create(pid,getpid(),getpid(),0,SIGCHLD);
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
			int flags;
			if(trace_this_pid!=-1) {
				flags = WUNTRACED|__WALL;
			} else {
				flags = WUNTRACED|__WALL|WNOHANG;
			}
			pid = wait4(trace_this_pid,&status,flags,&usage);
			if (pid == pfs_watchdog_pid) {
				if (WIFEXITED(status) || WIFSIGNALED(status)) {
				debug(D_NOTICE,"watchdog died unexpectedly; killing everyone");
				pfs_process_kill_everyone(SIGKILL);
				break;
			}
			} else if(pid>0) {
				handle_event(pid,status,usage);
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

	if(WIFEXITED(root_exitstatus)) {
		status = WEXITSTATUS(root_exitstatus);
		debug(D_PROCESS,"%s exited normally with status %d",argv[optind],status);
		if(pfs_write_rval) {
			write_rval("normal", status);
		}
		return status;
	} else {
		signum = WTERMSIG(root_exitstatus);
		debug(D_PROCESS,"%s exited abnormally with signal %d (%s)",argv[optind],signum,string_signal(signum));
		if(pfs_write_rval) {
			write_rval("abnormal", signum);
		}
		return 1;
	}
}

