/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_protocol.h"
#include "chirp_acl.h"
#include "chirp_reli.h"
#include "chirp_group.h"
#include "chirp_stats.h"
#include "chirp_alloc.h"
#include "chirp_filesystem.h"
#include "chirp_fs_local.h"
#include "chirp_fs_hdfs.h"
#include "chirp_fs_chirp.h"
#include "chirp_audit.h"
#include "chirp_thirdput.h"

#include "cctools.h"
#include "daemon.h"
#include "macros.h"
#include "debug.h"
#include "link.h"
#include "getopt_aux.h"
#include "auth_all.h"
#include "stringtools.h"
#include "full_io.h"
#include "datagram.h"
#include "username.h"
#include "disk_info.h"
#include "create_dir.h"
#include "catalog_server.h"
#include "domain_name_cache.h"
#include "list.h"
#include "xxmalloc.h"
#include "md5.h"
#include "load_average.h"
#include "memory_info.h"
#include "change_process_title.h"
#include "url_encode.h"
#include "get_canonical_path.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <signal.h>
#include <sys/wait.h>
#include <pwd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/select.h>

#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
#endif

/* The maximum chunk of memory the server will allocate to handle I/O */
#define MAX_BUFFER_SIZE (16*1024*1024)

static void chirp_receive(struct link *l);
static void chirp_handler(struct link *l, const char *addr, const char *subject);
static int errno_to_chirp(int e);

static int port = CHIRP_PORT;
static const char *port_file = NULL;
static int idle_timeout = 60;	/* one minute */
static int stall_timeout = 3600;	/* one hour */
static int parent_check_timeout = 300;	/* five minutes */
static int advertise_timeout = 300;	/* five minutes */
static int gc_timeout = 86400;
static time_t advertise_alarm = 0;
static time_t gc_alarm = 0;
static struct list *catalog_host_list;
static time_t starttime;
static struct datagram *catalog_port;
static char hostname[DOMAIN_NAME_MAX];
static const char *manual_hostname = 0;
static char address[LINK_ADDRESS_MAX];
static const char *safe_username = 0;
static uid_t safe_uid = 0;
static gid_t safe_gid = 0;
static int dont_dump_core = 0;
static UINT64_T minimum_space_free = 0;
static UINT64_T root_quota = 0;
static int did_explicit_auth = 0;
static int max_child_procs = 0;
static int total_child_procs = 0;
static int config_pipe[2];
static int exit_if_parent_fails = 0;
static const char *listen_on_interface = 0;
static const char *chirp_root_url = ".";
static const char *chirp_root_path = 0;
static char *chirp_debug_file = NULL;
static int sim_latency = 0;

char *chirp_transient_path = NULL;	/* local file system stuff */
extern const char *chirp_ticket_path;
const char *chirp_super_user = "";
const char *chirp_group_base_url = 0;
int chirp_group_cache_time = 900;
char chirp_owner[USERNAME_MAX] = "";

struct chirp_filesystem *cfs = 0;

static void show_help(const char *cmd)
{
	fprintf(stdout, "use: %s [options]\n", cmd);
	fprintf(stdout, "The most common options are:\n");
	fprintf(stdout, " %-30s URL of storage directory, like `file://path' or `hdfs://host:port/path'.\n", "-r,--root=<url>");
	fprintf(stdout, " %-30s Enable debugging for this sybsystem.\n", "-d,--debug=<name>");
	fprintf(stdout, " %-30s Send debugging output to this file.\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Send status updates to this host. (default: `%s')\n", "-u,--advertise=<host>", CATALOG_HOST);
	fprintf(stdout, " %-30s Show version info.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");

	fprintf(stdout, "\nLess common options are:\n");
	fprintf(stdout, " %-30s Use this file as the default ACL.\n", "-A,--default-acl=<file>");
	fprintf(stdout, " %-30s Enable this authentication method.\n", "-a,--auth=<method>");
	fprintf(stdout, " %-30s Write process identifier (PID) to file.\n", "-B,--pid-file=<file>");
	fprintf(stdout, " %-30s Run as a daemon.\n", "-b,--background");
	fprintf(stdout, " %-30s Do not create a core dump, even due to a crash.\n", "-C,--no-core-dump");
	fprintf(stdout, " %-30s Challenge directory for unix filesystem authentication.\n", "-c,--challenge-dir=<dir>");
	fprintf(stdout, " %-30s Exit if parent process dies.\n", "-E,--parent-death");
	fprintf(stdout, " %-30s Check for presence of parent at this interval. (default: %ds)\n", "-e,--parent-check=<seconds>", parent_check_timeout);
	fprintf(stdout, " %-30s Leave this much space free in the filesystem.\n", "-F,--free-space=<size>");
	fprintf(stdout, " %-30s Base url for group lookups. (default: disabled)\n", "-G,--group-url=<url>");
	fprintf(stdout, " %-30s Run as lower privilege user. (root protection)\n", "-i,--user=<user>");
	fprintf(stdout, " %-30s Listen only on this network interface.\n", "-I,--interface=<addr>");
	fprintf(stdout, " %-30s Set the maximum number of clients to accept at once. (default unlimited)\n", "-M,--max-clients=<count>");
	fprintf(stdout, " %-30s Use this name when reporting to the catalog.\n", "-n,--catalog-name=<name>");
	fprintf(stdout, " %-30s Rotate debug file once it reaches this size.\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Superuser for all directories. (default: none)\n", "-P,--superuser=<user>");
	fprintf(stdout, " %-30s Listen on this port. (default: %d)\n", "-p,--port=<port>", port);
	fprintf(stdout, " %-30s Enforce this root quota in software.\n", "-Q,--root-quota=<size>");
	fprintf(stdout, " %-30s Read-only mode.\n", "-R,--read-only");
	fprintf(stdout, " %-30s Abort stalled operations after this long. (default: %ds)\n", "-s,--stalled=<time>", stall_timeout);
	fprintf(stdout, " %-30s Maximum time to cache group information. (default: %ds)\n", "-T,--group-cache-exp=<time>", chirp_group_cache_time);
	fprintf(stdout, " %-30s Disconnect idle clients after this time. (default: %ds)\n", "-t,--idle-clients=<time>", idle_timeout);
	fprintf(stdout, " %-30s Send status updates at this interval. (default: 5m)\n", "-U,--catalog-update=<time>");
	fprintf(stdout, " %-30s Use alternate password file for unix authentication.\n", "-W,--passwd=<file>");
	fprintf(stdout, " %-30s The name of this server's owner. (default: `whoami`)\n", "-w,--owner=<user>");
	fprintf(stdout, " %-30s Location of transient data. (default: `.')\n", "-y,--transient=<dir>");
	fprintf(stdout, " %-30s Select port at random and write it to this file. (default: disabled)\n", "-Z,--port-file=<file>");
	fprintf(stdout, " %-30s Set max timeout for unix filesystem authentication. (default: 5s)\n", "-z,--unix-timeout=<file>");
	fprintf(stdout, "\n");
	fprintf(stdout, "Where debug flags are: ");
	debug_flags_print(stdout);
	fprintf(stdout, "\n\n");
}

void shutdown_clean(int sig)
{
	exit(0);
}

void ignore_signal(int sig)
{
}

void handle_child(int sig)
{
/*
Do nothing in this function, it only exists to catch a signal
so that we are forced to break out of sleep(5) on a SIGCHLD below.
*/
}

static void install_handler(int sig, void (*handler) (int sig))
{
	struct sigaction s;
	s.sa_handler = handler;
	sigfillset(&s.sa_mask);
	s.sa_flags = 0;
	sigaction(sig, &s, 0);
}

/*
space_available() is a simple mechanism to ensure that
a runaway client does not use up every last drop of disk
space on a machine.  This function returns false if
consuming the given amount of space will leave less than
a fixed amount of headroom on the disk.  Note that
get_disk_info() is quite expensive, so we do not call
it more than once per second.
*/

static int space_available(INT64_T amount)
{
	static UINT64_T avail;
	static time_t last_check = 0;
	int check_interval = 1;
	time_t current;

	if(minimum_space_free == 0)
		return 1;

	current = time(0);

	if((current - last_check) > check_interval) {
		struct chirp_statfs buf;
		if(cfs->statfs(chirp_root_path, &buf) < 0)
			return 0;
		avail = buf.f_bsize * buf.f_bfree;
		last_check = current;
	}

	if((avail - amount) > minimum_space_free) {
		avail -= amount;
		return 1;
	} else {
		errno = ENOSPC;
		return 0;
	}
}

int update_one_catalog(void *catalog_host, const void *text)
{
	char addr[DATAGRAM_ADDRESS_MAX];
	if(domain_name_cache_lookup(catalog_host, addr)) {
		debug(D_DEBUG, "sending update to %s:%d", catalog_host, CATALOG_PORT);
		datagram_send(catalog_port, text, strlen(text), addr, CATALOG_PORT);
	}
	return 1;
}

static int update_all_catalogs(const char *url)
{
	struct chirp_statfs info;
	struct utsname name;
	char text[DATAGRAM_PAYLOAD_MAX];
	int length;
	int cpus;
	double avg[3];
	UINT64_T memory_total, memory_avail;

	uname(&name);
	string_tolower(name.sysname);
	string_tolower(name.machine);
	string_tolower(name.release);
	load_average_get(avg);
	cpus = load_average_get_cpus();

	const char *path = cfs->init(url);

	if(cfs->statfs(path, &info) < 0) {
		memset(&info, 0, sizeof(info));
	}

	memory_info_get(&memory_avail, &memory_total);

	length = sprintf(text,
			 "type chirp\nversion %d.%d.%s\nurl chirp://%s:%d\nname %s\nowner %s\ntotal %" PRIu64 "\navail %" PRIu64 "\nstarttime %lu\nport %d\ncpu %s\nopsys %s\nopsysversion %s\nload1 %0.02lf\nload5 %0.02lf\nload15 %0.02lf\nminfree %" PRIu64
			 "\nmemory_total %" PRIu64 "\nmemory_avail %" PRIu64 "\ncpus %d\nbackend %s\n", CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, hostname, port, hostname, chirp_owner, info.f_blocks * info.f_bsize,
			 info.f_bavail * info.f_bsize, (unsigned long) starttime, port, name.machine, name.sysname, name.release, avg[0], avg[1], avg[2], minimum_space_free, memory_total, memory_avail, cpus, url);

	chirp_stats_summary(&text[length], DATAGRAM_PAYLOAD_MAX - length);
	list_iterate(catalog_host_list, update_one_catalog, text);

	return 0;
}

static int backend_setup(const char *url)
{
	const char *root_path;

	root_path = cfs->init(url);
	if(!root_path)
		fatal("couldn't initialize %s", url);

	int result = cfs_create_dir(root_path, 0711);
	if(!result)
		fatal("couldn't create root directory %s: %s", root_path, strerror(errno));

	chirp_acl_init_root(root_path);

	result = cfs->chdir(root_path);
	if(result < 0)
		fatal("couldn't move to %s: %s", root_path, strerror(errno));

	return 0;
}

static int gc_tickets(const char *url)
{
	const char *path = cfs->init(url);
	if(!path)
		fatal("couldn't initialize %s", url);

	chirp_ticket_path = path;

	chirp_acl_gctickets(path);

	return 0;
}

static int run_in_child_process(int (*func) (const char *a), const char *args, const char *name)
{
	debug(D_PROCESS, "*** %s starting ***", name);

	pid_t pid = fork();
	if(pid == 0) {
		_exit(func(args));
	} else if(pid > 0) {
		int status;
		while(waitpid(pid, &status, 0) != pid) {
		}
		debug(D_PROCESS, "*** %s complete ***", name);
		if(WIFEXITED(status)) {
			return WEXITSTATUS(status);
		} else {
			return -1;
		}
	} else {
		debug(D_PROCESS, "couldn't fork: %s", strerror(errno));
		return -1;
	}
}

/*
The parent Chirp server process maintains a pipe connected to
all child processes.  When the child must update the global
state, it is done by sending a message to the config pipe,
which the parent reads and processes.  This code relies
on the guarantee that all writes of less than PIPE_BUF size
are atomic, so here we expect a read to return one or
more complete messages, each delimited by a newline.
*/

static void config_pipe_handler(int fd)
{
	char line[PIPE_BUF];
	char flag[PIPE_BUF];
	char subject[PIPE_BUF];
	char address[PIPE_BUF];
	UINT64_T ops, bytes_read, bytes_written;

	while(1) {
		fcntl(fd, F_SETFL, O_NONBLOCK);

		int length = read(fd, line, PIPE_BUF);
		if(length <= 0)
			return;

		line[length] = 0;

		const char *msg = strtok(line, "\n");
		while(msg) {
			debug(D_DEBUG, "config message: %s", msg);

			if(sscanf(msg, "debug %s", flag) == 1) {
				debug_flags_set(flag);
			} else if(sscanf(msg, "stats %s %s %" SCNu64 " %" SCNu64 " %" SCNu64, address, subject, &ops, &bytes_read, &bytes_written) == 5) {
				chirp_stats_collect(address, subject, ops, bytes_read, bytes_written);
			} else {
				debug(D_NOTICE, "bad config message: %s\n", msg);
			}
			msg = strtok(0, "\n");
		}
	}
}


static struct option long_options[] = {
	{"advertise", required_argument, 0, 'u'},
	{"auth", required_argument, 0, 'a'},
	{"catalog-name", required_argument, 0, 'n'},
	{"challenge-dir", required_argument, 0, 'c'},
	{"catalog-update", required_argument, 0, 'U'},
	{"background", no_argument, 0, 'b'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"default-acl", required_argument, 0, 'A'},
	{"free-space", required_argument, 0, 'F'},
	{"group-cache-exp", required_argument, 0, 'T'},
	{"group-url", required_argument, 0, 'G'},
	{"help", no_argument, 0, 'h'},
	{"idle-clients", required_argument, 0, 't'},
	{"interface", required_argument, 0, 'I'},
	{"max-clients", required_argument, 0, 'M'},
	{"no-core-dump", no_argument, 0, 'C'},
	{"owner", required_argument, 0, 'w'},
	{"parent-check", required_argument, 0, 'e'},
	{"parent-death", no_argument, 0, 'E'},
	{"passwd", required_argument, 0, 'W'},
	{"pid-file", required_argument, 0, 'B'},
	{"port", required_argument, 0, 'p'},
	{"port-file", required_argument, 0, 'Z'},
	{"read-only", no_argument, 0, 'R'},
	{"root", required_argument, 0, 'r'},
	{"root-quota", required_argument, 0, 'Q'},
	{"debug-rotate-max", required_argument, 0, 'O'},
	{"stalled", required_argument, 0, 's'},
	{"superuser", required_argument, 0, 'P'},
	{"transient", required_argument, 0, 'y'},
	{"unix-timeout", required_argument, 0, 'z'},
	{"user", required_argument, 0, 'i'},
	{"version", no_argument, 0, 'v'},
	{0, 0, 0, 0}
};

int main(int argc, char *argv[])
{
	struct link *link;
	signed char c;
	time_t current;
	int is_daemon = 0;
	char *pidfile = NULL;

	change_process_title_init(argv);
	change_process_title("chirp_server");

	catalog_host_list = list_create();

	debug_config(argv[0]);

	/* Ensure that all files are created private by default. */
	umask(0077);

	while((c = getopt_long(argc, argv, "A:a:B:bCc:d:Ee:F:G:hI:i:l:M:n:O:o:P:p:Q:Rr:s:T:t:U:u:vW:w:y:Z:z:", long_options, NULL)) > -1) {
		switch (c) {
		case 'A':
			chirp_acl_default(optarg);
			break;
		case 'a':
			auth_register_byname(optarg);
			did_explicit_auth = 1;
			break;
		case 'b':
			is_daemon = 1;
			break;
		case 'B':
			free(pidfile);
			pidfile = strdup(optarg);
			break;
		case 'c':
			auth_unix_challenge_dir(optarg);
			break;
		case 'C':
			dont_dump_core = 1;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'e':
			parent_check_timeout = string_time_parse(optarg);
			exit_if_parent_fails = 1;
			break;
		case 'E':
			exit_if_parent_fails = 1;
			break;
		case 'F':
			minimum_space_free = string_metric_parse(optarg);
			break;
		case 'G':
			chirp_group_base_url = optarg;
			break;
		case 'i':
			safe_username = optarg;
			break;
		case 'n':
			manual_hostname = optarg;
			break;
		case 'M':
			max_child_procs = atoi(optarg);
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'P':
			chirp_super_user = optarg;
			break;
		case 'Q':
			root_quota = string_metric_parse(optarg);
			break;
		case 't':
			idle_timeout = string_time_parse(optarg);
			break;
		case 'T':
			chirp_group_cache_time = string_time_parse(optarg);
			break;
		case 's':
			stall_timeout = string_time_parse(optarg);
			break;
		case 'r':
			chirp_root_url = optarg;
			break;
		case 'R':
			chirp_acl_force_readonly();
			break;
		case 'o':
			free(chirp_debug_file);
			chirp_debug_file = strdup(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'u':
			list_push_head(catalog_host_list, strdup(optarg));
			break;
		case 'U':
			advertise_timeout = string_time_parse(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			return 1;
		case 'w':
			strcpy(chirp_owner, optarg);
			break;
		case 'W':
			auth_unix_passwd_file(optarg);
			break;
		case 'I':
			listen_on_interface = optarg;
			break;
		case 'y':
			free(chirp_transient_path);
			chirp_transient_path = strdup(optarg);
			break;
		case 'z':
			auth_unix_timeout_set(atoi(optarg));
			break;
		case 'Z':
			port_file = optarg;
			port = 0;
			break;
		case 'l':
			/* not documented, internal testing */
			sim_latency = atoi(optarg);
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	if(is_daemon)
		daemonize(0, pidfile);	/* don't chdir to "/" */

	/* Ensure that all files are created private by default (again because of daemonize). */
	umask(0077);

	/* open debug file now because daemonize closes all open fds */
	debug_config_file(chirp_debug_file);

	cctools_version_debug(D_DEBUG, argv[0]);

	/* if chirp_transient_path is NULL, use CWD */
	if(chirp_transient_path == NULL) {
		chirp_transient_path = realloc(chirp_transient_path, 8192);
		if(getcwd(chirp_transient_path, 8192) == NULL) {
			fatal("could not get current working directory: %s", strerror(errno));
		}
	} else if(!create_dir(chirp_transient_path, S_IRWXU)) {
		fatal("could not create transient data directory '%s': %s", chirp_transient_path, strerror(errno));
	}

	if(pipe(config_pipe) < 0)
		fatal("could not create internal pipe: %s", strerror(errno));

	if(dont_dump_core) {
		struct rlimit rl;
		rl.rlim_cur = rl.rlim_max = 0;
		setrlimit(RLIMIT_CORE, &rl);
	}

	current = time(0);
	debug(D_ALL, "*** %s starting at %s", argv[0], ctime(&current));

	if(!chirp_owner[0]) {
		if(!username_get(chirp_owner)) {
			strcpy(chirp_owner, "unknown");
		}
	}

	if(!did_explicit_auth) {
		auth_register_all();
	}

	cfs = cfs_lookup(chirp_root_url);

	if(run_in_child_process(backend_setup, chirp_root_url, "backend setup") != 0) {
		fatal("couldn't setup %s", chirp_root_url);
	}

	if(!list_size(catalog_host_list)) {
		list_push_head(catalog_host_list, CATALOG_HOST);
	}

	if(getuid() == 0) {
		if(!safe_username) {
			fprintf(stdout, "Sorry, I refuse to run as root without certain safeguards.\n");
			fprintf(stdout, "Please give me a safe username with the -i <user> option.\n");
			fprintf(stdout, "After using root access to authenticate users,\n");
			fprintf(stdout, "I will use the safe username to access data on disk.\n");
			exit(1);
		} else {
			struct passwd *p = getpwnam(safe_username);
			if(!p)
				fatal("unknown user: %s", safe_username);
			safe_uid = p->pw_uid;
			safe_gid = p->pw_gid;
		}
	} else if(safe_username) {
		fprintf(stdout, "Sorry, the -i option doesn't make sense\n");
		fprintf(stdout, "unless I am already running as root.\n");
		exit(1);
	}

	link = link_serve_address(listen_on_interface, port);

	if(!link) {
		if(listen_on_interface) {
			fatal("couldn't listen on interface %s port %d: %s", listen_on_interface, port, strerror(errno));
		} else {
			fatal("couldn't listen on port %d: %s", port, strerror(errno));
		}
	}

	link_address_local(link, address, &port);

	debug(D_DEBUG, "now listening port on port %d\n", port);

	if(port_file)
		opts_write_port_file(port_file, port);

	starttime = time(0);
	catalog_port = datagram_create(0);
	if(manual_hostname) {
		strcpy(hostname, manual_hostname);
	} else {
		domain_name_cache_guess(hostname);
	}

	install_handler(SIGPIPE, ignore_signal);
	install_handler(SIGHUP, ignore_signal);
	install_handler(SIGCHLD, handle_child);
	install_handler(SIGINT, shutdown_clean);
	install_handler(SIGTERM, shutdown_clean);
	install_handler(SIGQUIT, shutdown_clean);
	install_handler(SIGXFSZ, ignore_signal);

	while(1) {
		char addr[LINK_ADDRESS_MAX];
		int port;
		struct link *l;
		pid_t pid;

		if(exit_if_parent_fails) {
			if(getppid() < 5) {
				fatal("stopping because parent process died.");
				exit(0);
			}
		}

		while((pid = waitpid(-1, 0, WNOHANG)) > 0) {
			debug(D_PROCESS, "pid %d completed (%d total child procs)", pid, total_child_procs);
			total_child_procs--;
		}

		if(time(0) >= advertise_alarm) {
			run_in_child_process(update_all_catalogs, chirp_root_url, "catalog update");
			advertise_alarm = time(0) + advertise_timeout;
			chirp_stats_cleanup();
		}

		if(time(0) >= gc_alarm) {
			run_in_child_process(gc_tickets, chirp_root_url, "ticket cleanup");
			gc_alarm = time(0) + gc_timeout;
		}

		/* Wait for action on one of two ports: the master TCP port, or the internal pipe. */
		/* If the limit of child procs has been reached, don't watch the TCP port. */

		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(config_pipe[0], &rfds);
		if(max_child_procs == 0 || total_child_procs < max_child_procs) {
			FD_SET(link_fd(link), &rfds);
		}
		int maxfd = MAX(link_fd(link), config_pipe[0]) + 1;

		/* Sleep for the minimum of any periodic timers, but don't go negative. */

		struct timeval timeout;
		time_t current = time(0);
		timeout.tv_usec = 0;
		timeout.tv_sec = advertise_alarm - current;
		timeout.tv_sec = MIN(timeout.tv_sec, gc_alarm - current);
		timeout.tv_sec = MIN(timeout.tv_sec, parent_check_timeout);
		timeout.tv_sec = MAX(0, timeout.tv_sec);

		/* Wait for activity on the listening port or the config pipe */

		if(select(maxfd, &rfds, 0, 0, &timeout) < 0)
			continue;

		/* If the network port is active, accept the connection and fork the handler. */

		if(FD_ISSET(link_fd(link), &rfds)) {
			l = link_accept(link, time(0) + 5);
			if(!l)
				continue;

			link_address_remote(l, addr, &port);

			pid = fork();
			if(pid == 0) {
				chirp_receive(l);
				_exit(0);
			} else if(pid > 0) {
				total_child_procs++;
				debug(D_PROCESS, "created pid %d (%d total child procs)", pid, total_child_procs);
			} else {
				debug(D_PROCESS, "couldn't fork: %s", strerror(errno));
			}
			link_close(l);
		}

		/* If the config pipe is active, read and process those messages. */

		if(FD_ISSET(config_pipe[0], &rfds)) {
			config_pipe_handler(config_pipe[0]);
		}
	}
}

static void chirp_receive(struct link *link)
{
	char *atype, *asubject;
	char typesubject[AUTH_TYPE_MAX + AUTH_SUBJECT_MAX];
	char addr[LINK_ADDRESS_MAX];
	int port;

	change_process_title("chirp_server [authenticating]");

	/* Chirp's backend file system must be loaded here. HDFS loads in the JVM
	 * which does not play nicely with fork. So, we only manipulate the backend
	 * file system in a child process which actually handles client requests.
	 * */
	chirp_root_path = cfs->init(chirp_root_url);
	if(!chirp_root_path)
		fatal("could not initialize %s backend filesystem: %s", chirp_root_url, strerror(errno));

	chirp_ticket_path = chirp_root_path;

	if(root_quota > 0) {
		if(cfs == &chirp_fs_hdfs)
			/* FIXME: why can't HDFS do quotas? original comment: "using HDFS? Can't do quotas : /" */
			fatal("Cannot use quotas with HDFS\n");
		else
			chirp_alloc_init(chirp_root_path, root_quota);
	}

	if(cfs->chdir(chirp_root_path) != 0)
		fatal("couldn't move to %s: %s\n", chirp_root_path, strerror(errno));

	link_address_remote(link, addr, &port);

	auth_ticket_server_callback(chirp_acl_ticket_callback);

	if(auth_accept(link, &atype, &asubject, time(0) + idle_timeout)) {
		sprintf(typesubject, "%s:%s", atype, asubject);
		free(atype);
		free(asubject);

		debug(D_LOGIN, "%s from %s:%d", typesubject, addr, port);

		if(safe_username) {
			cfs->chown(chirp_root_path, safe_uid, safe_gid);
			cfs->chmod(chirp_root_path, 0700);
			debug(D_AUTH, "changing to uid %d gid %d", safe_uid, safe_gid);
			setgid(safe_gid);
			setuid(safe_uid);
		}
		/* Enable only globus, hostname, and address authentication for third-party transfers. */
		auth_clear();
		if(auth_globus_has_delegated_credential()) {
			auth_globus_use_delegated_credential(1);
			auth_globus_register();
		}
		auth_hostname_register();
		auth_address_register();

		change_process_title("chirp_server [%s:%d] [%s]", addr, port, typesubject);

		chirp_handler(link, addr, typesubject);
		chirp_alloc_flush();
		chirp_stats_report(config_pipe[1], addr, typesubject, 0);

		debug(D_LOGIN, "disconnected");
	} else {
		debug(D_LOGIN, "authentication failed from %s:%d", addr, port);
	}

	link_close(link);
}

/*
  Force a path to fall within the simulated root directory.
*/

static int chirp_path_fix(char *path)
{
	char decodepath[CHIRP_PATH_MAX];
	char shortpath[CHIRP_PATH_MAX];
	char rootpath[CHIRP_PATH_MAX];

	// Remove the percent-hex encoding
	url_decode(path, decodepath, sizeof(decodepath));

	// Collapse dots, double dots, and the like:
	string_collapse_path(decodepath, shortpath, 1);

	// Add the current directory to the root (backend file system changes directory to the root)
	sprintf(rootpath, "./%s", shortpath);

	// Collapse again...
	string_collapse_path(rootpath, path, 1);

	return 1;
}

/*
  A note on integers:
  Various operating systems employ integers of different sizes
  for fields such as file size, user identity, and so forth.
  Regardless of the operating system support, the Chirp protocol
  must support integers up to 64 bits.  So, in the server handling
  loop, we treat all integers as INT64_T.  What the operating system
  does from there is out of our hands.
*/

static void chirp_handler(struct link *l, const char *addr, const char *subject)
{
	char line[CHIRP_LINE_MAX];
	char *esubject;

	if(!chirp_acl_whoami(subject, &esubject))
		return;

	link_tune(l, LINK_TUNE_INTERACTIVE);

	while(1) {
		int do_stat_result = 0;
		int do_statfs_result = 0;
		int do_getdir_result = 0;
		int do_no_result = 0;
		INT64_T result = -1;

		char *dataout = 0;
		INT64_T dataoutlength = 0;

		char path[CHIRP_PATH_MAX];
		char newpath[CHIRP_PATH_MAX];
		char newsubject[CHIRP_LINE_MAX];
		char ticket_subject[CHIRP_LINE_MAX];
		char duration[CHIRP_LINE_MAX];
		char newacl[CHIRP_LINE_MAX];
		char hostname[CHIRP_LINE_MAX];

		/* extended attributes */
		char xattrname[CHIRP_LINE_MAX];
		size_t xattrsize;
		int xattrflags;

		char debug_flag[CHIRP_LINE_MAX];
		char pattern[CHIRP_LINE_MAX];

		INT64_T fd, length, flags, offset, actual;
		INT64_T uid, gid, mode;
		INT64_T size, inuse;
		INT64_T stride_length, stride_skip;
		int nreps;
		struct chirp_stat statbuf;
		struct chirp_statfs statfsbuf;
		INT64_T actime, modtime;
		time_t idletime = time(0) + idle_timeout;
		time_t stalltime = time(0) + stall_timeout;

		if(chirp_alloc_flush_needed()) {
			if(!link_usleep(l, 1000000, 1, 0)) {
				chirp_alloc_flush();
			}
		}

		if(!link_readline(l, line, sizeof(line), idletime)) {
			debug(D_CHIRP, "timeout: client idle too long\n");
			break;
		}

		string_chomp(line);
		if(strlen(line) < 1)
			continue;
		if(line[0] == 4)
			break;

		chirp_stats_report(config_pipe[1], addr, subject, advertise_alarm);

		chirp_stats_update(1, 0, 0);

		// Simulate network latency
		if(sim_latency > 0) {
			struct timeval tv;
			tv.tv_sec = 0;
			tv.tv_usec = sim_latency;
			select(0, NULL, NULL, NULL, &tv);
		}

		debug(D_CHIRP, "%s", line);

		if(sscanf(line, "pread %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &length, &offset) == 3) {
			length = MIN(length, MAX_BUFFER_SIZE);
			dataout = malloc(length);
			if(dataout) {
				result = chirp_alloc_pread(fd, dataout, length, offset);
				if(result >= 0) {
					dataoutlength = result;
					chirp_stats_update(0, result, 0);
				} else {
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
			}
		} else if(sscanf(line, "sread %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &length, &stride_length, &stride_skip, &offset) == 5) {
			length = MIN(length, MAX_BUFFER_SIZE);
			dataout = malloc(length);
			if(dataout) {
				result = chirp_alloc_sread(fd, dataout, length, stride_length, stride_skip, offset);
				if(result >= 0) {
					dataoutlength = result;
					chirp_stats_update(0, result, 0);
				} else {
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
			}
		} else if(sscanf(line, "pwrite %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &length, &offset) == 3) {
			INT64_T orig_length = length;
			length = MIN(length, MAX_BUFFER_SIZE);
			char *data = malloc(length);
			if(data) {
				actual = link_read(l, data, length, stalltime);
				if(actual != length) {
					free(data);
					break;
				}
				if(space_available(length)) {
					result = chirp_alloc_pwrite(fd, data, length, offset);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				link_soak(l, (orig_length - length), stalltime);
				free(data);
				if(result > 0) {
					chirp_stats_update(0, 0, result);
				}
			} else {
				link_soak(l, orig_length, stalltime);
				result = -1;
				errno = ENOMEM;
				break;
			}
		} else if(sscanf(line, "swrite %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &length, &stride_length, &stride_skip, &offset) == 5) {
			INT64_T orig_length = length;
			length = MIN(length, MAX_BUFFER_SIZE);
			char *data = malloc(length);
			if(data) {
				actual = link_read(l, data, length, stalltime);
				if(actual != length) {
					free(data);
					break;
				}
				if(space_available(length)) {
					result = chirp_alloc_swrite(fd, data, length, stride_length, stride_skip, offset);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				link_soak(l, (orig_length - length), stalltime);
				free(data);
				if(result > 0) {
					chirp_stats_update(0, 0, result);
				}
			} else {
				link_soak(l, orig_length, stalltime);
				result = -1;
				errno = ENOMEM;
				break;
			}
		} else if(sscanf(line, "whoami %" SCNd64, &length) == 1) {
			if((int) strlen(esubject) < length)
				length = strlen(esubject);
			dataout = malloc(length);
			if(dataout) {
				dataoutlength = length;
				strncpy(dataout, esubject, length);
				result = length;
			} else {
				result = -1;
			}
		} else if(sscanf(line, "whoareyou %s %" SCNd64, hostname, &length) == 2) {
			result = chirp_reli_whoami(hostname, newsubject, sizeof(newsubject), idletime);
			if(result > 0) {
				if(result > length)
					result = length;
				dataout = malloc(result);
				if(dataout) {
					dataoutlength = result;
					strncpy(dataout, newsubject, result);
				} else {
					errno = ENOMEM;
					result = -1;
				}
			} else {
				result = -1;
			}
		} else if(sscanf(line, "readlink %s %" SCNd64, path, &length) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_READ))
				goto failure;
			dataout = malloc(length);
			if(dataout) {
				result = chirp_alloc_readlink(path, dataout, length);
				if(result >= 0) {
					dataoutlength = result;
				} else {
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
				result = -1;
			}
		} else if(sscanf(line, "getlongdir %s", path) == 1) {
			struct chirp_dir *dir;
			struct chirp_dirent *d;

			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_LIST))
				goto failure;

			dir = chirp_alloc_opendir(path);
			if(dir) {
				link_putliteral(l, "0\n", stalltime);
				while((d = chirp_alloc_readdir(dir))) {
					if(!strncmp(d->name, ".__", 3))
						continue;
					link_putfstring(l, "%s\n%s\n", stalltime, d->name, chirp_stat_string(&d->info));
				}
				chirp_alloc_closedir(dir);
				do_getdir_result = 1;
				result = 0;
			} else {
				result = -1;
			}
		} else if(sscanf(line, "getdir %s", path) == 1) {
			struct chirp_dir *dir;
			struct chirp_dirent *d;

			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_LIST))
				goto failure;

			dir = chirp_alloc_opendir(path);
			if(dir) {
				link_putliteral(l, "0\n", stalltime);
				while((d = chirp_alloc_readdir(dir))) {
					if(!strncmp(d->name, ".__", 3))
						continue;
					link_putfstring(l, "%s\n", stalltime, d->name);
				}
				chirp_alloc_closedir(dir);
				do_getdir_result = 1;
				result = 0;
			} else {
				result = -1;
			}
		} else if(sscanf(line, "getacl %s", path) == 1) {
			char aclsubject[CHIRP_LINE_MAX];
			int aclflags;
			CHIRP_FILE *aclfile;

			if(!chirp_path_fix(path))
				goto failure;

			// Previously, the LIST right was necessary to view the ACL.
			// However, this has caused much confusion with debugging permissions problems.
			// As an experiment, let's trying making getacl accessible to everyone.

			// if(!chirp_acl_check_dir(path,subject,CHIRP_ACL_LIST)) goto failure;

			aclfile = chirp_acl_open(path);
			if(aclfile) {
				link_putliteral(l, "0\n", stalltime);
				while(chirp_acl_read(aclfile, aclsubject, &aclflags)) {
					link_putfstring(l, "%s %s\n", stalltime, aclsubject, chirp_acl_flags_to_text(aclflags));
				}
				chirp_acl_close(aclfile);
				do_getdir_result = 1;
				result = 0;
			} else {
				result = -1;
			}
		} else if(sscanf(line, "getfile %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!cfs_isnotdir(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;

			result = chirp_alloc_getfile(path, l, stalltime);

			if(result >= 0) {
				do_no_result = 1;
				chirp_stats_update(0, length, 0);
			}
		} else if(sscanf(line, "putfile %s %" SCNd64 " %" SCNd64, path, &mode, &length) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!cfs_isnotdir(path))
				goto failure;

			if(chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				/* writable, ok to proceed */
			} else if(chirp_acl_check(path, subject, CHIRP_ACL_PUT)) {
				if(cfs_exists(path)) {
					errno = EEXIST;
					goto failure;
				} else {
					/* ok to proceed */
				}
			} else {
				errno = EACCES;
				goto failure;
			}

			if(!space_available(length))
				goto failure;

			result = chirp_alloc_putfile(path, l, length, mode, stalltime);
			if(result >= 0) {
				chirp_stats_update(0, 0, length);
			}
		} else if(sscanf(line, "getstream %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!cfs_isnotdir(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;

			result = chirp_alloc_getstream(path, l, stalltime);
			if(result >= 0) {
				chirp_stats_update(0, length, 0);
				debug(D_CHIRP, "= %" SCNd64 " bytes streamed\n", result);
				/* getstream indicates end by closing the connection */
				break;
			}

		} else if(sscanf(line, "putstream %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!cfs_isnotdir(path))
				goto failure;

			if(chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				/* writable, ok to proceed */
			} else if(chirp_acl_check(path, subject, CHIRP_ACL_PUT)) {
				if(cfs_exists(path)) {
					errno = EEXIST;
					goto failure;
				} else {
					/* ok to proceed */
				}
			} else {
				errno = EACCES;
				goto failure;
			}

			result = chirp_alloc_putstream(path, l, stalltime);
			if(result >= 0) {
				chirp_stats_update(0, 0, length);
				debug(D_CHIRP, "= %" SCNd64 " bytes streamed\n", result);
				/* putstream getstream indicates end by closing the connection */
				break;
			}
		} else if(sscanf(line, "thirdput %s %s %s", path, hostname, newpath) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(cfs == &chirp_fs_hdfs)
				goto failure;

			/* ACL check will occur inside of chirp_thirdput */

			result = chirp_thirdput(subject, path, hostname, newpath, stalltime);

		} else if(sscanf(line, "open %s %s %" SCNd64, path, newpath, &mode) == 3) {
			flags = 0;

			if(strchr(newpath, 'r')) {
				if(strchr(newpath, 'w')) {
					flags = O_RDWR;
				} else {
					flags = O_RDONLY;
				}
			} else if(strchr(newpath, 'w')) {
				flags = O_WRONLY;
			}

			if(strchr(newpath, 'c'))
				flags |= O_CREAT;
			if(strchr(newpath, 't'))
				flags |= O_TRUNC;
			if(strchr(newpath, 'a'))
				flags |= O_APPEND;
			if(strchr(newpath, 'x'))
				flags |= O_EXCL;
#ifdef O_SYNC
			if(strchr(newpath, 's'))
				flags |= O_SYNC;
#endif

			if(!chirp_path_fix(path))
				goto failure;

			/*
			   This is a little strange.
			   For ordinary files, we check the ACL according
			   to the flags passed to open.  For some unusual
			   cases in Unix, we must also allow open()  for
			   reading on a directory, otherwise we fail
			   with EISDIR.
			 */

			if(cfs_isnotdir(path)) {
				if(chirp_acl_check(path, subject, chirp_acl_from_open_flags(flags))) {
					/* ok to proceed */
				} else if(chirp_acl_check(path, subject, CHIRP_ACL_PUT)) {
					if(flags & O_CREAT) {
						if(cfs_exists(path)) {
							errno = EEXIST;
							goto failure;
						} else {
							/* ok to proceed */
						}
					} else {
						errno = EACCES;
						goto failure;
					}
				} else {
					goto failure;
				}
			} else if(flags == O_RDONLY) {
				if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_LIST))
					goto failure;
			} else {
				errno = EISDIR;
				goto failure;
			}

			result = chirp_alloc_open(path, flags, (int) mode);
			if(result >= 0) {
				chirp_alloc_fstat(result, &statbuf);
				do_stat_result = 1;
			}


		} else if(sscanf(line, "close %" SCNd64, &fd) == 1) {
			result = chirp_alloc_close(fd);
		} else if(sscanf(line, "fchmod %" SCNd64 " %" SCNd64, &fd, &mode) == 2) {
			result = chirp_alloc_fchmod(fd, mode);
		} else if(sscanf(line, "fchown %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &uid, &gid) == 3) {
			result = 0;
		} else if(sscanf(line, "fsync %" SCNd64, &fd) == 1) {
			result = chirp_alloc_fsync(fd);
		} else if(sscanf(line, "ftruncate %" SCNd64 " %" SCNd64, &fd, &length) == 2) {
			result = chirp_alloc_ftruncate(fd, length);
		} else if(sscanf(line, "fgetxattr %" SCNd64 " %s", &fd, xattrname) == 2) {
			dataout = malloc(MAX_BUFFER_SIZE);
			if(dataout) {
				result = chirp_alloc_fgetxattr(fd, xattrname, dataout, MAX_BUFFER_SIZE);
				if(result > 0) {
					dataoutlength = result;
				} else {
					assert(result == -1);
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
				goto failure;
			}
		} else if(sscanf(line, "flistxattr %" SCNd64, &fd) == 1) {
			dataout = malloc(MAX_BUFFER_SIZE);
			if(dataout) {
				result = chirp_alloc_flistxattr(fd, dataout, MAX_BUFFER_SIZE);
				if(result >= 0) {
					dataoutlength = result;
				} else {
					assert(result == -1);
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
				goto failure;
			}
		} else if(sscanf(line, "fsetxattr %" SCNd64 " %s %zu %d", &fd, xattrname, &xattrsize, &xattrflags) == 4) {
			if(xattrsize > MAX_BUFFER_SIZE) {
				errno = ENOSPC;
				goto failure;
			}
			void *data = malloc(xattrsize);
			if(data) {
				int actual = link_read(l, data, xattrsize, stalltime);
				if(actual != (int) xattrsize) {
					free(data);
					break;
				}
				if(space_available(xattrsize)) {
					result = chirp_alloc_fsetxattr(fd, xattrname, data, xattrsize, xattrflags);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				free(data);
				if(result > 0) {
					chirp_stats_update(0, 0, result);
				}
			} else {
				link_soak(l, xattrsize, stalltime);
				result = -1;
				errno = ENOMEM;
			}
		} else if(sscanf(line, "fremovexattr %" SCNd64 " %s", &fd, xattrname) == 2) {
			result = chirp_alloc_fremovexattr(fd, xattrname);
		} else if(sscanf(line, "unlink %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check_link(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)
				) {
				result = chirp_alloc_unlink(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "access %s %" SCNd64, path, &flags) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			int chirp_flags = chirp_acl_from_access_flags(flags);
			/* If filename is a directory, then we change execute flags to list flags. */
			if(cfs_isdir(path) && (chirp_flags & CHIRP_ACL_EXECUTE)) {
				chirp_flags ^= CHIRP_ACL_EXECUTE;	/* remove execute flag */
				chirp_flags |= CHIRP_ACL_LIST;	/* change to list */
			}
			if(!chirp_acl_check(path, subject, chirp_flags))
				goto failure;
			result = chirp_alloc_access(path, flags);
		} else if(sscanf(line, "chmod %s %" SCNd64, path, &mode) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check_dir(path, subject, CHIRP_ACL_WRITE) || chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				result = chirp_alloc_chmod(path, mode);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "chown %s %" SCNd64 " %" SCNd64, path, &uid, &gid) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = 0;
		} else if(sscanf(line, "lchown %s %" SCNd64 " %" SCNd64, path, &uid, &gid) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = 0;
		} else if(sscanf(line, "truncate %s %" SCNd64, path, &length) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_truncate(path, length);
		} else if(sscanf(line, "rename %s %s", path, newpath) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_path_fix(newpath))
				goto failure;
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_READ | CHIRP_ACL_DELETE))
				goto failure;
			if(!chirp_acl_check(newpath, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_rename(path, newpath);
		} else if(sscanf(line, "getxattr %s %s", path, xattrname) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			dataout = malloc(MAX_BUFFER_SIZE);
			if(dataout) {
				result = chirp_alloc_getxattr(path, xattrname, dataout, MAX_BUFFER_SIZE);
				if(result > 0) {
					dataoutlength = result;
				} else {
					assert(result == -1);
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
				goto failure;
			}
		} else if(sscanf(line, "lgetxattr %s %s", path, xattrname) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			dataout = malloc(MAX_BUFFER_SIZE);
			if(dataout) {
				result = chirp_alloc_lgetxattr(path, xattrname, dataout, MAX_BUFFER_SIZE);
				if(result > 0) {
					dataoutlength = result;
				} else {
					assert(result == -1);
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
				goto failure;
			}
		} else if(sscanf(line, "listxattr %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			dataout = malloc(MAX_BUFFER_SIZE);
			if(dataout) {
				result = chirp_alloc_listxattr(path, dataout, MAX_BUFFER_SIZE);
				if(result >= 0) {
					dataoutlength = result;
				} else {
					assert(result == -1);
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
				goto failure;
			}
		} else if(sscanf(line, "llistxattr %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			dataout = malloc(MAX_BUFFER_SIZE);
			if(dataout) {
				result = chirp_alloc_llistxattr(path, dataout, MAX_BUFFER_SIZE);
				if(result >= 0) {
					dataoutlength = result;
				} else {
					assert(result == -1);
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
				goto failure;
			}
		} else if(sscanf(line, "setxattr %s %s %zu %d", path, xattrname, &xattrsize, &xattrflags) == 4) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			if(xattrsize > MAX_BUFFER_SIZE) {
				errno = ENOSPC;
				goto failure;
			}
			void *data = malloc(xattrsize);
			if(data) {
				int actual = link_read(l, data, xattrsize, stalltime);
				if(actual != (int) xattrsize) {
					free(data);
					break;
				}
				if(space_available(xattrsize)) {
					result = chirp_alloc_setxattr(path, xattrname, data, xattrsize, xattrflags);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				free(data);
				if(result > 0) {
					chirp_stats_update(0, 0, result);
				}
			} else {
				link_soak(l, xattrsize, stalltime);
				result = -1;
				errno = ENOMEM;
			}
		} else if(sscanf(line, "lsetxattr %s %s %zu %d", path, xattrname, &xattrsize, &xattrflags) == 4) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			if(xattrsize > MAX_BUFFER_SIZE) {
				errno = ENOSPC;
				goto failure;
			}
			void *data = malloc(xattrsize);
			if(data) {
				int actual = link_read(l, data, xattrsize, stalltime);
				if(actual != (int) xattrsize) {
					free(data);
					break;
				}
				if(space_available(xattrsize)) {
					result = chirp_alloc_lsetxattr(path, xattrname, data, xattrsize, xattrflags);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				free(data);
				if(result > 0) {
					chirp_stats_update(0, 0, result);
				}
			} else {
				link_soak(l, xattrsize, stalltime);
				result = -1;
				errno = ENOMEM;
			}
		} else if(sscanf(line, "removexattr %s %s", path, xattrname) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_removexattr(path, xattrname);
		} else if(sscanf(line, "lremovexattr %s %s", path, xattrname) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_lremovexattr(path, xattrname);
		} else if(sscanf(line, "link %s %s", path, newpath) == 2) {
			/* Can only hard link to files on which you already have r/w perms */
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ | CHIRP_ACL_WRITE))
				goto failure;
			if(!chirp_path_fix(newpath))
				goto failure;
			if(!chirp_acl_check(newpath, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_link(path, newpath);
		} else if(sscanf(line, "symlink %s %s", path, newpath) == 2) {
			/* Note that the link target (path) may be any arbitrary data. */
			/* Access permissions are checked when data is actually accessed. */
			if(!chirp_path_fix(newpath))
				goto failure;
			if(!chirp_acl_check(newpath, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_symlink(path, newpath);
		} else if(sscanf(line, "setacl %s %s %s", path, newsubject, newacl) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_ADMIN))
				goto failure;
			result = chirp_acl_set(path, newsubject, chirp_acl_text_to_flags(newacl), 0);
		} else if(sscanf(line, "resetacl %s %s", path, newacl) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_ADMIN))
				goto failure;
			result = chirp_acl_set(path, subject, chirp_acl_text_to_flags(newacl) | CHIRP_ACL_ADMIN, 1);
		} else if(sscanf(line, "ticket_register %s %s %" SCNd64, newsubject, duration, &length) == 3) {
			char *ticket;
			ticket = malloc(length + 1);	/* room for NUL terminator */
			if(ticket) {
				actual = link_read(l, ticket, length, stalltime);
				if(actual != length)
					break;
				*(ticket + length) = '\0';	/* NUL terminator... */
				if(strcmp(newsubject, "self") == 0)
					strcpy(newsubject, esubject);
				if(strcmp(esubject, newsubject) != 0 && strcmp(esubject, chirp_super_user) != 0) {	/* must be superuser to create a ticket for someone else */
					free(ticket);
					errno = EACCES;
					goto failure;
				}
				result = chirp_acl_ticket_create(chirp_ticket_path, subject, newsubject, ticket, duration);
				free(ticket);
			} else {
				link_soak(l, length, stalltime);
				result = -1;
				errno = ENOMEM;
				break;
			}
		} else if(sscanf(line, "ticket_delete %s", ticket_subject) == 1) {
			result = chirp_acl_ticket_delete(chirp_ticket_path, subject, ticket_subject);
		} else if(sscanf(line, "ticket_modify %s %s %s", ticket_subject, path, newacl) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			result = chirp_acl_ticket_modify(chirp_ticket_path, subject, ticket_subject, path, chirp_acl_text_to_flags(newacl));
		} else if(sscanf(line, "ticket_get %s", ticket_subject) == 1) {
			/* ticket_subject is ticket:MD5SUM */
			char *ticket_esubject;
			char *ticket;
			time_t expiration;
			char **ticket_rights;
			result = chirp_acl_ticket_get(chirp_ticket_path, subject, ticket_subject, &ticket_esubject, &ticket, &expiration, &ticket_rights);
			if(result == 0) {
				link_putliteral(l, "0\n", stalltime);
				link_putfstring(l, "%zu\n%s%zu\n%s%llu\n", stalltime, strlen(ticket_esubject), ticket_esubject, strlen(ticket), ticket, (unsigned long long) expiration);
				free(ticket_esubject);
				free(ticket);
				char **tr = ticket_rights;
				for(; tr[0] && tr[1]; tr += 2) {
					link_putfstring(l, "%s %s\n", stalltime, tr[0], tr[1]);
					free(tr[0]);
					free(tr[1]);
				}
				free(ticket_rights);
			}
		} else if(sscanf(line, "ticket_list %s", ticket_subject) == 1) {
			/* ticket_subject is the owner of the ticket, not ticket:MD5SUM */
			char **ticket_subjects;
			if(strcmp(ticket_subject, "self") == 0)
				strcpy(ticket_subject, esubject);
			int super = strcmp(subject, chirp_super_user) == 0;	/* note subject instead of esubject; super user must be authenticated as himself */
			if(!super && strcmp(ticket_subject, esubject) != 0) {
				errno = EACCES;
				goto failure;
			}
			result = chirp_acl_ticket_list(chirp_ticket_path, ticket_subject, &ticket_subjects);
			if(result == 0) {
				link_putliteral(l, "0\n", stalltime);
				char **ts = ticket_subjects;
				for(; ts && ts[0]; ts++) {
					link_putfstring(l, "%zu\n%s", stalltime, strlen(ts[0]), ts[0]);
					free(ts[0]);
				}
				free(ticket_subjects);
			}
		} else if(sscanf(line, "mkdir %s %" SCNd64, path, &mode) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check(path, subject, CHIRP_ACL_RESERVE)) {
				result = chirp_alloc_mkdir(path, mode);
				if(result == 0) {
					if(chirp_acl_init_reserve(path, subject)) {
						result = 0;
					} else {
						chirp_alloc_rmdir(path);
						errno = EACCES;
						goto failure;
					}
				}
			} else if(chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				result = chirp_alloc_mkdir(path, mode);
				if(result == 0) {
					if(chirp_acl_init_copy(path)) {
						result = 0;
					} else {
						chirp_alloc_rmdir(path);
						errno = EACCES;
						goto failure;
					}
				}
			} else if(cfs_isdir(path)) {
				errno = EEXIST;
				goto failure;
			} else {
				errno = EACCES;
				goto failure;
			}
		} else if(sscanf(line, "rmdir %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check_link(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)) {
				result = chirp_alloc_rmdir(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "rmall %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check_link(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)) {
				result = chirp_alloc_rmall(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "utime %s %" SCNd64 " %" SCNd64, path, &actime, &modtime) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_utime(path, actime, modtime);
		} else if(sscanf(line, "fstat %" SCNd64, &fd) == 1) {
			result = chirp_alloc_fstat(fd, &statbuf);
			do_stat_result = 1;
		} else if(sscanf(line, "fstatfs %" SCNd64, &fd) == 1) {
			result = chirp_alloc_fstatfs(fd, &statfsbuf);
			do_statfs_result = 1;
		} else if(sscanf(line, "statfs %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_LIST))
				goto failure;
			result = chirp_alloc_statfs(path, &statfsbuf);
			do_statfs_result = 1;
		} else if(sscanf(line, "stat %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_LIST))
				goto failure;
			result = chirp_alloc_stat(path, &statbuf);
			do_stat_result = 1;
		} else if(sscanf(line, "lstat %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_LIST))
				goto failure;
			result = chirp_alloc_lstat(path, &statbuf);
			do_stat_result = 1;
		} else if(sscanf(line, "lsalloc %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_LIST))
				goto failure;
			result = chirp_alloc_lsalloc(path, newpath, &size, &inuse);
			if(result >= 0) {
				link_putfstring(l, "0\n%s %" SCNd64 " %" SCNd64 "\n", stalltime, &newpath[strlen(chirp_root_path) + 1], size, inuse);
				do_no_result = 1;
			}
		} else if(sscanf(line, "mkalloc %s %" SCNd64 " %" SCNd64, path, &size, &mode) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check(path, subject, CHIRP_ACL_RESERVE)) {
				result = chirp_alloc_mkalloc(path, size, mode);
				if(result == 0) {
					if(chirp_acl_init_reserve(path, subject)) {
						result = 0;
					} else {
						chirp_alloc_rmdir(path);
						errno = EACCES;
						result = -1;
					}
				}
			} else if(chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				result = chirp_alloc_mkalloc(path, size, mode);
				if(result == 0) {
					if(chirp_acl_init_copy(path)) {
						result = 0;
					} else {
						chirp_alloc_rmdir(path);
						errno = EACCES;
						result = -1;
					}
				}
			} else {
				goto failure;
			}
		} else if(sscanf(line, "localpath %s", path) == 1) {
			struct chirp_stat info;
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_LIST) && !chirp_acl_check(path, "system:localuser", CHIRP_ACL_LIST))
				goto failure;
			result = chirp_alloc_stat(path, &info);
			if(result >= 0) {
				link_putfstring(l, "%zu\n%s", stalltime, strlen(path), path);
				do_no_result = 1;
			} else {
				result = -1;
			}
		} else if(sscanf(line, "audit %s", path) == 1) {
			struct hash_table *table;
			struct chirp_audit *entry;
			char *key;

			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_ADMIN))
				goto failure;

			table = chirp_audit(path);
			if(table) {
				link_putfstring(l, "%d\n", stalltime, hash_table_size(table));
				hash_table_firstkey(table);
				while(hash_table_nextkey(table, &key, (void *) &entry)) {
					link_putfstring(l, "%s %" SCNd64 " %" SCNd64 " %" SCNd64 "\n", stalltime, key, entry->nfiles, entry->ndirs, entry->nbytes);
				}
				chirp_audit_delete(table);
				result = 0;
				do_no_result = 1;
			} else {
				result = -1;
			}
		} else if(sscanf(line, "md5 %s", path) == 1) {
			dataout = xxmalloc(16);
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			if(chirp_alloc_md5(path, (unsigned char *) dataout) >= 0) {
				result = dataoutlength = 16;
			} else {
				result = errno_to_chirp(errno);
			}
		} else if(sscanf(line, "setrep %s %d", path, &nreps) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_setrep(path, nreps);
		} else if(sscanf(line, "debug %s", debug_flag) == 1) {
			if(strcmp(esubject, chirp_super_user) != 0) {
				errno = EPERM;
				goto failure;
			}
			result = 0;
			// send this message to the parent for processing.
			strcat(line, "\n");
			write(config_pipe[1], line, strlen(line));
			debug_flags_set(debug_flag);
		} else if(sscanf(line, "search %s %s %" PRId64, pattern, path, &flags) == 3) {
			link_putliteral(l, "0\n", stalltime);
			char *start = path;

			for(;;) {
				char fixed[CHIRP_PATH_MAX];
				char *end;
				if((end = strchr(start, CHIRP_SEARCH_DELIMITER)) != NULL)
					*end = '\0';

				strcpy(fixed, start);
				chirp_path_fix(fixed);
				assert(strcmp(fixed, ".") == 0 || strncmp(fixed, "./", 2) == 0);

				if(access(fixed, F_OK) == -1) {
					link_putfstring(l, "%d:%d:%s:\n", stalltime, ENOENT, CHIRP_SEARCH_ERR_OPEN, fixed);
				} else if(!chirp_acl_check(fixed, subject, CHIRP_ACL_WRITE)) {
					link_putfstring(l, "%d:%d:%s:\n", stalltime, EPERM, CHIRP_SEARCH_ERR_OPEN, fixed);
				} else {
					int found = chirp_alloc_search(subject, fixed, pattern, flags, l, stalltime);
					if(found && (flags & CHIRP_SEARCH_STOPATFIRST))
						break;
				}

				if(end != NULL) {
					start = end + 1;
					*end = CHIRP_SEARCH_DELIMITER;
				} else
					break;
			}

			do_getdir_result = 1;
			result = 0;
		} else {
			result = -1;
			errno = ENOSYS;
		}

		if(do_no_result) {
			/* nothing */
		} else if(result < 0) {
		      failure:
			result = errno_to_chirp(errno);
			sprintf(line, "%" PRId64 "\n", result);
		} else if(do_stat_result) {
			sprintf(line, "%" PRId64 "\n%s\n", result, chirp_stat_string(&statbuf));
		} else if(do_statfs_result) {
			sprintf(line, "%" PRId64 "\n%s\n", result, chirp_statfs_string(&statfsbuf));
		} else if(do_getdir_result) {
			sprintf(line, "\n");
		} else {
			sprintf(line, "%" SCNd64 "\n", result);
		}

		debug(D_CHIRP, "= %s", line);
		if(!do_no_result) {
			length = strlen(line);
			actual = link_putlstring(l, line, length, stalltime);

			if(actual != length)
				break;
		}

		if(dataout) {
			actual = link_putlstring(l, dataout, dataoutlength, stalltime);
			if(actual != dataoutlength)
				break;
			free(dataout);
			dataout = 0;
		}

	}
	free(esubject);
}

static int errno_to_chirp(int e)
{
	switch (e) {
	case EACCES:
	case EPERM:
	case EROFS:
		return CHIRP_ERROR_NOT_AUTHORIZED;
	case ENOENT:
		return CHIRP_ERROR_DOESNT_EXIST;
	case EEXIST:
		return CHIRP_ERROR_ALREADY_EXISTS;
	case EFBIG:
		return CHIRP_ERROR_TOO_BIG;
	case ENOSPC:
	case EDQUOT:
		return CHIRP_ERROR_NO_SPACE;
	case ENOMEM:
		return CHIRP_ERROR_NO_MEMORY;
#ifdef ENOATTR
	case ENOATTR:
#endif
	case ENOSYS:
	case EINVAL:
		return CHIRP_ERROR_INVALID_REQUEST;
	case EMFILE:
	case ENFILE:
		return CHIRP_ERROR_TOO_MANY_OPEN;
	case EBUSY:
		return CHIRP_ERROR_BUSY;
	case EAGAIN:
		return CHIRP_ERROR_TRY_AGAIN;
	case EBADF:
		return CHIRP_ERROR_BAD_FD;
	case EISDIR:
		return CHIRP_ERROR_IS_DIR;
	case ENOTDIR:
		return CHIRP_ERROR_NOT_DIR;
	case ENOTEMPTY:
		return CHIRP_ERROR_NOT_EMPTY;
	case EXDEV:
		return CHIRP_ERROR_CROSS_DEVICE_LINK;
	case EHOSTUNREACH:
		return CHIRP_ERROR_GRP_UNREACHABLE;
	case ESRCH:
		return CHIRP_ERROR_NO_SUCH_PROCESS;
	case ESPIPE:
		return CHIRP_ERROR_IS_A_PIPE;
	case ENOTSUP:
		return CHIRP_ERROR_NOT_SUPPORTED;
	default:
		debug(D_CHIRP, "zoiks, I don't know how to transform error %d (%s)\n", errno, strerror(errno));
		return CHIRP_ERROR_UNKNOWN;
	}
}
