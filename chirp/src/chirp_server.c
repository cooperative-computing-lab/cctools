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

#include "macros.h"
#include "debug.h"
#include "link.h"
#include "getopt.h"
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
#include "xmalloc.h"
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

/* The maximum chunk of memory the server will allocate to handle I/O */
#define MAX_BUFFER_SIZE (16*1024*1024)

static void chirp_receive(struct link *l);
static void chirp_handler(struct link *l, const char *addr, const char *subject);
static int errno_to_chirp(int e);

static int port = CHIRP_PORT;
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
static INT64_T minimum_space_free = 0;
static INT64_T root_quota = 0;
static int did_explicit_auth = 0;
static int max_child_procs = 0;
static int total_child_procs = 0;
static int config_pipe[2];

// XXX see if these can be made static

int exit_if_parent_fails = 0;
const char *listen_on_interface = 0;
const char *chirp_root_url = ".";
const char *chirp_root_path = 0;
const char *chirp_ticket_path = 0;
char *chirp_transient_path = "./";	/* local file system stuff */
pid_t chirp_master_pid = 0;
const char *chirp_super_user = "";
const char *chirp_group_base_url = 0;
int chirp_group_cache_time = 900;
char chirp_owner[USERNAME_MAX] = "";

struct chirp_filesystem *cfs = 0;

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("use: %s [options]\n", cmd);
	printf("The most common options are:\n");
	printf(" -r <url>    URL of storage directory, like  file://path or hdfs://host:port/path\n");
	printf(" -d <flag>   Enable debugging for this sybsystem\n");
	printf(" -o <file>   Send debugging output to this file.\n");
	printf(" -u <host>   Send status updates to this host. (default is %s)\n", CATALOG_HOST);
	printf(" -v          Show version info.\n");
	printf(" -h          This message.\n");

	printf("\nLess common options are:\n");
	printf(" -A <file>   Use this file as the default ACL.\n");
	printf(" -a <method> Enable this authentication method.\n");
	printf(" -c <dir>    Challenge directory for unix filesystem authentication.\n");
	printf(" -C          Do not create a core dump, even due to a crash.\n");
	printf(" -E          Exit if parent process dies.\n");
	printf(" -e <time>   Check for presence of parent at this interval. (default is %ds)\n", parent_check_timeout);
	printf(" -F <size>   Leave this much space free in the filesystem.\n");
	printf(" -G <url>    Base url for group lookups. (default: disabled)\n");
	printf(" -I <addr>   Listen only on this network interface.\n");
	printf(" -O <bytes>  Rotate debug file once it reaches this size.\n");
	printf(" -n <name>   Use this name when reporting to the catalog.\n");
	printf(" -M <count>  Set the maximum number of clients to accept at once. (default unlimited)\n");
	printf(" -p <port>   Listen on this port (default is %d)\n", port);
	printf(" -P <user>   Superuser for all directories. (default is none)\n");
	printf(" -Q <size>   Enforce this root quota in software.\n");
	printf(" -R          Read-only mode.\n");
	printf(" -s <time>   Abort stalled operations after this long. (default is %ds)\n", stall_timeout);
	printf(" -t <time>   Disconnect idle clients after this time. (default is %ds)\n", idle_timeout);
	printf(" -T <time>   Maximum time to cache group information. (default is %ds)\n", chirp_group_cache_time);
	printf(" -U <time>   Send status updates at this interval. (default is 5m)\n");
	printf(" -w <name>   The name of this server's owner.  (default is username)\n");
	printf(" -W <file>   Use alternate password file for unix authentication\n");
	printf(" -y <dir>    Location of transient data (default is pwd).\n");
	printf(" -z <time>   Set max timeout for unix filesystem authentication. (default is 5s)\n");
	printf("\n");
	printf("Where debug flags are: ");
	debug_flags_print(stdout);
	printf("\n\n");
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

static int update_all_catalogs( const char *url )
{
	struct chirp_statfs info;
	struct utsname name;
	char text[DATAGRAM_PAYLOAD_MAX];
	unsigned uptime;
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
	uptime = time(0) - starttime;

	length = sprintf(text,
			 "type chirp\nversion %d.%d.%d\nurl chirp://%s:%d\nname %s\nowner %s\ntotal %llu\navail %llu\nuptime %u\nport %d\ncpu %s\nopsys %s\nopsysversion %s\nload1 %0.02lf\nload5 %0.02lf\nload15 %0.02lf\nminfree %llu\nmemory_total %llu\nmemory_avail %llu\ncpus %d\nbackend %s\n",
			 CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, hostname, port, hostname, chirp_owner, info.f_blocks * info.f_bsize, info.f_bavail * info.f_bsize, uptime, port, name.machine, name.sysname, name.release, avg[0], avg[1], avg[2], minimum_space_free, memory_total, memory_avail, cpus, url);

	chirp_stats_summary(&text[length], DATAGRAM_PAYLOAD_MAX - length);
	list_iterate(catalog_host_list, update_one_catalog, text);

	return 0;
}

static int backend_setup( const char *url )
{
	const char *root_path;

	root_path = cfs->init(url);
	if(!root_path) fatal("couldn't initialize %s",url);

	int result = cfs_create_dir(root_path,0711);
	if(!result) fatal("couldn't create root directory %s: %s",root_path,strerror(errno));

	chirp_acl_init_root(root_path);

	result = cfs->chdir(root_path);
	if(result<0) fatal("couldn't move to %s: %s",root_path,strerror(errno));

	return 0;
}

static int gc_tickets( const char *url )
{
	const char * path = cfs->init(url);
	if(!path) fatal("couldn't initialize %s",url);

	chirp_ticket_path = path;

	chirp_acl_gctickets(path);

	return 0;
}

static int run_in_child_process( int (*func) ( const char *a ), const char *args, const char *name )
{
	debug(D_PROCESS,"*** %s starting ***",name);

	pid_t pid = fork();
	if(pid==0) {
		_exit(func(args));
	} else if(pid>0) {
		int status;
		while(waitpid(pid,&status,0)!=pid) { }
		debug(D_PROCESS,"*** %s complete ***",name);
		if(WIFEXITED(status)) {
			return WEXITSTATUS(status);
		} else {
			return -1;
		}
	} else {
		debug(D_PROCESS,"couldn't fork: %s",strerror(errno));
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

static void config_pipe_handler( int fd )
{
	char line[PIPE_BUF];
	char flag[PIPE_BUF];
	char subject[PIPE_BUF];
	char address[PIPE_BUF];
	UINT64_T ops, bytes_read, bytes_written;

	while(1) {
		fcntl(fd,F_SETFL,O_NONBLOCK);

		int length = read(fd,line,PIPE_BUF);
		if(length<=0) return;

		line[length] = 0;

		const char *msg = strtok(line,"\n");
		while(msg) {
			debug(D_DEBUG,"config message: %s",msg);

			if(sscanf(msg,"debug %s",flag)==1) {
				if(!strcmp(flag,"clear")) {
					debug_flags_clear();
				} else {
					debug_flags_set(flag);
				}
			} else if(sscanf(msg,"stats %s %s %llu %llu %llu",address,subject,&ops,&bytes_read,&bytes_written)==5) {
				chirp_stats_collect(address,subject,ops,bytes_read,bytes_written);
			} else {
				debug(D_NOTICE,"bad config message: %s\n",msg);
			}
			msg = strtok(0,"\n");
		}
	}
}


int main(int argc, char *argv[])
{
	struct link *link;
	char c;
	int c_input;
	time_t current;

	change_process_title_init(argv);
	change_process_title("chirp_server");

	chirp_master_pid = getpid();

	catalog_host_list = list_create();

	debug_config(argv[0]);

	/* Ensure that all files are created private by default. */
	umask(0077);

	while((c_input = getopt(argc, argv, "A:a:c:CEe:F:G:t:T:i:I:s:Sn:M:P:p:Q:r:Ro:O:d:vw:W:u:U:hXNL:f:y:x:z:")) != -1) {
		c = (char) c_input;
		switch (c) {
		case 'A':
			chirp_acl_default(optarg);
			break;
		case 'a':
			auth_register_byname(optarg);
			did_explicit_auth = 1;
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
		case 'S':
			/* deprecated */
			break;
		case 'r':
			chirp_root_url = optarg;
			break;
		case 'R':
			chirp_acl_force_readonly();
			break;
		case 'o':
			debug_config_file(optarg);
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
			show_version(argv[0]);
			return 1;
		case 'w':
			strcpy(chirp_owner, optarg);
			break;
		case 'W':
			auth_unix_passwd_file(optarg);
			break;
		case 'X':
			/* deprecated */
			break;
		case 'N':
			/* deprecated */
			break;
		case 'I':
			listen_on_interface = optarg;
			break;
		case 'L':
			/* deprecated */
			break;
		case 'f':
			fprintf(stderr,"chirp_server: option -f is deprecated, use -r with a URL instead.");
			break;
		case 'y':
			chirp_transient_path = optarg;
			break;
		case 'x':
			fprintf(stderr,"chirp_server: option -x is deprecated, use -r with a URL instead.");
			break;
		case 'z':
			auth_unix_timeout_set(atoi(optarg));
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	if(pipe(config_pipe)<0)
		fatal("could not create internal pipe: %s",strerror(errno));

	if(!create_dir(chirp_transient_path, 0711))
		fatal("could not create transient data directory '%s'", chirp_transient_path);

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

	if(run_in_child_process(backend_setup,chirp_root_url,"backend setup")!=0) {
		fatal("couldn't setup %s",chirp_root_url);
	}

	if(!list_size(catalog_host_list)) {
		list_push_head(catalog_host_list, CATALOG_HOST);
	}

	if(getuid() == 0) {
		if(!safe_username) {
			printf("Sorry, I refuse to run as root without certain safeguards.\n");
			printf("Please give me a safe username with the -i <user> option.\n");
			printf("After using root access to authenticate users,\n");
			printf("I will use the safe username to access data on disk.\n");
			exit(1);
		}
	} else if(safe_username) {
		printf("Sorry, the -i option doesn't make sense\n");
		printf("unless I am already running as root.\n");
		exit(1);
	}

	// XXX move allocation handling to the side.
	if(root_quota > 0) {
		if(cfs == &chirp_fs_hdfs)	/* using HDFS? Can't do quotas : / */
			fatal("Cannot use quotas with HDFS\n");
		else
			chirp_alloc_init(chirp_root_path, root_quota);
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

		while((pid = waitpid(-1,0,WNOHANG))>0) {
			debug(D_PROCESS,"pid %d completed (%d total child procs)",pid,total_child_procs);
			total_child_procs--;
		}

		if(time(0) >= advertise_alarm) {
			run_in_child_process(update_all_catalogs,chirp_root_url,"catalog update");
			advertise_alarm = time(0) + advertise_timeout;
			chirp_stats_cleanup();
		}

		if(time(0) >= gc_alarm) {
			run_in_child_process(gc_tickets,chirp_root_url,"ticket cleanup");
			gc_alarm = time(0) + gc_timeout;
		}

		/* Wait for action on one of two ports: the master TCP port, or the internal pipe. */
		/* If the limit of child procs has been reached, don't watch the TCP port. */

		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(config_pipe[0],&rfds);
		if(max_child_procs==0 || total_child_procs < max_child_procs) {
			FD_SET(link_fd(link),&rfds);
		}
		int maxfd = MAX(link_fd(link),config_pipe[0]) + 1;

		/* Sleep for the minimum of any periodic timers, but don't go negative. */

		struct timeval timeout;
		time_t current = time(0);
		timeout.tv_usec = 0;
		timeout.tv_sec = advertise_alarm-current;
		timeout.tv_sec = MIN(timeout.tv_sec,gc_alarm-current);
		timeout.tv_sec = MIN(timeout.tv_sec,parent_check_timeout);
		timeout.tv_sec = MAX(0,timeout.tv_sec);

		/* Wait for activity on the listening port or the config pipe */

		if(select(maxfd,&rfds,0,0,&timeout)<0) continue;

		/* If the network port is active, accept the connection and fork the handler. */

		if(FD_ISSET(link_fd(link),&rfds)) {
			l = link_accept(link, time(0) + 5 );
			if(!l) continue;

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

		if(FD_ISSET(config_pipe[0],&rfds)) {
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

	chirp_root_path = cfs->init(chirp_root_url);
	chirp_ticket_path = chirp_root_path;

        if(!chirp_root_path)
		fatal("could not initialize backend filesystem: %s", strerror(errno));

	if(cfs->chdir(chirp_root_path) != 0)
		fatal("couldn't move to %s: %s\n", chirp_root_path, strerror(errno));

	link_address_remote(link, addr, &port);

	auth_ticket_clear();
	struct hash_table *tickets = hash_table_create(0, 0);
	if(chirp_acl_gettickets(".", tickets) == 0) {
		char *key, *value;
		hash_table_firstkey(tickets);
		while(hash_table_nextkey(tickets, &key, (void **) &value)) {
			auth_ticket_add(key, value);
			free(value);
		}
	}
	hash_table_delete(tickets);

	if(auth_accept(link, &atype, &asubject, time(0) + idle_timeout)) {
		sprintf(typesubject, "%s:%s", atype, asubject);
		free(atype);
		free(asubject);

		debug(D_LOGIN, "%s from %s:%d", typesubject, addr, port);

		// XXX move getpwnam to main

		if(safe_username) {
			struct passwd *p;
			p = getpwnam(safe_username);
			if(!p)
				fatal("unknown user: %s", safe_username);
			safe_uid = p->pw_uid;
			safe_gid = p->pw_gid;
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
		chirp_stats_report(config_pipe[1],addr,typesubject,0);

		debug(D_LOGIN, "disconnected");
	} else {
		debug(D_LOGIN, "authentication failed from %s:%d", addr, port);
	}

	link_close(link);
}

/*
  Force a path to fall within the simulated root directory.
  XXX for safety, shouldn't we do collapse_path before prepending the root?
*/

int chirp_path_fix(char *path)
{
	char decodepath[CHIRP_PATH_MAX];
	char safepath[CHIRP_PATH_MAX];

	// Remove the percent-hex encoding
	url_decode(path, decodepath, sizeof(decodepath));

	// Add the Chirp root and copy it back out.
	sprintf(safepath, "%s/%s", chirp_root_path, decodepath);

	// Collapse dots, double dots, and the like:
	string_collapse_path(safepath, path, 1);

	return 1;
}

static int chirp_file_exists(const char *path)
{
	struct chirp_stat statbuf;
	if(chirp_alloc_lstat(path, &statbuf) == 0) {
		return 1;
	} else {
		return 0;
	}
}


char *chirp_stat_string(struct chirp_stat *info)
{
	static char line[CHIRP_LINE_MAX];

	sprintf(line, "%lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld", (long long) info->cst_dev, (long long) info->cst_ino, (long long) info->cst_mode, (long long) info->cst_nlink, (long long) info->cst_uid, (long long) info->cst_gid,
		(long long) info->cst_rdev, (long long) info->cst_size, (long long) info->cst_blksize, (long long) info->cst_blocks, (long long) info->cst_atime, (long long) info->cst_mtime, (long long) info->cst_ctime);

	return line;
}

char *chirp_statfs_string(struct chirp_statfs *info)
{
	static char line[CHIRP_LINE_MAX];

	sprintf(line, "%lld %lld %lld %lld %lld %lld %lld", (long long) info->f_type, (long long) info->f_bsize, (long long) info->f_blocks, (long long) info->f_bfree, (long long) info->f_bavail, (long long) info->f_files, (long long) info->f_ffree);

	return line;
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

	if(!chirp_acl_whoami( subject, &esubject))
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

		char debug_flag[CHIRP_LINE_MAX];

		INT64_T fd, length, flags, offset, actual;
		INT64_T uid, gid, mode;
		INT64_T size, inuse;
		INT64_T stride_length, stride_skip;
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

		chirp_stats_report(config_pipe[1],addr,subject,advertise_alarm);

		chirp_stats_update(1,0,0);

		debug(D_CHIRP, "%s", line);

		if(sscanf(line, "pread %lld %lld %lld", &fd, &length, &offset) == 3) {
			length = MIN(length, MAX_BUFFER_SIZE);
			dataout = malloc(length);
			if(dataout) {
				result = chirp_alloc_pread(fd, dataout, length, offset);
				if(result >= 0) {
					dataoutlength = result;
					chirp_stats_update(0,result,0);
				} else {
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
			}
		} else if(sscanf(line, "sread %lld %lld %lld %lld %lld", &fd, &length, &stride_length, &stride_skip, &offset) == 5) {
			length = MIN(length, MAX_BUFFER_SIZE);
			dataout = malloc(length);
			if(dataout) {
				result = chirp_alloc_sread(fd, dataout, length, stride_length, stride_skip, offset);
				if(result >= 0) {
					dataoutlength = result;
					chirp_stats_update(0,result,0);
				} else {
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
			}
		} else if(sscanf(line, "pwrite %lld %lld %lld", &fd, &length, &offset) == 3) {
			char *data;
			INT64_T orig_length = length;
			length = MIN(length, MAX_BUFFER_SIZE);
			data = malloc(length);
			if(data) {
				actual = link_read(l, data, length, stalltime);
				if(actual != length)
					break;
				if(space_available(length)) {
					result = chirp_alloc_pwrite(fd, data, length, offset);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				link_soak(l, (orig_length - length), stalltime);
				free(data);
				if(result > 0) {
					chirp_stats_update(0,0,result);
				}
			} else {
				link_soak(l, orig_length, stalltime);
				result = -1;
				errno = ENOMEM;
				break;
			}
		} else if(sscanf(line, "swrite %lld %lld %lld %lld %lld", &fd, &length, &stride_length, &stride_skip, &offset) == 5) {
			char *data;
			INT64_T orig_length = length;
			length = MIN(length, MAX_BUFFER_SIZE);
			data = malloc(length);
			if(data) {
				actual = link_read(l, data, length, stalltime);
				if(actual != length)
					break;
				if(space_available(length)) {
					result = chirp_alloc_swrite(fd, data, length, stride_length, stride_skip, offset);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				link_soak(l, (orig_length - length), stalltime);
				free(data);
				if(result > 0) {
					chirp_stats_update(0,0,result);
				}
			} else {
				link_soak(l, orig_length, stalltime);
				result = -1;
				errno = ENOMEM;
				break;
			}
		} else if(sscanf(line, "whoami %lld", &length) == 1) {
			if(strlen(esubject) < length)
				length = strlen(esubject);
			dataout = malloc(length);
			if(dataout) {
				dataoutlength = length;
				strncpy(dataout, esubject, length);
				result = length;
			} else {
				result = -1;
			}
		} else if(sscanf(line, "whoareyou %s %lld", hostname, &length) == 2) {
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
		} else if(sscanf(line, "readlink %s %lld", path, &length) == 2) {
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
			void *dir;
			const char *d;
			char subpath[CHIRP_PATH_MAX];

			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_LIST))
				goto failure;

			dir = chirp_alloc_opendir(path);
			if(dir) {
				link_putliteral(l, "0\n", stalltime);
				while((d = chirp_alloc_readdir(dir))) {
					if(!strncmp(d, ".__", 3))
						continue;
					link_putfstring(l, "%s\n", stalltime, d);
					sprintf(subpath, "%s/%s", path, d);
					chirp_alloc_lstat(subpath, &statbuf);
					link_putfstring(l, "%s\n", stalltime, chirp_stat_string(&statbuf));
				}
				chirp_alloc_closedir(dir);
				do_getdir_result = 1;
				result = 0;
			} else {
				result = -1;
			}
		} else if(sscanf(line, "getdir %s", path) == 1) {
			void *dir;
			const char *d;

			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_LIST))
				goto failure;

			dir = chirp_alloc_opendir(path);
			if(dir) {
				link_putliteral(l, "0\n", stalltime);
				while((d = chirp_alloc_readdir(dir))) {
					if(!strncmp(d, ".__", 3))
						continue;
					link_putfstring(l, "%s\n", stalltime, d);
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
				chirp_stats_update(0,length,0);
			}
		} else if(sscanf(line, "putfile %s %lld %lld", path, &mode, &length) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!cfs_isnotdir(path))
				goto failure;

			if(chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				/* writable, ok to proceed */
			} else if(chirp_acl_check(path, subject, CHIRP_ACL_PUT)) {
				if(chirp_file_exists(path)) {
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
				chirp_stats_update(0,0,length);
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
				chirp_stats_update(0,length,0);
				debug(D_CHIRP, "= %lld bytes streamed\n", result);
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
				if(chirp_file_exists(path)) {
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
				chirp_stats_update(0,0,length);
				debug(D_CHIRP, "= %lld bytes streamed\n", result);
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

		} else if(sscanf(line, "open %s %s %lld", path, newpath, &mode) == 3) {
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
						if(chirp_file_exists(path)) {
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


		} else if(sscanf(line, "close %lld", &fd) == 1) {
			result = chirp_alloc_close(fd);
		} else if(sscanf(line, "fchmod %lld %lld", &fd, &mode) == 2) {
			result = chirp_alloc_fchmod(fd, mode);
		} else if(sscanf(line, "fchown %lld %lld %lld", &fd, &uid, &gid) == 3) {
			result = 0;
		} else if(sscanf(line, "fsync %lld", &fd) == 1) {
			result = chirp_alloc_fsync(fd);
		} else if(sscanf(line, "ftruncate %lld %lld", &fd, &length) == 2) {
			result = chirp_alloc_ftruncate(fd, length);
		} else if(sscanf(line, "unlink %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check_link(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)
				) {
				result = chirp_alloc_unlink(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "access %s %lld", path, &flags) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			flags = chirp_acl_from_access_flags(flags);
			/* If filename is a directory, then we change execute flags to list flags. */
			if(cfs_isdir(path) && (flags & CHIRP_ACL_EXECUTE)) {
				flags ^= CHIRP_ACL_EXECUTE; /* remove execute flag */
				flags |= CHIRP_ACL_LIST; /* change to list */
			}
			if(!chirp_acl_check(path, subject, flags))
				goto failure;
			result = chirp_alloc_access(path, flags);
		} else if(sscanf(line, "chmod %s %lld", path, &mode) == 2) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check_dir(path, subject, CHIRP_ACL_WRITE) || chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				result = chirp_alloc_chmod(path, mode);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "chown %s %lld %lld", path, &uid, &gid) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = 0;
		} else if(sscanf(line, "lchown %s %lld %lld", path, &uid, &gid) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = 0;
		} else if(sscanf(line, "truncate %s %lld", path, &length) == 2) {
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
		} else if(sscanf(line, "ticket_register %s %s %lld", newsubject, duration, &length) == 3) {
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
		} else if(sscanf(line, "mkdir %s %lld", path, &mode) == 2) {
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
			if(chirp_acl_check(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)) {
				result = chirp_alloc_rmdir(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "rmall %s", path) == 1) {
			if(!chirp_path_fix(path))
				goto failure;
			if(chirp_acl_check(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)) {
				result = chirp_alloc_rmall(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "utime %s %lld %lld", path, &actime, &modtime) == 3) {
			if(!chirp_path_fix(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = chirp_alloc_utime(path, actime, modtime);
		} else if(sscanf(line, "fstat %lld", &fd) == 1) {
			result = chirp_alloc_fstat(fd, &statbuf);
			do_stat_result = 1;
		} else if(sscanf(line, "fstatfs %lld", &fd) == 1) {
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
				link_putfstring(l, "0\n%s %lld %lld\n", stalltime, &newpath[strlen(chirp_root_path) + 1], size, inuse);
				do_no_result = 1;
			}
		} else if(sscanf(line, "mkalloc %s %lld %lld", path, &size, &mode) == 3) {
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
					link_putfstring(l, "%s %lld %lld %lld\n", stalltime, key, entry->nfiles, entry->ndirs, entry->nbytes);
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
			if(chirp_alloc_md5(path, (unsigned char *) dataout)) {
				result = dataoutlength = 16;
			} else {
				result = errno_to_chirp(errno);
			}
		} else if(sscanf(line, "debug %s", debug_flag) == 1) {
			if(strcmp(esubject, chirp_super_user) != 0) {
				errno = EPERM;
				goto failure;
			}
			result = 0;
			// send this message to the parent for processing.
			strcat(line,"\n");
			write(config_pipe[1],line,strlen(line));
		} else {
			result = -1;
			errno = ENOSYS;
		}

		if(do_no_result) {
			/* nothing */
		} else if(result < 0) {
		      failure:
			result = errno_to_chirp(errno);
			sprintf(line, "%lld\n", result);
		} else if(do_stat_result) {
			sprintf(line, "%lld\n%s\n", (long long) result, chirp_stat_string(&statbuf));
		} else if(do_statfs_result) {
			sprintf(line, "%lld\n%s\n", (long long) result, chirp_statfs_string(&statfsbuf));
		} else if(do_getdir_result) {
			sprintf(line, "\n");
		} else {
			sprintf(line, "%lld\n", result);
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
