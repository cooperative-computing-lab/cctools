/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "chirp_protocol.h"
#include "chirp_acl.h"
#include "chirp_reli.h"
#include "chirp_group.h"
#include "chirp_stats.h"
#include "chirp_alloc.h"
#include "chirp_audit.h"
#include "chirp_thirdput.h"
#include "chirp_job.h"

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
#include "catalog_server.h"
#include "domain_name_cache.h"
#include "create_dir.h"
#include "list.h"
#include "xmalloc.h"
#include "md5.h"
#include "load_average.h"
#include "memory_info.h"
#include "change_process_title.h"
#include "url_encode.h"

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

/* The maximum chunk of memory the server will allocate to handle I/O */
#define MAX_BUFFER_SIZE (16*1024*1024)

static void chirp_receive( struct link *l );
static void chirp_handler( struct link *l, const char *subject );
static int errno_to_chirp( int e );

static int port = CHIRP_PORT;
static int idle_timeout = 60; /* one minute */
static int stall_timeout = 3600; /* one hour */
static int advertise_timeout = 300; /* five minutes */
static int advertise_alarm = 0;
static int single_mode = 0;
static struct list *catalog_host_list;
static char owner[USERNAME_MAX] = "";
static time_t starttime;
static struct datagram *catalog_port;
static char hostname[DOMAIN_NAME_MAX];
static const char *manual_hostname = 0;
static char address[LINK_ADDRESS_MAX];
static const char *startdir = ".";
static const char *safe_username = 0;
static uid_t safe_uid = 0;
static gid_t safe_gid = 0;
static int allow_execute = 0;
static int did_acl_default = 0;
static int dont_dump_core = 0;
static INT64_T minimum_space_free = 0;
static INT64_T root_quota = 0;
static int extra_latency = 0;
static int max_job_wait_timeout = 300;
static int did_explicit_auth = 0;
static int max_child_procs = 0;
static int total_child_procs = 0;

struct chirp_stats *global_stats = 0;
struct chirp_stats *local_stats = 0;

int        enable_identity_boxing = 1;
const char *chirp_server_path = 0;
const char *listen_on_interface = 0;
char       chirp_root_path[CHIRP_PATH_MAX];
pid_t      chirp_master_pid = 0;
const char *chirp_super_user = "";
const char *chirp_group_base_url = 0;
int         chirp_group_cache_time = 900;

static void show_version( const char *cmd )
{
        printf("%s version %d.%d.%d built by %s@%s on %s at %s\n",cmd,CCTOOLS_VERSION_MAJOR,CCTOOLS_VERSION_MINOR,CCTOOLS_VERSION_MICRO,BUILD_USER,BUILD_HOST,__DATE__,__TIME__);
}

static void show_help( const char *cmd )
{
	printf("use: %s [options]\n",cmd);
	printf("where options are:\n");
	printf(" -A <file>   Use this file as the default ACL.\n");
	printf(" -a <method> Enable this authentication method.\n");
	printf(" -d <flag>   Enable debugging for this sybsystem\n");
	printf(" -c <dir>    Challenge directory for filesystem authentication.\n");
	printf(" -C          Do not create a core dump, even due to a crash.\n");
	printf(" -F <size>   Leave this much space free in the filesystem.\n");
	printf(" -G <url>    Base url for group lookups. (default: disabled)\n");
	printf(" -h          This message.\n");
	printf(" -I <addr>   Listen only on this network interface.\n");
	printf(" -o <file>   Send debugging output to this file.\n");
	printf(" -O <bytes>  Rotate debug file once it reaches this size.\n");
	printf(" -n <name>   Use this name when reporting to the catalog.\n");
	printf(" -M <count>  Set the maximum number of clients to accept at once. (default unlimited)\n");
	printf(" -p <port>   Listen on this port (default is %d)\n",port);
	printf(" -P <user>   Superuser for all directories. (default is none)\n");
	printf(" -Q <size>   Enforce this root quota in software.\n");
	printf(" -r <dir>    Root of storage directory. (default is current dir)\n");
	printf(" -R          Read-only / read-everything mode.\n");
	printf(" -s <time>   Abort stalled operations after this long. (default is %ds)\n",stall_timeout);
	printf(" -S          Single process mode, do not fork.\n");
	printf(" -t <time>   Disconnect idle clients after this time. (default is %ds)\n",idle_timeout);
	printf(" -T <time>   Maximum time to cache group information. (default is %ds)\n",chirp_group_cache_time);
	printf(" -u <host>   Send status updates to this host. (default is %s)\n",CATALOG_HOST);
	printf(" -U <time>   Send status updates at this interval. (default is 5m)\n");
	printf(" -v          Show version info.\n");
	printf(" -w <name>   The name of this server's owner.  (default is username)\n");
	printf(" -W <file>   Use alternate password file for unix authentication\n");
	printf(" -X          Enable remote execution.\n");
	printf(" -N          Disable identity boxing for execution.  (discouraged)\n");
	printf("\n");
	printf("Where debug flags are: ");
	debug_flags_print(stdout);
	printf("\n\n");
}

void shutdown_clean( int sig )
{
	exit(0);
}

void ignore_signal( int sig )
{
}

void reap_child( int sig )
{
	pid_t pid;
	int status;

	do {
		pid = waitpid(-1,&status,WNOHANG);
		if(pid>0) total_child_procs--;
	} while(pid>0);
}

static void install_handler( int sig, void (*handler)(int sig))
{
	struct sigaction s;
	s.sa_handler = handler;
	sigfillset(&s.sa_mask);
	s.sa_flags = 0; 
	sigaction(sig,&s,0);
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

static int space_available( INT64_T amount )
{
	static UINT64_T total, avail;
	static time_t last_check=0;
	int check_interval=1;
	time_t current;

	if(minimum_space_free==0) return 1;

	current = time(0);

	if( (current-last_check) > check_interval) {
		disk_info_get(chirp_root_path,&avail,&total);
		last_check = current;
	}

	if((avail-amount)>minimum_space_free) {
		avail -= amount;
		return 1;
	} else {
		errno = ENOSPC;
		return 0;
	}
}

int update_one_catalog( void *catalog_host, const void *text )
{
	char addr[DATAGRAM_ADDRESS_MAX];
	if(domain_name_cache_lookup(catalog_host,addr)) {
		debug(D_DEBUG,"sending update to %s:%d",catalog_host,CATALOG_PORT);
		datagram_send(catalog_port,text,strlen(text),addr,CATALOG_PORT);
	}
	return 1;
}

static void update_all_catalogs()
{
	struct chirp_statfs info;
	struct utsname name;
	char text[DATAGRAM_PAYLOAD_MAX];
	unsigned uptime;
	int length;
	int cpus;
	double avg[3];
	UINT64_T memory_total,memory_avail;

	uname(&name);
	string_tolower(name.sysname);
	string_tolower(name.machine);
	string_tolower(name.release);
	load_average_get(avg);
	cpus = load_average_get_cpus();
	chirp_alloc_statfs(chirp_root_path,&info);
	memory_info_get(&memory_avail,&memory_total);
	uptime = time(0)-starttime;

	length = sprintf(
		text,
		"type chirp\nversion %d.%d.%d\nurl chirp://%s:%d\nname %s\nowner %s\ntotal %llu\navail %llu\nuptime %u\nport %d\nbytes_written %lld\nbytes_read %lld\ntotal_ops %d\ncpu %s\nopsys %s\nopsysversion %s\nload1 %0.02lf\nload5 %0.02lf\nload15 %0.02lf\nminfree %llu\nmemory_total %llu\nmemory_avail %llu\ncpus %d\n",
		CCTOOLS_VERSION_MAJOR,
		CCTOOLS_VERSION_MINOR,
		CCTOOLS_VERSION_MICRO,
		hostname,
		port,
		hostname,
		owner,
		info.f_blocks*info.f_bsize,
		info.f_bavail*info.f_bsize,
		uptime,
		port,
		global_stats->bytes_written,
		global_stats->bytes_read,
		global_stats->total_ops,
		name.machine,
		name.sysname,
		name.release,
		avg[0],
		avg[1],
		avg[2],
		minimum_space_free,
		memory_total,
		memory_avail,
		cpus
		);

	chirp_stats_summary(&text[length],DATAGRAM_PAYLOAD_MAX-length);
	chirp_stats_cleanup();
	list_iterate(catalog_host_list,update_one_catalog,text);
}

int main( int argc, char *argv[] )
{
	struct link *link;
	char c;
	int c_input;
	time_t current;

	change_process_title_init(argv);
	change_process_title("chirp_server");

	chirp_server_path = argv[0];
	chirp_master_pid = getpid();

	catalog_host_list = list_create();

	debug_config(argv[0]);

	/* Ensure that all files are created private by default. */
	umask(0077);

	while((c_input = getopt( argc,argv,"A:a:c:CF:G:t:T:i:I:s:Sn:M:P:p:Q:r:Ro:O:d:vw:W:u:U:hXNL:")) != -1 ) {
	        c = ( char ) c_input;
		switch(c) {
		case 'A':
			chirp_acl_default(optarg);
			did_acl_default = 1;
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
			single_mode = 1;
			break;
		case 'r':
			startdir = optarg;
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
			list_push_head(catalog_host_list,strdup(optarg));
			break;
		case 'U':
			advertise_timeout = string_time_parse(optarg);
			break;
		case 'v':
			show_version(argv[0]);
			return 1;
		case 'w':
			strcpy(owner,optarg);
			break;
		case 'W':
			auth_unix_passwd_file(optarg);
			break;
		case 'X':
			allow_execute = 1;
			break;
		case 'N':
			enable_identity_boxing = 0;
			break;
		case 'I':
			listen_on_interface = optarg;
			break;
		case 'L':
			extra_latency = atoi(optarg);
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	if(dont_dump_core) {
		struct rlimit rl;
		rl.rlim_cur = rl.rlim_max = 0;
		setrlimit(RLIMIT_CORE,&rl);
	}

	current = time(0);
	debug(D_ALL,"*** %s starting at %s",argv[0],ctime(&current));

	if(!list_size(catalog_host_list)) {
		list_push_head(catalog_host_list,CATALOG_HOST);
	}

	if(getuid()==0) {
		struct passwd *p;
		if(!safe_username) {		
			printf("Sorry, I refuse to run as root without certain safeguards.\n");
			printf("Please give me a safe username with the -i <user> option.\n");
			printf("After using root access to authenticate users,\n");
			printf("I will use the safe username to access data on disk.\n");
			exit(1);
		}
		p = getpwnam(safe_username);
		if(!p) fatal("unknown user: %s",safe_username);
		safe_uid = p->pw_uid;
		safe_gid = p->pw_gid;
		chown(startdir,safe_uid,safe_gid);
		chmod(startdir,0700);
	} else {
		if(safe_username) {
			printf("Sorry, the -i option doesn't make sense\n");
			printf("unless I am already running as root.\n");
			exit(1);
		}
	}

	if(!create_dir(startdir,0711))
		fatal("couldn't create %s: %s\n",startdir,strerror(errno));

	/* It's ok if this fails because there is a default permission check. */
        /* Note that it might fail if we are exporting a read-only volume. */
        chirp_acl_init_root(startdir);
	chirp_stats_init();
	chmod(startdir,0755);

	if(root_quota>0)
		chirp_alloc_init(startdir,root_quota);
	if(chdir(startdir)!=0)
		fatal("couldn't move to %s: %s\n",startdir,strerror(errno));
	if(getcwd(chirp_root_path,sizeof(chirp_root_path))<0)
		fatal("couldn't get working dir: %s\n",strerror(errno));

	link = link_serve_address(listen_on_interface,port);
	if(!link) {
		if(listen_on_interface) {
			fatal("couldn't listen on interface %s port %d: %s",listen_on_interface,port,strerror(errno));
		} else {
			fatal("couldn't listen on port %d: %s",port,strerror(errno));
		}
	}

	link_address_local(link,address,&port);

	if(!did_explicit_auth) {
		auth_register_all();
	}

	starttime = time(0);
	catalog_port = datagram_create(0);
	if(manual_hostname) {
		strcpy(hostname,manual_hostname);
	} else {
		domain_name_cache_guess(hostname);
	}
	if(!owner[0]) {
		if(!username_get(owner)) {
			strcpy(owner,"unknown");
		}
	}

	install_handler(SIGPIPE,ignore_signal);
	install_handler(SIGHUP,ignore_signal);
	install_handler(SIGCHLD,reap_child);
	install_handler(SIGINT,shutdown_clean);
	install_handler(SIGTERM,shutdown_clean);
	install_handler(SIGQUIT,shutdown_clean);
	install_handler(SIGXFSZ,ignore_signal);

	if(allow_execute) {
		if(fork()==0) {
			chirp_job_starter();
		}
	}

	global_stats = chirp_stats_global();

	while(1) {
		char addr[LINK_ADDRESS_MAX];
		int port;
		struct link *l;
		pid_t pid;

		if(time(0)>advertise_alarm) {
			update_all_catalogs();
			advertise_alarm = time(0)+advertise_timeout;
		}

		if(max_child_procs>0) {
			if(total_child_procs>=max_child_procs) {
				sleep(1);
				continue;
			}
		}

		l = link_accept(link,time(0)+advertise_timeout);
		if(!l) continue;
		link_address_remote(l,addr,&port);

		local_stats = chirp_stats_local_begin(addr);

		if(single_mode) {
			chirp_receive(l);
		} else {
			pid = fork();
			if(pid==0) {
				change_process_title("chirp_server [authenticating]");
				install_handler(SIGCHLD,ignore_signal);
				chirp_receive(l);
				_exit(0);
			} else if(pid>=0) {
				total_child_procs++;
				debug(D_CHIRP,"created pid %d (%d total child procs)",pid,total_child_procs);
			} else {
				debug(D_CHIRP,"couldn't fork: %s",strerror(errno));
			}
			link_close(l);
		}
	}
}

void millisleep( int n )
{
	if(n==1) {
		int i;
		for(i=0;i<1000000;i++) { }
	} else {
		usleep(n*1000);
	}
}

static void chirp_receive( struct link *link )
{
	char *atype, *asubject;
	char typesubject[AUTH_TYPE_MAX+AUTH_SUBJECT_MAX];
	char addr[LINK_ADDRESS_MAX];
	int port;

	link_address_remote(link,addr,&port);

	if(extra_latency) millisleep(extra_latency*4);

	if(auth_accept(link,&atype,&asubject,time(0)+idle_timeout)) {
		sprintf(typesubject,"%s:%s",atype,asubject);
		free(atype);
		free(asubject);

		debug(D_LOGIN,"%s from %s:%d",typesubject,addr,port);

		if(safe_username) {
			debug(D_AUTH,"changing to uid %d gid %d",safe_uid,safe_gid);
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
		change_process_title("chirp_server [%s:%d] [%s]",addr,port,typesubject);
		chirp_handler(link,typesubject); 
		chirp_alloc_flush();

		debug(D_LOGIN,"disconnected");
	} else {
		debug(D_LOGIN,"authentication failed from %s:%d",addr,port);
	}

	link_close(link);
	chirp_stats_local_end(local_stats);
	chirp_stats_sync();
}

/*
  Force a path to fall within the simulated root directory.
*/

int chirp_path_fix( char *path )
{
	char decodepath[CHIRP_PATH_MAX];
	char safepath[CHIRP_PATH_MAX];

	// Remove the percent-hex encoding
	url_decode(path,decodepath,sizeof(decodepath));

	// Collapse dots, double dots, and the like:
	string_collapse_path(decodepath,safepath,1);

	// Add the Chirp root and copy it back out.
	sprintf(path,"%s/%s",chirp_root_path,safepath);

	return 1;
}

static int chirp_not_directory( const char *path )
{
	struct chirp_stat statbuf;

	if(chirp_alloc_stat(path,&statbuf)==0) {
		if(S_ISDIR(statbuf.cst_mode)) {
			errno = EISDIR;
			return 0;
		} else {
			return 1;
		}
	} else {
		return 1;
	}
}

static int chirp_is_directory( const char *path )
{
	struct chirp_stat statbuf;

	if(chirp_alloc_stat(path,&statbuf)==0) {
		if(S_ISDIR(statbuf.cst_mode)) {
			return 1;
		} else {
			errno = ENOTDIR;
			return 0;
		}
	} else {
		return 1;
	}
}

static int chirp_file_exists( const char *path )
{
	struct chirp_stat statbuf;
	if(chirp_alloc_lstat(path,&statbuf)==0) {
		return 1;
	} else {
		return 0;
	}
}


char * chirp_stat_string( struct chirp_stat *info )
{
	static char line[CHIRP_LINE_MAX];

	sprintf(line,"%lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld",
		(long long) info->cst_dev,
		(long long) info->cst_ino,
		(long long) info->cst_mode,
		(long long) info->cst_nlink,
		(long long) info->cst_uid,
		(long long) info->cst_gid,
		(long long) info->cst_rdev,
		(long long) info->cst_size,
		(long long) info->cst_blksize,
		(long long) info->cst_blocks,
		(long long) info->cst_atime,
		(long long) info->cst_mtime,
		(long long) info->cst_ctime
	);

	return line;
}

char * chirp_statfs_string( struct chirp_statfs *info )
{
	static char line[CHIRP_LINE_MAX];

	sprintf(line,"%lld %lld %lld %lld %lld %lld %lld",
		(long long) info->f_type,
       		(long long) info->f_bsize,
		(long long) info->f_blocks,
		(long long) info->f_bfree,
		(long long) info->f_bavail,
		(long long) info->f_files,
		(long long) info->f_ffree
	);

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

static void chirp_handler( struct link *l, const char *subject )
{
	char line[CHIRP_LINE_MAX];
	
	link_tune(l,LINK_TUNE_INTERACTIVE);

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
		char newacl[CHIRP_LINE_MAX];
		char hostname[CHIRP_LINE_MAX];

		char jobcwd[CHIRP_PATH_MAX];
		char infile[CHIRP_PATH_MAX];
		char outfile[CHIRP_PATH_MAX];
		char errfile[CHIRP_PATH_MAX];
		INT64_T jobid;
		struct chirp_job_state jobstate;
		int  wait_timeout;
		
		INT64_T fd, length, flags, offset, actual;
		INT64_T uid, gid, mode;
		INT64_T size, inuse;
		INT64_T stride_length, stride_skip;
		struct chirp_stat statbuf;
		struct chirp_statfs statfsbuf;
		INT64_T actime, modtime;
		time_t idletime = time(0)+idle_timeout;
		time_t stalltime = time(0)+stall_timeout;
		char args[CHIRP_LINE_MAX];

		if(chirp_alloc_flush_needed()) {
			if(!link_usleep(l,100000,1,0)) {
				chirp_alloc_flush();
			}
		}

		if(!link_readline(l,line,sizeof(line),idletime)) {
			debug(D_CHIRP,"timeout: client idle too long\n");
			break;
		}

		if(extra_latency) millisleep(extra_latency);

		string_chomp(line);
		if(strlen(line)<1) continue;
		if(line[0]==4) break;

		global_stats->total_ops++;
		local_stats->total_ops++;

		debug(D_CHIRP,"%s",line);

		if(sscanf(line,"pread %lld %lld %lld",&fd,&length,&offset)==3) {
			length = MIN(length,MAX_BUFFER_SIZE);
			dataout = malloc(length);
			if(dataout) {
				result = chirp_alloc_pread(fd,dataout,length,offset);
				if(result>=0) {
					dataoutlength = result;
					global_stats->bytes_read += result;
					local_stats->bytes_read += result;
				} else {
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
			}
		} else if(sscanf(line,"sread %lld %lld %lld %lld %lld",&fd,&length,&stride_length,&stride_skip,&offset)==5) {
			length = MIN(length,MAX_BUFFER_SIZE);
			dataout = malloc(length);
			if(dataout) {
				result = chirp_alloc_sread(fd,dataout,length,stride_length,stride_skip,offset);
				if(result>=0) {
					dataoutlength = result;
					global_stats->bytes_read += result;
					local_stats->bytes_read += result;
				} else {
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
			}
		} else if(sscanf(line,"pwrite %lld %lld %lld",&fd,&length,&offset)==3) {
			char *data;
			INT64_T orig_length = length;
			length = MIN(length,MAX_BUFFER_SIZE);
			data = malloc(length);
			if(data) {
				actual = link_read(l,data,length,stalltime);
				if(actual!=length) break;
				if(space_available(length)) {
					result = chirp_alloc_pwrite(fd,data,length,offset);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				link_soak(l,(orig_length-length),stalltime);
				free(data);
				if(result>0) {
					global_stats->bytes_written += result;
					local_stats->bytes_written += result;
				}
			} else {
				link_soak(l,orig_length,stalltime);
				result = -1;
				errno = ENOMEM;
				break;
			}
		} else if(sscanf(line,"swrite %lld %lld %lld %lld %lld",&fd,&length,&stride_length,&stride_skip,&offset)==5) {
			char *data;
			INT64_T orig_length = length;
			length = MIN(length,MAX_BUFFER_SIZE);
			data = malloc(length);
			if(data) {
				actual = link_read(l,data,length,stalltime);
				if(actual!=length) break;
				if(space_available(length)) {
					result = chirp_alloc_swrite(fd,data,length,stride_length,stride_skip,offset);
				} else {
					result = -1;
					errno = ENOSPC;
				}
				link_soak(l,(orig_length-length),stalltime);
				free(data);
				if(result>0) {
					global_stats->bytes_written += result;
					local_stats->bytes_written += result;
				}
			} else {
				link_soak(l,orig_length,stalltime);
				result = -1;
				errno = ENOMEM;
				break;
			}
		} else if(sscanf(line,"whoami %lld",&length)==1) {
			if(strlen(subject)<length) length = strlen(subject);
			dataout = malloc(length);
			if(dataout) {
				dataoutlength = length;
				strncpy(dataout,subject,length);
				result = length;
			} else {
				result = -1;
			}
		} else if(sscanf(line,"whoareyou %s %lld",hostname,&length)==2) {
			result = chirp_reli_whoami(hostname,newsubject,sizeof(newsubject),idletime);
			if(result>0) {
				if(result>length) result=length;
				dataout = malloc(result);
				if( dataout ){
					dataoutlength = result;
					strncpy(dataout,newsubject,result);
				} else {
					errno = ENOMEM;
					result = -1;
				}
			} else {
				result = -1;
			}
		} else if(sscanf(line,"readlink %s %lld",path,&length)==2) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check_link(path,subject,CHIRP_ACL_READ)) goto failure;
			dataout = malloc(length);
			if(dataout) {
				result = chirp_alloc_readlink(path,dataout,length);
				if(result>=0) {
					dataoutlength = result;
				} else {
					free(dataout);
					dataout = 0;
				}
			} else {
				errno = ENOMEM;
				result = -1;
			}
		} else if(sscanf(line,"getlongdir %s",path)==1) {
			void *dir;
			const char *d;
			char subpath[CHIRP_PATH_MAX];

			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check_dir(path,subject,CHIRP_ACL_LIST)) goto failure;

			dir = chirp_alloc_opendir(path);
			if(dir) {
				link_write(l,"0\n",2,stalltime);
				while((d=chirp_alloc_readdir(dir))) {
					if(!strncmp(d,".__",3)) continue;
					sprintf(line,"%s\n",d);
					link_write(l,line,strlen(line),stalltime);
					sprintf(subpath,"%s/%s",path,d);
					chirp_alloc_lstat(subpath,&statbuf);
					sprintf(line,"%s\n",chirp_stat_string(&statbuf));
					link_write(l,line,strlen(line),stalltime);					
				}
				chirp_alloc_closedir(dir);
				do_getdir_result = 1;
				result = 0;
			} else {
				result = -1;
			}
		} else if(sscanf(line,"getdir %s",path)==1) {
			void *dir;
			const char *d;

			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check_dir(path,subject,CHIRP_ACL_LIST)) goto failure;

			dir = chirp_alloc_opendir(path);
			if(dir) {
				link_write(l,"0\n",2,stalltime);
				while((d=chirp_alloc_readdir(dir))) {
					if(!strncmp(d,".__",3)) continue;
					sprintf(line,"%s\n",d);
					link_write(l,line,strlen(line),stalltime);
				}
				chirp_alloc_closedir(dir);
				do_getdir_result = 1;
				result = 0;
			} else {
				result = -1;
			}
		} else if(sscanf(line,"getacl %s",path)==1) {
			char aclsubject[CHIRP_LINE_MAX];
			int aclflags;
			FILE *aclfile;

			if(!chirp_path_fix(path)) goto failure;

			// Previously, the LIST right was necessary to view the ACL.
			// However, this has caused much confusion with debugging permissions problems.
			// As an experiment, let's trying making getacl accessible to everyone.
	
			// if(!chirp_acl_check_dir(path,subject,CHIRP_ACL_LIST)) goto failure;

			aclfile = chirp_acl_open(path);
			if(aclfile) {
				link_write(l,"0\n",2,stalltime);
				while(chirp_acl_read(aclfile,aclsubject,&aclflags)) {
					sprintf(line,"%s %s\n",aclsubject,chirp_acl_flags_to_text(aclflags));
					link_write(l,line,strlen(line),stalltime);
				}
				chirp_acl_close(aclfile);
				do_getdir_result = 1;
				result = 0;
			} else {
				result = -1;
			}
		} else if(sscanf(line,"getfile %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_not_directory(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_READ)) goto failure;

			result = chirp_alloc_getfile(path,l,stalltime);

			if(result>=0) {
				do_no_result=1;
				global_stats->bytes_read += length;
				local_stats->bytes_read += length;
			}
		} else if(sscanf(line,"putfile %s %lld %lld",path,&mode,&length)==3) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_not_directory(path)) goto failure;

			if(chirp_acl_check(path,subject,CHIRP_ACL_WRITE)) {
				/* writable, ok to proceed */
			} else if(chirp_acl_check(path,subject,CHIRP_ACL_PUT)) {
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

			if(!space_available(length)) goto failure;

			result = chirp_alloc_putfile(path,l,length,mode,stalltime);
			if(result>=0) {
				global_stats->bytes_written += length;
				local_stats->bytes_written += length;
			}
		} else if(sscanf(line,"getstream %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_not_directory(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_READ)) goto failure;

			result = chirp_alloc_getstream(path,l,stalltime);
			if(result>=0) {
				global_stats->bytes_read += length;
				local_stats->bytes_read += length;
				debug(D_CHIRP,"= %lld bytes streamed\n",result);
				/* getstream indicates end by closing the connection */
				break;
			}

		} else if(sscanf(line,"putstream %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_not_directory(path)) goto failure;

			if(chirp_acl_check(path,subject,CHIRP_ACL_WRITE)) {
				/* writable, ok to proceed */
			} else if(chirp_acl_check(path,subject,CHIRP_ACL_PUT)) {
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

			result = chirp_alloc_putstream(path,l,stalltime);
			if(result>=0) {
				global_stats->bytes_written += length;
				local_stats->bytes_written += length;
				debug(D_CHIRP,"= %lld bytes streamed\n",result);
				/* putstream getstream indicates end by closing the connection */
				break;
			}
		} else if(sscanf(line,"thirdput %s %s %s",path,hostname,newpath)==3) {
			if(!chirp_path_fix(path)) goto failure;

			/* ACL check will occur inside of chirp_thirdput */

			result = chirp_thirdput(subject,path,hostname,newpath,stalltime);

		} else if(sscanf(line,"open %s %s %lld",path,newpath,&mode)==3) {
			flags = 0;

			if(strchr(newpath,'r')) {
				if(strchr(newpath,'w')) {
					flags = O_RDWR;
				} else {
					flags = O_RDONLY;
				}
			} else if(strchr(newpath,'w')) {
				flags = O_WRONLY;
			}

			if(strchr(newpath,'c')) flags |= O_CREAT;
			if(strchr(newpath,'t')) flags |= O_TRUNC;
			if(strchr(newpath,'a'))	flags |= O_APPEND;
			if(strchr(newpath,'x')) flags |= O_EXCL;
#ifdef O_SYNC
			if(strchr(newpath,'s')) flags |= O_SYNC;
#endif

			if(!chirp_path_fix(path)) goto failure;

			/*
			This is a little strange.
			For ordinary files, we check the ACL according
			to the flags passed to open.  For some unusual
			cases in Unix, we must also allow open()  for
			reading on a directory, otherwise we fail
			with EISDIR.
			*/

			if(chirp_not_directory(path)) {
				if(chirp_acl_check(path,subject,chirp_acl_from_open_flags(flags))) {
					/* ok to proceed */
				} else if(chirp_acl_check(path,subject,CHIRP_ACL_PUT)) {
					if(flags&O_CREAT) {
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
			} else if(flags==O_RDONLY) {
				if(!chirp_acl_check_dir(path,subject,CHIRP_ACL_LIST)) goto failure;
			} else {
				errno = EISDIR;
				goto failure;
			}

			result = chirp_alloc_open(path,flags,(int)mode);
			if(result>=0) {
				chirp_alloc_fstat(result,&statbuf);
				do_stat_result = 1;
			}

			
		} else if(sscanf(line,"close %lld",&fd)==1) {
			result = chirp_alloc_close(fd);
		} else if(sscanf(line,"fchmod %lld %lld",&fd,&mode)==2) {
			result = chirp_alloc_fchmod(fd,mode);			
		} else if(sscanf(line,"fchown %lld %lld %lld",&fd,&uid,&gid)==3) {
			result = 0;
		} else if(sscanf(line,"fsync %lld",&fd)==1) {
			result = chirp_alloc_fsync(fd);
		} else if(sscanf(line,"ftruncate %lld %lld",&fd,&length)==2) {
			result = chirp_alloc_ftruncate(fd,length);
		} else if(sscanf(line,"unlink %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(
				chirp_acl_check_link(path,subject,CHIRP_ACL_DELETE) ||
				chirp_acl_check_dir(path,subject,CHIRP_ACL_DELETE)
			) {
				result = chirp_alloc_unlink(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line,"mkfifo %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_WRITE)) goto failure;
			result = chirp_alloc_mkfifo(path);
		} else if(sscanf(line,"access %s %lld",path,&flags)==2) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,chirp_acl_from_access_flags(flags))) goto failure;
			result = chirp_alloc_access(path,flags);
		} else if(sscanf(line,"chmod %s %lld",path,&mode)==2) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_WRITE)) goto failure;
			result = chirp_alloc_chmod(path,mode);
		} else if(sscanf(line,"chown %s %lld %lld",path,&uid,&gid)==3) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_WRITE)) goto failure;
			result = 0;
		} else if(sscanf(line,"lchown %s %lld %lld",path,&uid,&gid)==3) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_WRITE)) goto failure;
			result = 0;
		} else if(sscanf(line,"truncate %s %lld",path,&length)==2) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_WRITE)) goto failure;
			result = chirp_alloc_truncate(path,length);
		} else if(sscanf(line,"rename %s %s",path,newpath)==2) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_path_fix(newpath)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_READ|CHIRP_ACL_DELETE)) goto failure;
			if(!chirp_acl_check(newpath,subject,CHIRP_ACL_WRITE)) goto failure;
			result = chirp_alloc_rename(path,newpath);
		} else if(sscanf(line,"link %s %s",path,newpath)==2) {
			/* Can only hard link to files on which you already have r/w perms */
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_READ|CHIRP_ACL_WRITE)) goto failure;
			if(!chirp_path_fix(newpath)) goto failure;
			if(!chirp_acl_check(newpath,subject,CHIRP_ACL_WRITE)) goto failure;
			result = chirp_alloc_link(path,newpath);
		} else if(sscanf(line,"symlink %s %s",path,newpath)==2) {
			/* Note that the link target (path) may be any arbitrary data. */
			/* Access permissions are checked when data is actually accessed. */
			if(!chirp_path_fix(newpath)) goto failure;
			if(!chirp_acl_check(newpath,subject,CHIRP_ACL_WRITE)) goto failure;
			result = chirp_alloc_symlink(path,newpath);
		} else if(sscanf(line,"setacl %s %s %s",path,newsubject,newacl)==3) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check_dir(path,subject,CHIRP_ACL_ADMIN)) goto failure;
			result = chirp_acl_set(path,newsubject,chirp_acl_text_to_flags(newacl),0);
		} else if(sscanf(line,"resetacl %s %s",path,newacl)==2) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check_dir(path,subject,CHIRP_ACL_ADMIN)) goto failure;
			result = chirp_acl_set(path,subject,chirp_acl_text_to_flags(newacl)|CHIRP_ACL_ADMIN,1);
		} else if(sscanf(line,"mkdir %s %lld",path,&mode)==2) {
			if(!chirp_path_fix(path)) goto failure;
			if(chirp_acl_check(path,subject,CHIRP_ACL_RESERVE)) {
				result = chirp_alloc_mkdir(path,mode);
				if(result==0){
					if(chirp_acl_init_reserve(path,subject)) {
						result = 0;
					} else {
						chirp_alloc_rmdir(path);
						errno = EACCES;
						goto failure;
					}
				}
			} else if(chirp_acl_check(path,subject,CHIRP_ACL_WRITE)){
				result = chirp_alloc_mkdir(path,mode);
				if(result==0){
					if(chirp_acl_init_copy(path)) {
						result = 0;
					} else {
						chirp_alloc_rmdir(path);
						errno = EACCES;
						goto failure;
					}
				}
			} else if(chirp_is_directory(path)) {
				errno = EEXIST;
				goto failure;
			} else {
				errno = EACCES;
				goto failure;
			}
		} else if(sscanf(line,"rmdir %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(chirp_acl_check(path,subject,CHIRP_ACL_DELETE) || chirp_acl_check_dir(path,subject,CHIRP_ACL_DELETE)) {
				result = chirp_alloc_rmdir(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line,"rmall %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(chirp_acl_check(path,subject,CHIRP_ACL_DELETE) || chirp_acl_check_dir(path,subject,CHIRP_ACL_DELETE)) {
				result = chirp_alloc_rmall(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line,"utime %s %lld %lld",path,&actime,&modtime)==3) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_WRITE)) goto failure;
			result = chirp_alloc_utime(path,actime,modtime);
		} else if(sscanf(line,"fstat %lld",&fd)==1) {
			result = chirp_alloc_fstat(fd,&statbuf);
			do_stat_result = 1;
		} else if(sscanf(line,"fstatfs %lld",&fd)==1) {
			result = chirp_alloc_fstatfs(fd,&statfsbuf);
			do_statfs_result = 1;
		} else if(sscanf(line,"statfs %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_LIST)) goto failure;
			result = chirp_alloc_statfs(path,&statfsbuf);
			do_statfs_result = 1;
		} else if(sscanf(line,"stat %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_LIST)) goto failure;
			result = chirp_alloc_stat(path,&statbuf);
			do_stat_result = 1; 
		} else if(sscanf(line,"lstat %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check_link(path,subject,CHIRP_ACL_LIST)) goto failure;
			result = chirp_alloc_lstat(path,&statbuf);
			do_stat_result = 1;
		} else if(sscanf(line,"lsalloc %s",path)==1) {
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check_link(path,subject,CHIRP_ACL_LIST)) goto failure;
			result = chirp_alloc_lsalloc(path,newpath,&size,&inuse);
			if(result>=0) {
				sprintf(line,"0\n%s %lld %lld\n",&newpath[strlen(chirp_root_path)+1],size,inuse);
				link_write(l,line,strlen(line),stalltime);
				do_no_result = 1;
			}
		} else if(sscanf(line,"mkalloc %s %lld %lld",path,&size,&mode)==3) {
			if(!chirp_path_fix(path)) goto failure;
			if(chirp_acl_check(path,subject,CHIRP_ACL_RESERVE)) {
				result = chirp_alloc_mkalloc(path,size,mode);
				if(result==0){
					if(chirp_acl_init_reserve(path,subject)) {
						result = 0;
					} else {
						chirp_alloc_rmdir(path);
						errno = EACCES;
						result = -1;
					}
				}
			} else if(chirp_acl_check(path,subject,CHIRP_ACL_WRITE)){
				result = chirp_alloc_mkalloc(path,size,mode);
				if(result==0){
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
		} else if(sscanf(line,"localpath %s",path)==1) {
			struct chirp_stat info;
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_LIST) && !chirp_acl_check(path,"system:localuser",CHIRP_ACL_LIST)) goto failure;
			result = chirp_alloc_stat(path,&info);
			if(result>=0) {
				sprintf(line,"%d\n",(int)strlen(path));
				link_write(l,line,strlen(line),stalltime);
				link_write(l,path,strlen(path),stalltime);
				do_no_result = 1;
			} else {
				result = -1;
			}
		} else if(sscanf(line,"audit %s",path)==1) {
			struct hash_table *table;
			struct chirp_audit *entry;
			char *key;

			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_ADMIN)) goto failure;

			table = chirp_audit(path);
			if(table) {
				sprintf(line,"%d\n",hash_table_size(table));
				link_write(l,line,strlen(line),stalltime);
				hash_table_firstkey(table);
				while(hash_table_nextkey(table,&key,(void*)&entry)) {
					sprintf(line,"%s %lld %lld %lld\n",key,entry->nfiles,entry->ndirs,entry->nbytes);
					link_write(l,line,strlen(line),stalltime);
				}
				chirp_audit_delete(table);
				result = 0;
				do_no_result = 1;
			} else {
				result = -1;
			}
		} else if(sscanf(line,"job_begin %s %s %s %s %s %[^\n]",jobcwd,infile,outfile,errfile,path,args)>=5) {
			if(!allow_execute) {
				errno = EPERM;
				goto failure;
			}

			if(!chirp_path_fix(jobcwd)) goto failure;
			if(!chirp_acl_check(jobcwd,subject,CHIRP_ACL_LIST)) goto failure;

			if(!strcmp(infile,"-")) {
				strcpy(infile,"/dev/null");
			} else {
				if(!chirp_path_fix(infile)) goto failure;
				if(!chirp_acl_check(infile,subject,CHIRP_ACL_READ)) goto failure;
			}
			if(!strcmp(outfile,"-")) {
				strcpy(outfile,"/dev/null");
			} else {
				if(!chirp_path_fix(outfile)) goto failure;
				if(!chirp_acl_check(outfile,subject,CHIRP_ACL_WRITE)) goto failure;
			}
			if(!strcmp(errfile,"-")) {
				strcpy(errfile,"/dev/null");
			} else {
				if(!chirp_path_fix(errfile)) goto failure;
				if(!chirp_acl_check(errfile,subject,CHIRP_ACL_WRITE)) goto failure;
			}

			if(path[0]!='@') {
				if(!chirp_path_fix(path)) goto failure;
				if(!chirp_acl_check(path,subject,CHIRP_ACL_EXECUTE)) goto failure;
			}

			result = chirp_job_begin(subject,jobcwd,infile,outfile,errfile,path,args);

		} else if(sscanf(line,"job_wait %lld %d",&jobid,&wait_timeout)==2) {
			result = chirp_job_wait(subject,jobid,&jobstate,time(0)+MIN(wait_timeout,max_job_wait_timeout));
			if(result>=0) {
				sprintf(line,"0\n%lld %s %s %u %d %lu %lu %lu %d\n",
					jobstate.jobid,
					jobstate.command,
					jobstate.owner,
					jobstate.state,
					jobstate.exit_code,
					jobstate.submit_time,
					jobstate.start_time,
					jobstate.stop_time,
					jobstate.pid);
				link_write(l,line,strlen(line),stalltime);
				do_no_result = 1;
			}
		} else if(sscanf(line,"job_commit %lld",&jobid)==1) {
			result = chirp_job_commit(subject,jobid);	
		} else if(sscanf(line,"job_kill %lld",&jobid)==1) {
			result = chirp_job_kill(subject,jobid);
		} else if(sscanf(line,"job_remove %lld",&jobid)==1) {
			result = chirp_job_remove(subject,jobid);
		} else if(!strcmp(line,"job_list")) {
			struct chirp_job_state *job;
			void * list = chirp_job_list_open();
			if(!list) goto failure;

			link_write(l,"0\n",2,stalltime);

			while((job=chirp_job_list_next(list))) {
				sprintf(line,"%lld %s %s %u %d %lu %lu %lu %d\n",
					job->jobid,
					job->command,
					job->owner,
					job->state,
					job->exit_code,
					job->submit_time,
					job->start_time,
					job->stop_time,
					job->pid);
				link_write(l,line,strlen(line),stalltime);
			}

			link_write(l,"\n",1,stalltime);
			chirp_job_list_close(list);
			do_no_result = 1;

		} else if(sscanf(line,"md5 %s",path)==1) {
			dataout = xxmalloc(16);
			if(!chirp_path_fix(path)) goto failure;
			if(!chirp_acl_check(path,subject,CHIRP_ACL_READ)) goto failure;			
			if(chirp_alloc_md5(path,(unsigned char*)dataout)) {
				result = dataoutlength = 16;
			} else {
				result = errno_to_chirp(errno);
			}	
		} else {
			result = -1;
			errno = ENOSYS;
		}

		if(do_no_result) {
			/* nothing */
		} else if(result<0) {
		failure:
			result = errno_to_chirp(errno);
			sprintf(line,"%lld\n",result);
		} else if(do_stat_result) {
			sprintf(line,"%lld\n%s\n",(long long) result,chirp_stat_string(&statbuf));
		} else if(do_statfs_result) {
			sprintf(line,"%lld\n%s\n",(long long)result,chirp_statfs_string(&statfsbuf));
		} else if(do_getdir_result) {
			sprintf(line,"\n");
		} else {
			sprintf(line,"%lld\n",result);
		}

		debug(D_CHIRP,"= %s",line);
		if(!do_no_result) {
			length = strlen(line);
			actual = link_write(l,line,length,stalltime);
			if(actual!=length) break;
		}

		if(dataout) {
			actual = link_write(l,dataout,dataoutlength,stalltime);
			if(actual!=dataoutlength) break;
			free(dataout);
			dataout = 0;
		}

	}
}

static int errno_to_chirp( int e )
{
	switch(e) {
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
	default:
		debug(D_CHIRP,"zoiks, I don't know how to transform error %d (%s)\n",errno,strerror(errno));
		return CHIRP_ERROR_UNKNOWN;
	}
}
