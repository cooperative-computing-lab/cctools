/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"

#include "catalog_query.h"
#include "catalog_server.h"
#include "datagram.h"
#include "domain_name_cache.h"
#include "nvpair.h"
#include "copy_stream.h"
#include "memory_info.h"
#include "disk_info.h"
#include "hash_cache.h"
#include "link.h"
#include "list.h"
#include "xmalloc.h"
#include "debug.h"
#include "stringtools.h"
#include "load_average.h"
#include "domain_name_cache.h"
#include "getopt.h"
#include "full_io.h"

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <math.h>
#include <signal.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/signal.h>

#define WQ_MASTER "wq_master"
#define WQ_MASTER_PROJ_MAX 256

static int auto_worker = 0;
static int exclusive_worker = 1;
static char *preference = NULL;
static const int non_preference_priority_max = 100;
static char *catalog_server_host = NULL;
static int catalog_server_port = 0;
static struct wq_master *actual_master = NULL;

struct hash_cache *bad_masters = NULL;

struct wq_master {
	char addr[LINK_ADDRESS_MAX];
	int port;
	char proj[WQ_MASTER_PROJ_MAX];
	int priority;
};

void debug_print_masters(struct list *ml) {
	struct wq_master *m;
	int count = 0;
	
	debug(D_DEBUG, "All available Masters:\n");
	list_first_item(ml);
	while((m = (struct wq_master *)list_next_item(ml)) != NULL) {
		debug(D_DEBUG, "Master %d:\n", ++count);
		debug(D_DEBUG, "addr:\t%s\n", m->addr);
		debug(D_DEBUG, "port:\t%d\n", m->port);
		debug(D_DEBUG, "project:\t%s\n", m->proj);
		debug(D_DEBUG, "priority:\t%d\n", m->priority);
		debug(D_DEBUG, "\n");
	}
}


static void make_hash_key(const char *addr, int port, char *key)
{
	sprintf(key, "%s:%d", addr, port);
}

int parse_catalog_server_description(char* server_string, char **host, int *port) {
	char *colon;

	colon = strchr(server_string, ':');

	if(!colon) {
		*host = NULL;
		*port = 0;
		return 0;
	}

	*colon = '\0';

	*host = strdup(server_string);
	*port = atoi(colon+1);
	
	return *port;
}
	
struct wq_master * parse_wq_master_nvpair(struct nvpair *nv) {
	struct wq_master *m;

	m = xxmalloc(sizeof(struct wq_master));

	strncpy(m->addr, nvpair_lookup_string(nv, "address"), LINK_ADDRESS_MAX);
	strncpy(m->proj, nvpair_lookup_string(nv, "project"), WQ_MASTER_PROJ_MAX);
	m->port = nvpair_lookup_integer(nv, "port");
	m->priority = nvpair_lookup_integer(nv, "priority");
	if(m->priority < 0) m->priority = 0; 

	return m;
}

struct wq_master * duplicate_wq_master(struct wq_master *master) {
	struct wq_master *m;

	m = xxmalloc(sizeof(struct wq_master));
	strncpy(m->addr, master->addr, LINK_ADDRESS_MAX);
	strncpy(m->proj, master->proj, WQ_MASTER_PROJ_MAX);
	m->port = master->port;
	m->priority = master->priority;
	if(m->priority < 0) m->priority = 0; 

	return m;
}

/**
 * Reasons for a master being bad:
 * 1. The master does not need more workers right now;
 * 2. The master is already shut down but its record is still in the catalog server.
 */
static void record_bad_master(struct wq_master *m) {
	char key[LINK_ADDRESS_MAX + 10]; // addr:port
	int lifetime = 10;

	if(!m) return;

	make_hash_key(m->addr, m->port, key);
	hash_cache_insert(bad_masters, key, m, lifetime);
	debug(D_DEBUG, "Master at %s:%d is not receiving more workers.\nWon't connect to this master in %d seconds.", m->addr, m->port, lifetime);
}

struct list * get_work_queue_masters(const char * catalog_host, int catalog_port) {
	struct catalog_query *q;
	struct nvpair *nv;
	struct list *ml;
	struct wq_master *m;
	time_t timeout=60, stoptime;
	char key[LINK_ADDRESS_MAX + 10]; // addr:port
	
	stoptime = time(0) + timeout;

	q = catalog_query_create(catalog_host, catalog_port, stoptime);
	if(!q) {
		fprintf(stderr,"Couldn't query catalog: %s\n",strerror(errno));
		return NULL;
	}

	ml = list_create();
	if(!ml) return NULL;

	while((nv = catalog_query_read(q, stoptime))){
		if(strcmp(nvpair_lookup_string(nv, "type"), WQ_MASTER) == 0) {
			m = parse_wq_master_nvpair(nv);	
			if(preference) {
				if(strncmp(m->proj, preference, WQ_MASTER_PROJ_MAX) == 0) {
					m->priority += non_preference_priority_max; 
				} else {
					if(exclusive_worker) continue;
					m->priority = non_preference_priority_max < m->priority ? non_preference_priority_max : m->priority;
				}
			}
			
			// exclude 'bad' masters
			make_hash_key(m->addr, m->port, key);
			if(!hash_cache_lookup(bad_masters, key)) {
				list_push_priority(ml, m, m->priority);
			}
		}
		nvpair_delete(nv);
	}

	return ml;
}

struct link * auto_link_connect(char *addr, int *port, time_t master_stoptime) {
	struct link *master=0;
	struct list *ml;
	struct wq_master *m;

	ml = get_work_queue_masters(catalog_server_host, catalog_server_port);
	debug_print_masters(ml);

	list_first_item(ml);
	while((m = (struct wq_master *)list_next_item(ml)) != NULL) {
		master = link_connect(m->addr,m->port,master_stoptime);
		if(master) {
			debug(D_DEBUG, "Talking to the Master at:\n");
			debug(D_DEBUG, "addr:\t%s\n", m->addr);
			debug(D_DEBUG, "port:\t%d\n", m->port);
			debug(D_DEBUG, "project:\t%s\n", m->proj);
			debug(D_DEBUG, "priority:\t%d\n", m->priority);
			debug(D_DEBUG, "\n");

			strncpy(addr, m->addr, LINK_ADDRESS_MAX);
			(*port) = m->port;

			if(actual_master) free(actual_master);
			actual_master = duplicate_wq_master(m);

			break;
		} else {
			record_bad_master(duplicate_wq_master(m));
		}
	}

	list_free(ml);
	list_delete(ml);

	return master;
}



// Maximum time to wait before aborting if there is no connection to the master.
static int idle_timeout=900;

// Maxium time to wait before switching to another master.
static int master_timeout=60;

// Maximum time to wait when actively communicating with the master.
static int active_timeout=3600;

// Flag gets set on receipt of a terminal signal.
static int abort_flag = 0;

static void handle_abort( int sig )
{
	abort_flag = 1;
}

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s <masterhost> <port>\n", cmd);
	printf("where options are:\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.\n");
	printf(" -a             Enable auto master selection mode.\n");
	printf(" -N <name>      Preferred master name.\n");
	printf(" -t <time>      Abort after this amount of idle time. (default=%ds)\n",idle_timeout);
	printf(" -S             Run as a shared worker. (Would work for non-preferred master when preferred master is not up.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -w <size>      Set TCP window size.\n");
	printf(" -h             Show this help screen\n");
}

int main( int argc, char *argv[] )
{
	const char *host;
	int port;
	char actual_addr[LINK_ADDRESS_MAX];
	int actual_port;
	struct link *master=0;
	char addr[LINK_ADDRESS_MAX];
	UINT64_T memory_avail, memory_total;
	UINT64_T disk_avail, disk_total;
	int ncpus;
	char c;
	char hostname[DOMAIN_NAME_MAX];
	int w;

	ncpus = load_average_get_cpus();
	memory_info_get(&memory_avail,&memory_total);
	disk_info_get(".",&disk_avail,&disk_total);
	
	debug_config(argv[0]);

	while((c = getopt(argc, argv, "ac:d:t:o:N:s:Sw:vih")) != (char) -1) {
		switch (c) {
		case 'a':
			auto_worker = 1;	
			break;
		case 'c':
			auto_worker = 1;	
			break;
		case 's':
			port = parse_catalog_server_description(optarg, &catalog_server_host, &catalog_server_port);
			if(!port) {
				fprintf(stderr,"The provided catalog server is invalid. The format of the '-s' option should be '-s HOSTNAME:PORT'.\n");
				exit(1);
			}
			break;
		case 'S':
			exclusive_worker = 0;	
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 't':
			idle_timeout = string_time_parse(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'N':
			preference = strdup(optarg);
			break;
		case 'v':
			show_version(argv[0]);
			return 0;
		case 'w':
			w = string_metric_parse(optarg);
			link_window_set(w,w);
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	if(!auto_worker) {
		if((argc-optind) != 2) {
			show_help(argv[0]);
			return 1;
		}
		host = argv[optind];
		port = atoi(argv[optind+1]);

		if(!domain_name_cache_lookup(host,addr)) {
			printf("couldn't lookup address of host %s\n",host);
			return 1;
		}
	}


	signal(SIGTERM,handle_abort);
	signal(SIGQUIT,handle_abort);
	signal(SIGINT,handle_abort);
	
	const char *workdir;

	if(getenv("_CONDOR_SCRATCH_DIR")) {
		workdir = getenv("_CONDOR_SCRATCH_DIR");
	} else {
		workdir = "/tmp";
	}

	char tempdir[WORK_QUEUE_LINE_MAX];
	sprintf(tempdir,"%s/worker-%d-%d",workdir,(int)getuid(),(int)getpid());

	printf("worker: working in %s\n",tempdir);
	mkdir(tempdir,0700);
	chdir(tempdir);

	domain_name_cache_guess(hostname);

	time_t idle_stoptime = time(0) + idle_timeout;
	time_t switch_master_time = time(0) + master_timeout;

	bad_masters = hash_cache_create(127, hash_string, (hash_cache_cleanup_t) free);

	while(!abort_flag) {
		char line[WORK_QUEUE_LINE_MAX];
		int result, length, mode, fd;
		char filename[WORK_QUEUE_LINE_MAX];
		char path[WORK_QUEUE_LINE_MAX];
		char *buffer;
		FILE *stream;

		if(time(0)>idle_stoptime) {
			if(master) {
				printf("worker: gave up after waiting %ds to receive a task.\n",idle_timeout);
			} else {
				if(auto_worker) {
					printf("worker: gave up after waiting %ds to connect to all the available masters.\n",idle_timeout);
				} else {
					printf("worker: gave up after waiting %ds to connect to %s port %d.\n",idle_timeout,host,port);
				}
			}
			break;
		}

		switch_master_time = time(0) + master_timeout;
		if(!master) {
			if(auto_worker) {
				master = auto_link_connect(actual_addr, &actual_port, switch_master_time);
			} else {
				master = link_connect(addr,port,idle_stoptime);
			}
			if(!master) {
				sleep(5);
				continue;
			}

			link_tune(master,LINK_TUNE_INTERACTIVE);
			sprintf(line,"ready %s %d %llu %llu %llu %llu\n",hostname,ncpus,memory_avail,memory_total,disk_avail,disk_total);
			link_write(master,line,strlen(line),time(0)+active_timeout);
		}

		if(link_readline(master,line,sizeof(line),time(0)+active_timeout)) {
			debug(D_DEBUG,"%s",line);
			if(sscanf(line,"work %d",&length)) {
				buffer = malloc(length+10);
				link_read(master,buffer,length,time(0)+active_timeout);
				buffer[length] = 0;
				strcat(buffer," 2>&1");
				debug(D_DEBUG,"%s",buffer);
				stream = popen(buffer,"r");
				free(buffer);
				if(stream) {
					length = copy_stream_to_buffer(stream,&buffer);
					if(length<0) length = 0;
					result = pclose(stream);
				} else {
					length = 0;
					result = -1;
					buffer = 0;
				}
				sprintf(line,"result %d %d\n",result,length);
				debug(D_DEBUG,"%s",line);
				link_write(master,line,strlen(line),time(0)+active_timeout);
				link_write(master,buffer,length,time(0)+active_timeout);
				if(buffer) free(buffer);
			} else if(sscanf(line,"put %s %d %o",filename,&length,&mode)==3) {
				mode = mode | 0600;
				fd = open(filename,O_WRONLY|O_CREAT|O_TRUNC,mode);
				if(fd<0) goto recover;
				int actual = link_stream_to_fd(master,fd,length,time(0)+active_timeout);
				close(fd);
				if(actual!=length) goto recover;
			} else if(sscanf(line, "unlink %s", path) == 1) {
				result = remove(path);
				if(result != 0) { // 0 - succeeded; otherwise, failed
					fprintf(stderr,"Could not remove file: %s.(%s)\n", path, strerror(errno));
					goto recover;
				}
			} else if(sscanf(line, "mkdir %s %o", filename, &mode)==2) {
				mode = mode | 0700;
				result = mkdir(filename, mode);
				if(result != 0) { // 0 - succeeded; otherwise, failed
					fprintf(stderr,"Could not make directory: %s.(%s)\n",filename, strerror(errno));
					goto recover;
				}
			} else if(sscanf(line,"get %s",filename)==1) {
				fd = open(filename,O_RDONLY,0);
				if(fd>=0) {
					struct stat info;
					fstat(fd,&info);
					length = info.st_size;
					sprintf(line,"%d\n",(int)length);
					link_write(master,line,strlen(line),time(0)+active_timeout);
					int actual = link_stream_from_fd(master,fd,length,time(0)+active_timeout);
					close(fd);
					if(actual!=length) goto recover;
				} else {
					sprintf(line,"-1\n");
					link_write(master,line,strlen(line),time(0)+active_timeout);
				}					
			} else if(!strcmp(line,"exit")) {
				break;
			} else {
				link_write(master,"error\n",6,time(0)+active_timeout);
			}

			idle_stoptime = time(0) + idle_timeout;

		} else {
			recover:
			link_close(master);
			master = 0;
			if(auto_worker) {
				record_bad_master(duplicate_wq_master(actual_master));
			}
			sleep(5);
		}
	}

	char deletecmd[WORK_QUEUE_LINE_MAX];
	printf("worker: cleaning up %s\n",tempdir);
	sprintf(deletecmd,"rm -rf %s",tempdir);
	system(deletecmd);
	
	return 0;
}
