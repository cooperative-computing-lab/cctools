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
#include "create_dir.h"

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <math.h>
#include <signal.h>
#include <dirent.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/signal.h>

#define WQ_MASTER "wq_master"
#define WQ_MASTER_PROJ_MAX 256

// Maximum time to wait before aborting if there is no connection to the master.
static int idle_timeout=900;

// Maxium time to wait before switching to another master.
static int master_timeout=60;

// Maximum time to wait when actively communicating with the master.
static int active_timeout=3600;

// Flag gets set on receipt of a terminal signal.
static int abort_flag = 0;

// Catalog mode control variables
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
	
	debug(D_WQ, "All available Masters:\n");
	list_first_item(ml);
	while((m = (struct wq_master *)list_next_item(ml)) != NULL) {
		debug(D_WQ, "Master %d:\n", ++count);
		debug(D_WQ, "addr:\t%s\n", m->addr);
		debug(D_WQ, "port:\t%d\n", m->port);
		debug(D_WQ, "project:\t%s\n", m->proj);
		debug(D_WQ, "priority:\t%d\n", m->priority);
		debug(D_WQ, "\n");
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
	debug(D_WQ, "Master at %s:%d is not receiving more workers.\nWon't connect to this master in %d seconds.", m->addr, m->port, lifetime);
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
			debug(D_WQ, "Talking to the Master at:\n");
			debug(D_WQ, "addr:\t%s\n", m->addr);
			debug(D_WQ, "port:\t%d\n", m->port);
			debug(D_WQ, "project:\t%s\n", m->proj);
			debug(D_WQ, "priority:\t%d\n", m->priority);
			debug(D_WQ, "\n");

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

/**
 * Stream file/directory contents for the rget protocol.
 * Format:
 * 		for a directory: a new line in the format of "dir $DIR_NAME 0"
 * 		for a file: a new line in the format of "file $FILE_NAME $FILE_LENGTH"
 * 					then file contents.
 * 		string "end" at the end of the stream (on a new line).
 *
 * Example:
 * Assume we have the following directory structure:
 * mydir
 * 		-- 1.txt
 * 		-- 2.txt
 * 		-- mysubdir
 * 			-- a.txt
 * 			-- b.txt
 * 		-- z.jpg
 *
 * The stream contents would be:
 *
 * dir mydir 0
 * file 1.txt $file_len
 * $$ FILE 1.txt's CONTENTS $$
 * file 2.txt $file_len
 * $$ FILE 2.txt's CONTENTS $$
 * dir mysubdir 0
 * file mysubdir/a.txt $file_len
 * $$ FILE mysubdir/a.txt's CONTENTS $$
 * file mysubdir/b.txt $file_len
 * $$ FILE mysubdir/b.txt's CONTENTS $$
 * file z.jpg $file_len
 * $$ FILE z.jpg's CONTENTS $$
 * end
 *
 */
int stream_output_item(struct link *master, const char *filename) {
	DIR *dir;
	struct dirent *dent;
	char dentline[WORK_QUEUE_LINE_MAX];
	struct stat info;
	char line[WORK_QUEUE_LINE_MAX];
	INT64_T actual, length;
	int fd;

	if(stat(filename, &info) != 0) {
		fprintf(stderr,"Output item %s was not created (%s).\n",filename, strerror(errno));
		sprintf(line,"item %s %lld\n", filename, (INT64_T)-1);
		link_write(master,line,strlen(line),time(0)+active_timeout);
		return 0;
	}

	if(S_ISDIR(info.st_mode)) {
		// stream a directory
		sprintf(line,"dir %s %lld\n", filename, (INT64_T)0);
		link_write(master,line,strlen(line),time(0)+active_timeout);

		dir = opendir(filename);
		if(!dir) {
			fprintf(stderr,"Could not open directory %s. (%s)\n",filename, strerror(errno));
			return 0;
		}

		while((dent = readdir(dir))) {
			if(!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, "..")) continue;
			sprintf(dentline, "%s/%s", filename, dent->d_name);
			stream_output_item(master, dentline);
		}

		closedir(dir);
	} else {
		// stream a file
		fd = open(filename,O_RDONLY,0);
		if(fd>=0) {
			length = (INT64_T)info.st_size;
			sprintf(line,"file %s %lld\n", filename, length);
			link_write(master,line,strlen(line),time(0)+active_timeout);
			actual = link_stream_from_fd(master,fd,length,time(0)+active_timeout);
			close(fd);
			if(actual!=length) {
				debug(D_WQ,"Sending back output file - %s failed: bytes to send = %lld and bytes actually sent = %lld.\nEntering recovery process now ...\n", filename, length, actual);
				return 0;
			}
		} else {
			fprintf(stderr,"Could not open output file %s. (%s)\n",filename, strerror(errno));
			return 0;
		}
	}

	return 1;
}

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
	printf(" -N <name>      Enable auto master selection mode and use this preferred master name.\n");
	printf(" -S             Enable auto master selection mode and run as a non-exclusive shared worker.\n");
	printf(" -t <time>      Abort after this amount of idle time. (default=%ds)\n",idle_timeout);
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

	while((c = getopt(argc, argv, "d:t:o:N:s:Sw:vih")) != (char) -1) {
		switch (c) {
		case 's':
			port = parse_catalog_server_description(optarg, &catalog_server_host, &catalog_server_port);
			if(!port) {
				fprintf(stderr,"The provided catalog server is invalid. The format of the '-s' option should be '-s HOSTNAME:PORT'.\n");
				exit(1);
			}
			break;
		case 'S':
			auto_worker = 1;	
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
			auto_worker = 1;	
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
		int result, mode, fd;
		INT64_T length;
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
			debug(D_WQ,"%s",line);
			if(sscanf(line,"work %lld",&length)) {
				buffer = malloc(length+10);
				link_read(master,buffer,length,time(0)+active_timeout);
				buffer[length] = 0;
				strcat(buffer," 2>&1");
				debug(D_WQ,"%s",buffer);
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
				sprintf(line,"result %d %lld\n",result,length);
				debug(D_WQ,"%s",line);
				link_write(master,line,strlen(line),time(0)+active_timeout);
				link_write(master,buffer,length,time(0)+active_timeout);
				if(buffer) free(buffer);
			} else if(sscanf(line,"put %s %lld %o",filename,&length,&mode)==3) {
				mode = mode | 0600;
				char *cur_pos, *tmp_pos;

				cur_pos = filename;
				
				if (!strncmp(cur_pos, "./", 2)){
					cur_pos += 2;
				}

				tmp_pos = strrchr(cur_pos, '/');
				if(tmp_pos) {
					*tmp_pos = '\0';
					if(!create_dir(cur_pos, mode | 0700)) {
						debug(D_WQ,"Could not create directory - %s (%s)\n", cur_pos, strerror(errno));
						goto recover;
					}
					*tmp_pos = '/';
				}

				fd = open(filename,O_WRONLY|O_CREAT|O_TRUNC,mode);
				if(fd<0) goto recover;
				INT64_T actual = link_stream_to_fd(master,fd,length,time(0)+active_timeout);
				close(fd);
				if(actual!=length) goto recover;
			} else if(sscanf(line, "unlink %s", path) == 1) {
				result = remove(path);
				if(result != 0) { // 0 - succeeded; otherwise, failed
					fprintf(stderr,"Could not remove file: %s.(%s)\n", path, strerror(errno));
					goto recover;
				}
			} else if(sscanf(line, "mkdir %s %o", filename, &mode)==2) {
				if(!create_dir(filename, mode | 0700)) {
					debug(D_WQ,"Could not create directory - %s (%s)\n", filename, strerror(errno));
					goto recover;
				}
			} else if(sscanf(line, "rget %s",filename)==1) {
				if(!stream_output_item(master, filename)) {
					fprintf(stderr,"Failed to stream output item %s back to the master.\n",filename);
					goto recover;
				}
				sprintf(line,"end\n");
				link_write(master,line,strlen(line),time(0)+active_timeout);
			} else if(sscanf(line, "get %s",filename)==1) { // for backward compatibility
				struct stat info;
				if(stat(filename, &info) != 0) {
					fprintf(stderr,"Output file %s was not created. (%s)\n",filename, strerror(errno));
					goto recover;
				}

				// send back a single file
				fd = open(filename,O_RDONLY,0);
				if(fd>=0) {
					length = (INT64_T)info.st_size;
					sprintf(line,"%lld\n",length);
					link_write(master,line,strlen(line),time(0)+active_timeout);
					INT64_T actual = link_stream_from_fd(master,fd,length,time(0)+active_timeout);
					close(fd);
					if(actual!=length) {
						debug(D_WQ,"Sending back output file - %s failed: bytes to send = %lld and bytes actually sent = %lld.\nEntering recovery process now ...\n", filename, length, actual);
						goto recover;
					}
				} else {
					fprintf(stderr,"Could not open output file %s. (%s)\n",filename, strerror(errno));
					goto recover;
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
