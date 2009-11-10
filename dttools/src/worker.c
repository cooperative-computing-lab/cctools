/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"

#include "copy_stream.h"
#include "memory_info.h"
#include "disk_info.h"
#include "link.h"
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

// Maximum time to wait before aborting if there is no connection to the master.
static int idle_timeout=900;

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
	printf(" -d <subsystem> Enable debugging for this subsystem\n");
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

	while((c = getopt(argc, argv, "d:t:o:w:vi")) != (char) -1) {
		switch (c) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 't':
			idle_timeout = string_time_parse(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
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

	if((argc-optind)!=2) {
		show_help(argv[0]);
		return 1;
	}

	host = argv[optind];
	port = atoi(argv[optind+1]);

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

	if(!domain_name_cache_lookup(host,addr)) {
		printf("couldn't lookup address of host %s\n",host);
		return 1;
	}

	domain_name_cache_guess(hostname);

	time_t idle_stoptime = time(0) + idle_timeout;

	while(!abort_flag) {
		char line[WORK_QUEUE_LINE_MAX];
		int result, length, mode, fd;
		char filename[WORK_QUEUE_LINE_MAX];
		char *buffer;
		FILE *stream;

		if(time(0)>idle_stoptime) {
			printf("worker: gave up after waiting for %ds to connect to the master.\n",idle_timeout);
			break;
		}

		if(!master) {
			master = link_connect(addr,port,idle_stoptime);
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
				buffer = malloc(length+1);
				link_read(master,buffer,length,time(0)+active_timeout);
				buffer[length] = 0;
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
			sleep(5);
		}
	}

	char deletecmd[WORK_QUEUE_LINE_MAX];
	printf("worker: cleaning up %s\n",tempdir);
	sprintf(deletecmd,"rm -rf %s",tempdir);
	system(deletecmd);

	return 0;
}
