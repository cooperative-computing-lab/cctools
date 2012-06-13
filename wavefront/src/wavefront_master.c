
/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <signal.h>

#include "debug.h"
#include "work_queue.h"
#include "text_array.h"
#include "macros.h"

#define WAVEFRONT_LINE_MAX 1024

static const char *function = 0;
static struct text_array *array = 0;
static struct work_queue *queue = 0;
static int xsize = 0;
static int ysize = 0;
static int port = 9068;
static const char *infile;
static const char *outfile;
static FILE *logfile;
static int cells_total = 0;
static int cells_complete = 0;
static int tasks_done = 0;
static double sequential_run_time = 7.75;
static time_t start_time = 0;
static time_t last_display_time = 0;

static const time_t long_wait = 60;
static const time_t short_wait = 5; 

static int task_consider( int x, int y )
{
	char command[WAVEFRONT_LINE_MAX];
	char tag[WAVEFRONT_LINE_MAX];
	
	struct work_queue_task* t;

	if(x>=xsize) return 1;
	if(y>=ysize) return 1;

	if(text_array_get(array,x,y)) return 0;

	const char *left   = text_array_get(array,x-1,y);
	const char *bottom = text_array_get(array,x,y-1);
	const char *diag   = text_array_get(array,x-1,y-1);

	if(!left || !bottom || !diag) return 1;

	sprintf(command,"./%s %d %d xfile yfile dfile",function,x,y);
	sprintf(tag,"%d %d",x,y);
	
	t = work_queue_task_create(command);
	work_queue_task_specify_tag(t,tag);
	work_queue_task_specify_input_file(t, function, function);
	work_queue_task_specify_input_buf(t, left, strlen(left), "xfile");
	work_queue_task_specify_input_buf(t, bottom, strlen(bottom), "yfile");
	work_queue_task_specify_input_buf(t, diag, strlen(diag), "dfile");
	work_queue_submit(queue,t);

	return 1;
}

static void task_complete( int x, int y )
{
	cells_complete++;
	task_consider(x+1,y);
	task_consider(x,y+1);
}

static void task_prime()
{
	int i,j;
	for(j=0;j<ysize;j++) {
		for(i=0;i<xsize;i++) {
			if(task_consider(i,j)) break;
			if(i!=0 && j!=0) cells_complete++;
		}
	}
}


static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <command> <xsize> <ysize> <inputdata> <outputdata>\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number for queue master to listen on.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

static void display_progress( struct work_queue *q )
{
        struct work_queue_stats info;
        time_t current = time(0);
        work_queue_get_stats(queue,&info);
        if(current==start_time) current++;
        double speedup = (sequential_run_time*tasks_done)/(current-start_time);
        printf("%2.02lf%% %6d %6ds %4d %4d %4d %4d %4d %4d %.02lf\n",100.0*cells_complete/cells_total,cells_complete,(int)(time(0)-start_time),info.workers_init,info.workers_ready,info.workers_busy,info.tasks_waiting,info.tasks_running,info.tasks_complete,speedup);
        last_display_time = current;
}

int main( int argc, char *argv[] )
{
	char c;

	const char *progname = "wavefront";

	debug_config(progname);

	while((c=getopt(argc,argv,"p:Pd:o:vh"))!=(char)-1) {
		switch(c) {
			case 'p':
				port = atoi(optarg);
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'v':
				show_version(progname);
				exit(0);
				break;
			case 'h':
				show_help(progname);
				exit(0);
				break;
		}
	}

	if( (argc-optind)!=5 ) {
		show_help(progname);
		exit(1);
	}

	function = argv[optind];
	xsize=atoi(argv[optind+1]);
	ysize=atoi(argv[optind+2]);
	infile=argv[optind+3];
	outfile=argv[optind+4];

	start_time = time(0);
	last_display_time = 0;

	cells_total = xsize*ysize;
	
	xsize++;
	ysize++;

	array = text_array_create(xsize,ysize);
	if(!text_array_load(array,infile)) {
		fprintf(stderr,"couldn't load %s: %s",infile,strerror(errno));
		return 1;
	}

	int count = text_array_load(array,outfile);
	if(count>0) printf("recovered %d results from %s\n",count,outfile);
	
	logfile = fopen(outfile,"a");
	if(!logfile) {
		fprintf(stderr,"couldn't open %s for append: %s\n",outfile,strerror(errno));
		return 1;
	}

	queue = work_queue_create(port);

	task_prime();

	struct work_queue_task *t;

	while(1) {
		if(time(0)!=last_display_time) display_progress(queue);

		t = work_queue_wait(queue,WORK_QUEUE_WAITFORTASK);
		if(!t) break;
		
		if(t->return_status==0) {
			int x,y;
			if(sscanf(t->tag,"%d %d",&x,&y)==2) {
				text_array_set(array,x,y,t->output);
				task_complete(x,y);
				fprintf(logfile,"%d %d %s\n",x,y,t->output);
				fflush(logfile);
				tasks_done++;
			} else {
				fprintf(stderr,"unexpected output: %s\nfrom command: %s\non host: %s",t->output,t->command_line,t->hostaddr);
			}
		} else {
		    fprintf(stderr,"function failed return value (%i) result (%i) on host %s. output:\n%s\n",t->return_status,t->result,t->hostaddr,t->output);
		}
		work_queue_task_delete(t);
		if(work_queue_empty(queue))
		    break;
	}

	display_progress(queue);
	return 0;
}
