
/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <string.h>
#include <sys/wait.h>

#include "cctools.h"
#include "list.h"
#include "itable.h"
#include "debug.h"
#include "batch_job.h"
#include "bitmap.h"
#include "load_average.h"
#include "macros.h"
#include "timestamp.h"

#define PATH_MAX 256

#define WAVEFRONT_TASK_STATE_COMPLETE   MAKE_RGBA(0,0,255,0)
#define WAVEFRONT_TASK_STATE_RUNNING    MAKE_RGBA(0,255,0,0)
#define WAVEFRONT_TASK_STATE_READY      MAKE_RGBA(255,255,0,0)
#define WAVEFRONT_TASK_STATE_NOTREADY   MAKE_RGBA(255,0,0,0)

#define WAVEFRONT_MODE_AUTO 0
#define WAVEFRONT_MODE_MULTICORE 1
#define WAVEFRONT_MODE_DISTRIBUTED 2

static int wavefront_mode = WAVEFRONT_MODE_MULTICORE;

const char * function = "unknown";
static int max_jobs_running = 1;
static const char * progress_bitmap_file = 0;
static int progress_bitmap_interval = 5;
static FILE * progress_log_file = 0;
static int abort_mode = 0;
static int block_size = 1;
static int batch_system_type = BATCH_QUEUE_TYPE_CONDOR;
static int verify_mode = 0;
static struct batch_queue * batch_q = 0;

static int manual_max_jobs_running = 0;
static int manual_block_size = 0;

static int xstart = 1;
static int ystart = 1;
static int xsize = 1;
static int ysize = 1;

static int total_dispatch_time = 0;
static int total_execute_time = 0;
static int total_jobs_complete = 0;
static int total_cells_complete = 0;
static int total_cells = 0;

static double average_dispatch_time = 30.0;
static double average_task_time = 1.0;

struct wavefront_task {
       int x;
       int y;
       int width;
       int height;
};

struct wavefront_task * wavefront_task_create( int x, int y, int w, int h )
{
	struct wavefront_task *n = malloc(sizeof(*n));
	n->x = x;
	n->y = y;
	n->width = w;
	n->height = h;
	return n;
}

void wavefront_task_delete( struct wavefront_task *n )
{
	free(n);
}

void wavefront_task_initialize( struct bitmap *b, struct list * list )
{
	int i, j;

	bitmap_reset(b,WAVEFRONT_TASK_STATE_NOTREADY);

	for(i=0;i<=xsize;i++) {
		bitmap_set(b,i,0,WAVEFRONT_TASK_STATE_COMPLETE);
	}

	for(j=0;j<=ysize;j++) {
		bitmap_set(b,0,j,WAVEFRONT_TASK_STATE_COMPLETE);
	}

	list_push_head(list,wavefront_task_create(xstart,ystart,block_size,block_size));
}

int wavefront_task_submit_recursive( struct wavefront_task *n )
{
	int i,j;

	char extra_input_files[PATH_MAX*100];
	char extra_output_files[PATH_MAX];
	char command[PATH_MAX];
	char filename[PATH_MAX];

	sprintf(command,"./wavefront -M -X %d -Y %d ./%s %d %d >output.%d.%d 2>&1",n->x,n->y,function,n->width,n->height,n->x,n->y);
	sprintf(extra_output_files,"output.%d.%d",n->x,n->y);
	sprintf(extra_input_files,"wavefront,%s",function);

	for(i=-1;i<n->width;i++) {
		strcat(extra_input_files,",");
		sprintf(filename,"R.%d.%d",n->x+i,n->y-1);
		strcat(extra_input_files,filename);
	}

	for(j=0;j<n->height;j++) {
		strcat(extra_input_files,",");
		sprintf(filename,"R.%d.%d",n->x-1,n->y+j);
		strcat(extra_input_files,filename);
	}

	return batch_job_submit_simple(batch_q,command,extra_input_files,extra_output_files);
}

int wavefront_task_submit_single( struct wavefront_task *n )
{
	char command[PATH_MAX];
	char leftfile[PATH_MAX];
	char bottomfile[PATH_MAX];
	char diagfile[PATH_MAX];
	char extra_input_files[PATH_MAX*4];

	sprintf(leftfile,"R.%d.%d",n->x-1,n->y);
	sprintf(bottomfile,"R.%d.%d",n->x,n->y-1);
	sprintf(diagfile,"R.%d.%d",n->x-1,n->y-1);

	sprintf(extra_input_files,"%s,%s,%s,%s",function,leftfile,bottomfile,diagfile);

	sprintf(command,"./%s %s %s %s >R.%d.%d",function,leftfile,bottomfile,diagfile,n->x,n->y);

	return batch_job_submit_simple(batch_q,command,extra_input_files,0);
}

int wavefront_task_submit( struct wavefront_task *n )
{
	if(n->width==1 && n->height==1) {
		return wavefront_task_submit_single(n);
	} else {
		return wavefront_task_submit_recursive(n);
	}
}


void wavefront_task_mark_range( struct wavefront_task *t, struct bitmap *b, int state )
{
	int i,j;

	for(i=0;i<t->width;i++) {
		for(j=0;j<t->height;j++) {
			bitmap_set(b,t->x+i-xstart+1,t->y+j-ystart+1,state);
		}
	}
}

void wavefront_task_consider( struct bitmap *b, struct list *list, int x, int y )
{
	int i,j;

	for(i=0;(i<block_size) && (x+i-xstart+1)<=xsize;i++) {
		if(bitmap_get(b,x+i-xstart+1,y-ystart)!=WAVEFRONT_TASK_STATE_COMPLETE) break;
	}

	for(j=0;(j<block_size) && (y+j-ystart+1)<=ysize;j++) {
		if(bitmap_get(b,x-xstart,y+j-ystart+1)!=WAVEFRONT_TASK_STATE_COMPLETE) break;
	}

	if(i==0 || j==0) return;

	struct wavefront_task *t = wavefront_task_create(x,y,i,j);
	wavefront_task_mark_range(t,b,WAVEFRONT_TASK_STATE_READY);
	list_push_head(list,t);
}

void wavefront_task_complete( struct bitmap *b, struct list *list, struct wavefront_task *t )
{
	wavefront_task_mark_range(t,b,WAVEFRONT_TASK_STATE_COMPLETE);
	wavefront_task_consider(b,list,t->x+t->width,t->y);
	wavefront_task_consider(b,list,t->x,t->y+t->height);
	wavefront_task_delete(t);
}

static double wavefront_multicore_model( int size, int cpus, double tasktime )
{
	int slices = 2*size-1;

	double runtime=0;
	int slicesize = 0;
	int i;

	for(i=0;i<slices;i++) {

		if(i<size) {
			slicesize = i+1;
		} else {
			slicesize = 2*size-i-1;
		}

		while(slicesize>cpus) {
			slicesize -= cpus;
			runtime += tasktime;
		}

		if(slicesize>0) {
			runtime += tasktime;
		}
	}

	return runtime;		
}

static double wavefront_distributed_model( int size, int nodes, int cpus_per_node, double tasktime, int blocksize, double dispatchtime )
{
	double blocktime = wavefront_multicore_model(blocksize,cpus_per_node,tasktime);
	double runtime = wavefront_multicore_model( size/blocksize,nodes,blocktime+dispatchtime);
	debug(D_DEBUG,"model: runtime=%.02lf for size=%d nodes=%d cpus=%d tasktime=%.02lf blocksize=%d dispatchtime=%.02lf",runtime,size,nodes,cpus_per_node,tasktime,blocksize,dispatchtime);
	return runtime;
}

static int find_best_block_size( int size, int nodes, int cpus_per_node, double task_time, double dispatch_time )
{
	double t=0;
	double lasttime=10000000;
	int b;

	for(b=1;b<(xsize/4);b++) {
		t = wavefront_distributed_model(size,nodes,cpus_per_node,task_time,b,dispatch_time);
		if(t>lasttime) {
			b--;
			break;
		}

		lasttime = t;
	}

	return b;
}

void save_status( struct bitmap *b, struct list *ready_list, struct itable *running_table )
{
	static time_t last_saved = 0;
	static time_t start_time = 0;

	time_t current = time(0);
	if(!start_time) start_time = current;

	if(progress_bitmap_file) {
		if((current-last_saved) >= progress_bitmap_interval) {
			bitmap_save_bmp(b,progress_bitmap_file);
		}
	}

	fprintf(progress_log_file,
		"%.2lf %% %d s %d %d %d %.02lf %.02lf\n",
		100.0*total_cells_complete/total_cells,
		(int)(current-start_time),
		list_size(ready_list),
		itable_size(running_table),
		total_cells_complete,
		average_dispatch_time,
		average_task_time);
	fflush(0);
}

static int check_configuration( const char *function, int xsize, int ysize )
{
	char path[PATH_MAX];
	int i;

	printf("Checking for presence of function %s...\n",function);

	if(access(function,R_OK|X_OK)!=0) {
		printf("Error: Cannot access %s: %s\n",function,strerror(errno));
		printf("You must provide an executable program named %s\n",function);
		return 0;
	}

	printf("Checking for initial data files...\n");

	for(i=0;i<=xsize;i++) {
		sprintf(path,"R.%d.%d",xstart+i-1,ystart-1);
		if(access(path,R_OK)!=0) {
			printf("Cannot access initial file %s: %s\n",path,strerror(errno));
			return 0;
		}
	}

	for(i=0;i<=ysize;i++) {
		sprintf(path,"R.%d.%d",xstart-1,ystart+i-1);
		if(access(path,R_OK)!=0) {
			printf("Cannot access initial file %s: %s\n",path,strerror(errno));
			return 0;
		}
	}  

	return 1;
}

static double measure_task_time()
{
	struct wavefront_task *t = wavefront_task_create(1,1,1,1);
	struct batch_job_info info;
	batch_job_id_t jobid;
	int test_jobs_complete = 0;

	batch_q = batch_queue_create(BATCH_QUEUE_TYPE_LOCAL);

	timestamp_t start = timestamp_get();
	timestamp_t stop;

	printf("Measuring wavefront_task execution time...\n");

	do {
		jobid = wavefront_task_submit_single(t);
		if(jobid<0) {
			fprintf(stderr,"wavefront: couldn't create a local process: %s\n",strerror(errno));
			exit(1);
		}

		jobid = batch_job_wait(batch_q,&info);
		if(jobid<0) {
			fprintf(stderr,"wavefront: couldn't wait for process %d: %s\n",jobid,strerror(errno));
			exit(1);
		}

		if(!info.exited_normally || info.exit_code!=0) {
			fprintf(stderr,"wavefront: %s exited with an error. See files R.1.1 and E.1.1 for details.\n",function);
			exit(1);
		}

		test_jobs_complete++;
		stop = timestamp_get();

	} while((stop-start)<5000000);

	double task_time = (stop-start)/test_jobs_complete/1000000.0;

	printf("Average execution time is %0.02lf\n",task_time);

	return task_time;
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options] <command> <xsize> <ysize>\n", cmd);
	printf("where options are:\n");
	printf(" -n <njobs>     Manually set the number of process to run at once.\n");
	printf(" -b <size>      Manually set the block size for batch mode.\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -l <file>      Save progress log to this file.\n");
	printf(" -i <file.bmp>  Save progress image to this file.\n");
	printf(" -t <secs>      Interval between image writes, in seconds. (default=%d)\n",progress_bitmap_interval);
	printf(" -A             Automatically choose between multicore and batch mode.\n");
	printf(" -M             Run the whole problem locally in multicore mode. (default)\n");
	printf(" -D             Run the whole problem in distributed mode.\n");
	printf(" -T <type>      Type of batch system: %s\n",batch_queue_type_string());
	printf(" -V             Verify mode: check the configuration and then exit.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

int main( int argc, char *argv[] )
{
	char c;

	const char *progname = "wavefront";

	debug_config(progname);

	progress_log_file = stdout;

	while((c=getopt(argc,argv,"n:b:d:o:l:i:t:qAMDT:VX:Y:vh"))!=(char)-1) {
		switch(c) {
			case 'n':
				manual_max_jobs_running = atoi(optarg);
				break;
			case 'b':
				manual_block_size = atoi(optarg);
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'i':
				progress_bitmap_file = optarg;
				break;
			case 't':
				progress_bitmap_interval = atoi(optarg);
				break;
			case 'l':
				progress_log_file = fopen(optarg,"w");
				if(!progress_log_file) {
					fprintf(stderr,"couldn't open %s: %s\n",optarg,strerror(errno));
					return 1;
				}
				break;
			case 'A':
				wavefront_mode = WAVEFRONT_MODE_AUTO;
				break;
			case 'M':
				wavefront_mode = WAVEFRONT_MODE_MULTICORE;
				break;
			case 'D':
				wavefront_mode = WAVEFRONT_MODE_DISTRIBUTED;
				break;
			case 'T':
				batch_system_type = batch_queue_type_from_string(optarg);
				if(batch_system_type==BATCH_QUEUE_TYPE_UNKNOWN) {
					fprintf(stderr,"unknown batch system type: %s\n",optarg);
					exit(1);
				}
				break;
			case 'V':
				verify_mode = 1;
				break;
			case 'X':
				xstart = atoi(optarg);
				break;
			case 'Y':
				ystart = atoi(optarg);
				break;
			case 'v':
				cctools_version_print(stdout, progname);
				exit(0);
				break;
			case 'h':
				show_help(progname);
				exit(0);
				break;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if( (argc-optind<3) ) {
		show_help(progname);
		exit(1);
	}

	function = argv[optind];
	xsize=atoi(argv[optind+1]);
	ysize=atoi(argv[optind+2]);
	total_cells = xsize*ysize;

	if(!verify_mode && !check_configuration(function,xsize,ysize)) exit(1);

	int ncpus = load_average_get_cpus();

	if(wavefront_mode!=WAVEFRONT_MODE_MULTICORE) {
		double task_time = measure_task_time();
		printf("Each function takes %.02lfs to run.\n",task_time);

		block_size = find_best_block_size(xsize,1000,2,task_time,average_dispatch_time);
		double distributed_time = wavefront_distributed_model(xsize,1000,2,task_time,block_size,average_dispatch_time);
		double multicore_time = wavefront_multicore_model(xsize,ncpus,task_time);
		double ideal_multicore_time = wavefront_multicore_model(xsize,xsize,task_time);
		double sequential_time = wavefront_multicore_model(xsize,1,task_time);

		printf("---------------------------------\n");
		printf("This workload would take:\n");
		printf("%.02lfs sequentially\n",sequential_time);
		printf("%.02lfs on this %d-core machine\n",multicore_time,ncpus);
		printf("%.02lfs on a %d-core machine\n",ideal_multicore_time,xsize);
		printf("%.02lfs on a 1000-node distributed system with block size %d\n",distributed_time,block_size);
		printf("---------------------------------\n");

		if(wavefront_mode==WAVEFRONT_MODE_AUTO) {
			if(multicore_time < distributed_time*2) {
				wavefront_mode = WAVEFRONT_MODE_MULTICORE;
			} else {
				wavefront_mode = WAVEFRONT_MODE_DISTRIBUTED;
			}
		}
	}

	if(wavefront_mode==WAVEFRONT_MODE_MULTICORE) {
		batch_system_type = BATCH_QUEUE_TYPE_LOCAL;
		max_jobs_running = ncpus;
	} else {
		max_jobs_running = 1000;
	}

	if(manual_block_size!=0) {
		block_size = manual_block_size;
	}

	if(manual_max_jobs_running!=0) {
		max_jobs_running = manual_max_jobs_running;
	}

	if(wavefront_mode==WAVEFRONT_MODE_MULTICORE) {
		printf("Running in multicore mode with %d CPUs.\n",max_jobs_running);
	} else {
		printf("Running in distributed mode with block size %d on up to %d CPUs\n",block_size,max_jobs_running);
	}

	batch_q = batch_queue_create(batch_system_type);

	if(verify_mode) exit(0);

	struct bitmap * b = bitmap_create(xsize+1,ysize+1);
	struct list *ready_list = list_create();
	struct itable *running_table = itable_create(0);

	struct batch_job_info info;
	UINT64_T jobid;
	struct wavefront_task *task;

	wavefront_task_initialize(b,ready_list);

	printf("Starting workload...\n");

	fprintf(progress_log_file,"# elapsed time : waiting jobs / running jobs / cells complete (percent complete)\n");

	while(1) {

		if(abort_mode) {
			while((task=list_pop_tail(ready_list))) {
				wavefront_task_delete(task);
			}

			itable_firstkey(running_table);
			while(itable_nextkey(running_table,&jobid,(void**)&task)) {
				batch_job_remove(batch_q,jobid);
			}
		}

		if(list_size(ready_list)==0 && itable_size(running_table)==0) break;

		while(1) {
			if(itable_size(running_table)>=max_jobs_running) break;

			task = list_pop_tail(ready_list);
			if(!task) break;
			
			jobid = wavefront_task_submit(task);
			if(jobid>0) {
				itable_insert(running_table,jobid,task);
				wavefront_task_mark_range(task,b,WAVEFRONT_TASK_STATE_RUNNING);
			} else {
				abort();
				sleep(1);
				list_push_head(ready_list,task);
			}
		}


		save_status(b,ready_list,running_table);

		jobid = batch_job_wait(batch_q,&info);
		if(jobid>0) {
			task = itable_remove(running_table,jobid);
			if(task) {
				if(info.exited_normally && info.exit_code==0) {
					total_dispatch_time += info.started-info.submitted;
					total_execute_time += MAX(info.finished-info.started,1);
					total_cells_complete+=task->width*task->height;
					total_jobs_complete++;

					average_dispatch_time = 1.0*total_dispatch_time / total_jobs_complete;
					average_task_time = 1.0*total_execute_time / total_cells_complete;

					wavefront_task_complete(b,ready_list,task);
				} else {
					printf("job %llu failed, aborting this workload\n",jobid);
					abort_mode = 1;
				}
			}
		}
	}

	save_status(b,ready_list,running_table);

	if(abort_mode) {
		printf("Workload was aborted.\n");
	} else {
		printf("Workload complete.\n");
	}

	return 0;
}
