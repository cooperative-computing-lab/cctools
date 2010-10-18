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

#include "debug.h"
#include "work_queue.h"
#include "envtools.h"
#include "text_list.h"
#include "hash_table.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "macros.h"
#include "envtools.h"
#include "fast_popen.h"
#include "list.h"

#include "allpairs_compare.h"

#define ALLPAIRS_LINE_MAX 4096

static const char *progname = "allpairs_master";
static char allpairs_multicore_program[ALLPAIRS_LINE_MAX] = "allpairs_multicore";
static char allpairs_compare_program[ALLPAIRS_LINE_MAX];

static double compare_program_time = 0.0;
static const char * extra_arguments = "";
static int use_external_program = 0;
static struct list *extra_files_list = 0;

static int xcurrent = 0;
static int ycurrent = 0;
static int xblock = 0;
static int yblock = 0;
static int xstop = 0;
static int ystop = 0;

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Usage: %s [options] <set A> <set B> <compare function>\n", cmd);
	printf("The most common options are:\n");
	printf(" -p <port>	The port that the master will be listening on.\n");
	printf(" -e <args>      Extra arguments to pass to the comparison function.\n");
	printf(" -f <file>      Extra input file needed by the comparison function.  (may be given multiple times)\n");
	printf(" -t <seconds>   Estimated time to run one comparison.  (default chosen at runtime)\n");
	printf(" -x <items>	Width of one work unit, in items to compare.  (default chosen at runtime)\n");
	printf(" -y <items>	Height of one work unit, in items to compare.  (default chosen at runtime)\n");
	printf(" -N <project>   Report the master information to a catalog server with the project name - <project>\n");
	printf(" -E <priority>  Priority. Higher the value, higher the priority.\n");
	printf(" -d <flag>	Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -v         	Show program version.\n");
	printf(" -h         	Display this message.\n");
}

/*
Run the comparison program repeatedly until five seconds have elapsed,
in order to get a rough measurement of the execution time.
No very accurate for embedded functions. 
*/

double estimate_run_time( struct text_list *seta, struct text_list *setb )
{
	char line[ALLPAIRS_LINE_MAX];
	int loops=0;
	time_t starttime, stoptime;

	printf("%s: sampling execution time of %s...\n",progname,allpairs_compare_program);

	sprintf(line,"./%s %s %s %s",
		string_basename(allpairs_compare_program),
		extra_arguments,
		text_list_get(seta,0),
		text_list_get(setb,0)
		);

	starttime = time(0);

	do {
		FILE *file = fast_popen(line);
		if(!file) {
			fprintf(stderr,"%s: couldn't execute %s: %s\n",progname,line,strerror(errno));
			exit(1);
		}

		while(fgets(line,sizeof(line),file)) {
			printf("%s\n",line);
		}

		fast_pclose(file);

		stoptime = time(0);
		loops++;
	} while( (stoptime-starttime) < 5 );

	double t = (stoptime - starttime) / loops;

	if(t<0.01) t = 0.01;

	return  t;  
}

/*
After measuring the function run time, try to choose a
squarish work unit that will take just over one minute to complete.
*/

void estimate_block_size( struct text_list *seta, struct text_list *setb, int *xblock, int *yblock )
{
	if(compare_program_time==0) {
		if(use_external_program) {
			compare_program_time = estimate_run_time(seta,setb);
		} else {
			compare_program_time = 0.1;
		}
	}

	printf("%s: %s estimated at %.02lfs per comparison\n",progname,allpairs_compare_program,compare_program_time);

	int block_limit = 60;
	double block_time;

	*xblock = *yblock = 1;

	while(1) {
		block_time = *xblock * *yblock * compare_program_time;
		if(block_time>block_limit) break;

		if(*xblock < text_list_size(seta)) (*xblock)++;
		if(*yblock < text_list_size(setb)) (*yblock)++;

		if(*xblock==text_list_size(seta) && *yblock==text_list_size(setb)) break;
	}
}

/*
Convert a text_list object into a single string that we can
pass as a buffer to a remote task via work queue.
*/

char * text_list_string( struct text_list *t, int a, int b )
{
	static int buffer_size = 128;
	char *buffer = malloc(buffer_size);
	int buffer_pos = 0;
	int i;
		
	for(i=a;i<b;i++) {
		const char *str = text_list_get(t,i);
		if(!str) break;
		str = string_basename(str);
		while((strlen(str) + buffer_pos + 3)>= buffer_size) {
			buffer_size *= 2;
			buffer = realloc(buffer,buffer_size);
		}
		buffer_pos += sprintf(&buffer[buffer_pos],"%s\n",str);
	}

	buffer[buffer_pos] = 0;

	return buffer;
}

/*
Create the next task in order to be submitted to the work queue.
Basically, bump the current position in the results matrix by
xblock, yblock, and then construct a task with a list of files
on each axis, and attach the necessary files.
*/

struct work_queue_task * task_create( struct text_list *seta, struct text_list *setb )
{
	int x,y;
	char *buf, *name;

	if(xcurrent>=xstop) {
		xcurrent=0;
		ycurrent+=yblock;
	}

	if(ycurrent>=ystop) return 0;

	char cmd[ALLPAIRS_LINE_MAX];
	sprintf(cmd,"%s -e \"%s\" A B %s%s",allpairs_multicore_program,extra_arguments,use_external_program ? "./" : "",string_basename(allpairs_compare_program));
	struct work_queue_task *task = work_queue_task_create(cmd);

	if(use_external_program) {
		work_queue_task_specify_file(task,allpairs_compare_program,string_basename(allpairs_compare_program),WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);
	}

	work_queue_task_specify_file(task,allpairs_multicore_program,string_basename(allpairs_multicore_program),WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);

	const char *f;
	list_first_item(extra_files_list);
	while((f = list_next_item(extra_files_list))) {
		work_queue_task_specify_file(task,f,string_basename(f),WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);
	}

	buf = text_list_string(seta,xcurrent,xcurrent+xblock);
	work_queue_task_specify_buffer(task,buf,strlen(buf),"A",WORK_QUEUE_NOCACHE);
	free(buf);

	buf = text_list_string(setb,ycurrent,ycurrent+yblock);
	work_queue_task_specify_buffer(task,buf,strlen(buf),"B",WORK_QUEUE_NOCACHE);
	free(buf);
	
	for(x=xcurrent;x<(xcurrent+xblock);x++) {
		name = text_list_get(seta,x);
		if(!name) break;
		work_queue_task_specify_file(task,name,string_basename(name),WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);
	}

	for(y=ycurrent;y<(ycurrent+yblock);y++) {
		name = text_list_get(setb,y);
		if(!name) break;
		work_queue_task_specify_file(task,name,string_basename(name),WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);
	}

	/* advance to the next row/column */
	xcurrent += xblock;

	return task;
}

void task_complete( struct work_queue_task *t )
{
	printf("%s\n",t->output);
	work_queue_task_delete(t);
}

int main(int argc, char **argv)
{
	char c;
	struct work_queue *q;
	struct work_queue_task *task;
	int port = WORK_QUEUE_DEFAULT_PORT;

	extra_files_list = list_create();

	while((c = getopt(argc, argv, "e:f:t:x:y:p:N:E:d:vh")) != (char) -1) {
		switch (c) {
		case 'e':
			extra_arguments = optarg;
			break;
		case 'f':
			list_push_head(extra_files_list,optarg);
			break;
		case 't':
			compare_program_time = atof(optarg);
			break;
		case 'x':
			xblock = atoi(optarg);
			break;
		case 'y':
			yblock = atoi(optarg);
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'N':
			setenv("WORK_QUEUE_NAME", optarg, 1);
			break;
		case 'E':
			setenv("WORK_QUEUE_PRIORITY", optarg, 1);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'v':
			show_version(progname);
			exit(0);
			break;
		case 'h':
			show_help(progname);
			exit(0);
			break;
		default:
			show_help(progname);
			return 1;
		}
	}

	if((argc - optind) < 3) {
		show_help(progname);
		exit(1);
	}

	struct text_list *seta = text_list_load(argv[optind]);
	if(!seta) {
		fprintf(stderr,"%s: couldn't open %s: %s\n",progname,argv[optind+1],strerror(errno));
		return 1;
	}

	printf("%s: %s has %d elements\n",progname,argv[optind],text_list_size(seta));

	struct text_list *setb = text_list_load(argv[optind+1]);
	if(!setb) {
		fprintf(stderr,"%s: couldn't open %s: %s\n",progname,argv[optind+1],strerror(errno));
		return 1;
	}

	printf("%s: %s has %d elements\n",progname,argv[optind+1],text_list_size(setb));

	if (!find_executable("allpairs_multicore", "PATH", allpairs_multicore_program, ALLPAIRS_LINE_MAX)) {
		fprintf(stderr,"%s: couldn't find allpairs_multicore in path\n",progname);
		return 1;
	}

	if(allpairs_compare_function_get(argv[optind+2])) {
		strcpy(allpairs_compare_program,argv[optind+2]);
		debug(D_DEBUG,"using internal function %s",allpairs_compare_program);
		use_external_program = 0;
	} else {
		if(!find_executable(argv[optind+2],"PATH",allpairs_compare_program,sizeof(allpairs_compare_program))) {
			fprintf(stderr,"%s: %s is neither an executable nor an internal comparison function.\n",progname,allpairs_compare_program);
			return 1;
		}
		debug(D_DEBUG,"using comparison executable %s",allpairs_compare_program);
		use_external_program = 1;
	}

	if(!xblock || !yblock) {
		estimate_block_size(seta,setb,&xblock,&yblock);
	}

	printf("%s: using block size of %dx%d\n",progname,xblock,yblock);

	q = work_queue_create(port);
	if(!q) {
		fprintf(stderr,"%s: could not create work queue on port %d: %s\n",progname,port,strerror(errno));
		return 1;
	}

	printf("%s: listening for workers on port %d...\n",progname,work_queue_port(q));

	if(!xstop) xstop = text_list_size(seta);
	if(!ystop) ystop = text_list_size(setb);

	while(1) {
		while(work_queue_hungry(q)) {
			task = task_create(seta,setb);
			if(task) {
				work_queue_submit(q, task);
			} else {
				break;
			}
		}

		if(!task && work_queue_empty(q)) break;

		task = work_queue_wait(q,5);
		if(task) task_complete(task);
	}

	work_queue_delete(q);

	return 0;
}
