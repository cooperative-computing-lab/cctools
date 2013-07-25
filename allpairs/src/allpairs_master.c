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

#include "cctools.h"
#include "debug.h"
#include "work_queue.h"
#include "envtools.h"
#include "text_list.h"
#include "hash_table.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "macros.h"
#include "envtools.h"
#include "fast_popen.h"
#include "list.h"
#include "timestamp.h"
#include "getopt_aux.h"

#include "allpairs_compare.h"

#define ALLPAIRS_LINE_MAX 4096

static const char *progname = "allpairs_master";
static char allpairs_multicore_program[ALLPAIRS_LINE_MAX] = "allpairs_multicore";
static char allpairs_compare_program[ALLPAIRS_LINE_MAX];
static char *output_filename = NULL;

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

static void show_help(const char *cmd)
{
	fprintf(stdout, "Usage: %s [options] <set A> <set B> <compare function>\n", cmd);
	fprintf(stdout, "The most common options are:\n");
	fprintf(stdout, " %-30s The port that the master will be listening on.\n", "-p,--port=<port>");
	fprintf(stdout, " %-30s Extra arguments to pass to the comparison function.\n", "-e,--extra-args=<args>");
	fprintf(stdout, " %-30s Extra input file needed by the comparison function. (may be given multiple times)\n", "-f,--input-file=<file>");
	fprintf(stdout, " %-30s Write debugging output to this file (default to standard output)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Estimated time to run one comparison. (default chosen at runtime)\n", "-t,--estimated-time=<seconds>");
	fprintf(stdout, " %-30s Width of one work unit, in items to compare. (default chosen at runtime)\n", "-x,--width=<items>");
	fprintf(stdout, " %-30s Height of one work unit, in items to compare. (default chosen at runtime)\n", "-y,--height=<items>");
	fprintf(stdout, " %-30s Set the project name to <project>\n", "-N,--project-name=<project>");
	fprintf(stdout, " %-30s Priority. Higher the value, higher the priority.\n", "-P,--priority=<integer>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem.  (Try -d all to start.)\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Show program version.\n", "-v,--version");
	fprintf(stdout, " %-30s Display this message.\n", "-h,--help");
	fprintf(stdout, " %-30s Select port at random and write it to this file.\n", "-Z,--random-port=<file>");
}

/*
Run the comparison program repeatedly until five seconds have elapsed,
in order to get a rough measurement of the execution time.
No very accurate for embedded functions. 
*/

double estimate_run_time( struct text_list *seta, struct text_list *setb )
{
	char line[ALLPAIRS_LINE_MAX];
	timestamp_t starttime, stoptime;
	int x,y;

	fprintf(stdout, "%s: sampling execution time of %s...\n",progname,allpairs_compare_program);

	starttime = timestamp_get();

	for(x=0;x<xstop;x++) {
		for(y=0;y<ystop;y++) {
			sprintf(line,"./%s %s %s %s",
				string_basename(allpairs_compare_program),
				extra_arguments,
				text_list_get(seta,x),
				text_list_get(setb,y)
				);

			FILE *file = fast_popen(line);
			if(!file)
				fatal("%s: couldn't execute %s: %s\n",progname,line,strerror(errno));

			while(fgets(line,sizeof(line),file)) {
				fprintf(stdout,"%s",line);
			}

			fast_pclose(file);

			stoptime = timestamp_get();
		
			if(stoptime-starttime>5000000) break;
		}
		if(stoptime-starttime>5000000) break;
	}

	double t = (double)(stoptime - starttime) / (x * ystop + y + 1) / 1000000;

	if(t<0.01) t = 0.01;

	return t;
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

	fprintf(stdout, "%s: %s estimated at %.02lfs per comparison\n",progname,allpairs_compare_program,compare_program_time);

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
		while( (int) (strlen(str) + buffer_pos + 3) >= buffer_size) {
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

struct work_queue_task * ap_task_create( struct text_list *seta, struct text_list *setb )
{
	int x,y;
	char *buf, *name;

	if(xcurrent>=xstop) {
		xcurrent=0;
		ycurrent+=yblock;
	}

	if(ycurrent>=ystop) return 0;

	char cmd[ALLPAIRS_LINE_MAX];
	sprintf(cmd,"./%s -e \"%s\" A B %s%s",string_basename(allpairs_multicore_program),extra_arguments,use_external_program ? "./" : "",string_basename(allpairs_compare_program));
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
	FILE *output = stdout;
	if(output_filename)
	{
		output = fopen(output_filename, "a");
		if(!output)
		{
			fprintf(stderr, "Cannot open %s for writing. Output to stdout instead.\n", output_filename);
			output = stdout;
		}
	}

	string_chomp(t->output);
	fprintf(output, "%s\n",t->output);

	if(output != stdout)
		fclose(output);

	work_queue_task_delete(t);
}

int main(int argc, char **argv)
{
	signed char c;
	struct work_queue *q;
	int port = WORK_QUEUE_DEFAULT_PORT;
	static const char *port_file = NULL;
	int work_queue_master_mode = WORK_QUEUE_MASTER_MODE_STANDALONE;
	char *project = NULL;
	int priority = 0;

	debug_config("allpairs_master");

	extra_files_list = list_create();

	struct option long_options[] = {
		{"debug", required_argument, 0, 'd'},
		{"help",  no_argument, 0, 'h'},
		{"version", no_argument, 0, 'v'},
		{"port", required_argument, 0, 'p'},
		{"random-port", required_argument, 0, 'Z'},
		{"extra-args", required_argument, 0, 'e'},
		{"width", required_argument, 0, 'x'},
		{"height", required_argument, 0, 'y'},
		{"advertise", no_argument, 0, 'a'},    //deprecated, left here for backwards compatibility
		{"project-name", required_argument, 0, 'N'},
		{"debug-file", required_argument, 0, 'o'},
		{"input-file", required_argument, 0, 'f'},
		{"estimated-time", required_argument, 0, 't'},
		{"priority", required_argument, 0, 'P'},
        {0,0,0,0}
	};


	while((c = getopt_long(argc, argv, "ad:e:f:hN:p:P:t:vx:y:Z:o:", long_options, NULL)) >= 0) {
		switch (c) {
	    case 'a':
			work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'e':
			extra_arguments = optarg;
			break;
		case 'f':
			list_push_head(extra_files_list,optarg);
			break;
		case 'o':
			free(output_filename);
			output_filename=xxstrdup(optarg);
			break;
		case 'h':
			show_help(progname);
			exit(0);
			break;
		case 'N':
			work_queue_master_mode = WORK_QUEUE_MASTER_MODE_CATALOG;
			free(project);
			project = xxstrdup(optarg);
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'P':
			priority = atoi(optarg);
			break;
		case 't':
			compare_program_time = atof(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, progname);
			exit(0);
			break;
		case 'x':
			xblock = atoi(optarg);
			break;
		case 'y':
			yblock = atoi(optarg);
			break;
		case 'Z':
			port_file = optarg;
			port = 0;
			break;
		default:
			show_help(progname);
			return 1;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if((argc - optind) < 3) {
		show_help(progname);
		exit(1);
	}

	struct text_list *seta = text_list_load(argv[optind]);
	if(!seta) {
		fprintf(stderr,"%s: couldn't open %s: %s\n",progname,argv[optind+1],strerror(errno));
		return 1;
	}

	fprintf(stdout, "%s: %s has %d elements\n",progname,argv[optind],text_list_size(seta));

	struct text_list *setb = text_list_load(argv[optind+1]);
	if(!setb) {
		fprintf(stderr,"%s: couldn't open %s: %s\n",progname,argv[optind+1],strerror(errno));
		return 1;
	}

	fprintf(stdout, "%s: %s has %d elements\n",progname,argv[optind+1],text_list_size(setb));

	if (!find_executable("allpairs_multicore","PATH",allpairs_multicore_program,sizeof(allpairs_multicore_program))) {
		fprintf(stderr,"%s: couldn't find allpairs_multicore in path\n",progname);
		return 1;
	}

	debug(D_DEBUG,"using multicore executable %s",allpairs_multicore_program);
	
	xstop = text_list_size(seta);
	ystop = text_list_size(setb);

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

	fprintf(stdout, "%s: using block size of %dx%d\n",progname,xblock,yblock);

	if(work_queue_master_mode == WORK_QUEUE_MASTER_MODE_CATALOG && !project) {
		fprintf(stderr, "allpairs: allpairs master running in catalog mode. Please use '-N' option to specify the name of this project.\n");
		fprintf(stderr, "allpairs: Run \"%s -h\" for help with options.\n", argv[0]);
		return 1;
	}

	q = work_queue_create(port);

	//Read the port the queue is actually running, in case we just called
	//work_queue_create(LINK_PORT_ANY)
	port  = work_queue_port(q);

	if(!q) {
		fprintf(stderr,"%s: could not create work queue on port %d: %s\n",progname,port,strerror(errno));
		return 1;
	}

	if(port_file)
		opts_write_port_file(port_file, port);

	// advanced work queue options
	work_queue_specify_master_mode(q, work_queue_master_mode);
	work_queue_specify_name(q, project);
	work_queue_specify_priority(q, priority);

	fprintf(stdout, "%s: listening for workers on port %d...\n",progname,work_queue_port(q));

	while(1) {
		struct work_queue_task *task = NULL;
		while(work_queue_hungry(q)) {
			task = ap_task_create(seta,setb);
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
