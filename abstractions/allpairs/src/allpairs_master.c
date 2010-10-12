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

#include "allpairs_compare.h"

#define ALLPAIRS_LINE_MAX 4096

static const char *progname = "allpairs_master";
static const char *allpairs_multicore_program = "allpairs_multicore";
static char allpairs_compare_program[ALLPAIRS_LINE_MAX];

static int use_external_program = 0;

static int xcurrent = 0;
static int ycurrent = 0;
static int xblock = 10;
static int yblock = 10;
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
	printf(" -p <integer>	The port that the Master will be listening on.\n");
	printf(" -x <integer>	Block width.  (default is chosen according to hardware conditions)\n");
	printf(" -y <integer>	Block height. (default is chosen according to hardware conditions)\n");
	printf(" -N <project>   Report the master information to a catalog server with the project name - <project>\n");
	printf(" -E <integer>   Priority. Higher the value, higher the priority.\n");
	printf(" -d <string>	Enable debugging for this subsystem.\n");
	printf(" -v         	Show program version.\n");
	printf(" -h         	Display this message.\n");
}

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
	sprintf(cmd,"./%s A B %s%s",allpairs_multicore_program,use_external_program ? "./" : "",string_basename(allpairs_compare_program));
	struct work_queue_task *task = work_queue_task_create(cmd);

	if(use_external_program) {
		work_queue_task_specify_file(task,allpairs_compare_program,string_basename(allpairs_compare_program),WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);
	}

	work_queue_task_specify_file(task,allpairs_multicore_program,string_basename(allpairs_multicore_program),WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);

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

	while((c = getopt(argc, argv, "d:E:vhx:p:y:i:j:k:l:N:X:Y:c:")) != (char) -1) {
		switch (c) {
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

	struct text_list *setb = text_list_load(argv[optind+1]);
	if(!setb) {
		fprintf(stderr,"%s: couldn't open %s: %s\n",progname,argv[optind+2],strerror(errno));
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

	q = work_queue_create(port);
	if(!q) {
		fprintf(stderr,"%s: could not create work queue on port %d: %s\n",progname,port,strerror(errno));
		return 1;
	}

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
