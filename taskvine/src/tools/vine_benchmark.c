/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "taskvine.h"

#include "debug.h"
#include "cctools.h"
#include "path.h"
#include "errno.h"
#include "unlink_recursive.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "itable.h"
#include "list.h"
#include "get_line.h"

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>


int submit_tasks(struct vine_manager *q, int input_size, int run_time, int output_size, int count, char *category )
{
	static int ntasks=0;
	char output_file[128];
	char input_file[128];
	char command[256];
	char gen_input_cmd[256];

	sprintf(input_file, "input.%d",ntasks);
	sprintf(gen_input_cmd, "dd if=/dev/zero of=%s bs=1048576 count=%d",input_file,input_size);
	system(gen_input_cmd);

	struct vine_file *input = vine_declare_file(q, input_file, VINE_CACHE);

	/*
	Note that bs=1m and similar are not portable across various
	implementations of dd, so we spell it out as bs=1048576
	*/

	int i;
	for(i=0;i<count;i++) {

		sprintf(output_file, "output.%d",ntasks);
		sprintf(command, "dd if=/dev/zero of=outfile bs=1048576 count=%d; sleep %d", output_size, run_time );
		struct vine_file *output = vine_declare_file(q, output_file, VINE_PEER_NOSHARE);

		ntasks++;

		struct vine_task *t = vine_task_create(command);
		vine_task_add_input(t, input, "infile", 0);
		vine_task_add_output(t, output, "outfile", 0);
		vine_task_set_cores(t,1);

		if(category && strlen(category) > 0)
			vine_task_set_category(t, category);

		vine_submit(q, t);
	}

	return 1;
}

void wait_for_all_tasks( struct vine_manager *q )
{
	struct vine_task *t;
	while(!vine_empty(q)) {
		t = vine_wait(q,5);
		if(t) vine_task_delete(t);
	}
}

void mainloop( struct vine_manager *q )
{
	char line[1024];
	char category[1024];

	int sleep_time, run_time, input_size, output_size, count;

	while(1) {
		printf("vine_test > ");
		fflush(stdout);

		if(!fgets(line,sizeof(line),stdin)) break;

		if(line[0]=='#') continue;

		string_chomp(line);

		strcpy(category, "default");

		if(sscanf(line,"sleep %d",&sleep_time)==1) {
			printf("sleeping %d seconds...\n",sleep_time);
			sleep(sleep_time);
		} else if(!strcmp(line,"wait")) {
			printf("waiting for all tasks...\n");
			wait_for_all_tasks(q);
		} else if(sscanf(line, "submit %d %d %d %d %s",&input_size, &run_time, &output_size, &count, category) >= 4) {
			printf("submitting %d tasks...\n",count);
			submit_tasks(q,input_size,run_time,output_size,count,category);
		} else if(!strcmp(line,"quit") || !strcmp(line,"exit")) {
			break;
		} else if(!strcmp(line,"help")) {
			printf("Available commands are:\n");
			printf("sleep <n>               Sleep for n seconds.\n");
			printf("wait                    Wait for all submitted tasks to finish.\n");
			printf("submit <I> <T> <O> <N>  Submit N tasks that read I MB input,\n");
			printf("                        run for T seconds, and produce O MB of output.\n");
			printf("quit, exit              Wait for all tasks to complete, then exit.\n");
			printf("\n");
		} else {
			fprintf(stderr,"ignoring badly formatted line: %s\n",line);			continue;
		}
	}
}

/* vim: set noexpandtab tabstop=8: */


void show_help( const char *cmd )
{
	printf("Usage: %s [options]\n",cmd);
	printf("Where options are:\n");
	printf("-m         Enable resource monitoring.\n");
	printf("-Z <file>  Write listening port to this file.\n");
	printf("-p <port>  Listen on this port.\n");
	printf("-N <name>  Advertise this project name.\n");
	printf("-v         Show version information.\n");
	printf("-h         Show this help screen.\n");
}

int main(int argc, char *argv[])
{
	int port = VINE_DEFAULT_PORT;
	const char *port_file=0;
	const char *project_name=0;
	int monitor_flag = 0;
	int c;

	while((c = getopt(argc, argv, "d:mN:p:Z:vh"))!=-1) {
		switch (c) {
		case 'p':
			port = atoi(optarg);
			break;
		case 'm':
			monitor_flag = 1;
			break;
		case 'N':
			project_name = optarg;
			break;
		case 'Z':
			port_file = optarg;
			port = 0;
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			return 0;
			break;
		case 'h':
			show_help(path_basename(argv[0]));
			return 0;
		default:
			show_help(path_basename(argv[0]));
			return 1;
		}
	}

    vine_set_runtime_info_path("vine_benchmark_info");

	struct vine_manager *q = vine_create(port);
	if(!q) fatal("couldn't listen on any port!");

	printf("listening on port %d...\n", vine_port(q));

	if(port_file) {
		FILE *file = fopen(port_file,"w");
		if(!file) fatal("couldn't open %s: %s",port_file,strerror(errno));
		fprintf(file,"%d\n",vine_port(q));
		fclose(file);
	}

	if(project_name) {
		vine_set_name(q,project_name);
	}

	if(monitor_flag) {
		unlink_recursive("vine_benchmark_monitor");
		vine_enable_monitoring(q, 1, 0);
		vine_set_category_mode(q, NULL, VINE_ALLOCATION_MODE_MAX_THROUGHPUT);
	}


	mainloop(q);

	vine_delete(q);

	return 0;
}
