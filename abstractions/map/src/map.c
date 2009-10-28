/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
First version written by Connor Keenan in 2009.
*/

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "list.h"
#include "debug.h"
#include "auth_all.h"
#include "work_queue.h"
#include "stringtools.h"

const char * get_file_extension( const char *path )
{
	char *p = strrchr(path,'.');
	if(p) {
		p++;
		return p;
	} else {
		return "";
	}
}

static void show_version(const char *cmd)
{
        printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help( const char *cmd )
{
	printf("use: %s [options] <executable> <input-list> <output-dir> <output-type>\n",cmd);
	printf("where options are:\n");
	printf(" -p         Port number to listen on.  (default=%d)\n",WORK_QUEUE_DEFAULT_PORT);
	printf(" -r         Retry application level failures.\n");
	printf(" -f <a,b,c> Extra files to send along to each job.\n"); 
	printf(" -d <flag>  Enable debugging for this subsystem.\n");
	printf(" -v         Show version string.\n");
	printf(" -h         This message.\n");
}

int main(int argc, char *argv[])
{
	int retry_failures = 0;
	char c;

	int port = WORK_QUEUE_DEFAULT_PORT;

	struct work_queue *q;
	struct work_queue_task *t;
	struct list *extra_files = list_create();

	while((c=getopt(argc,argv,"p:f:rd:vh"))!=(char)-1) {
		switch(c) {
			case 'p':
				port = atoi(optarg);
				break;
			case 'r':
				retry_failures = 1;
				break;
			case 'f':
				list_push_head(extra_files,strdup(optarg));
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'v':
				show_version(argv[0]);
				exit(0);
				break;
			case 'h':
				show_help(argv[0]);
				exit(0);
				break;
		}
	}

	if(argc-optind!=4) {
		show_help(argv[0]);
		return 0;
	}

	const char *command = argv[optind];
	const char *inputlist = argv[optind+1];
	const char *outputdir = argv[optind+2];
	const char *outputtype = argv[optind+3];

	debug_config(argv[0]);

	FILE *file = fopen(inputlist,"r");
	if(!file) {
		fprintf(stderr,"map: couldn't open %s: %s\n",inputlist,strerror(errno));
		return 1;
	}

	q = work_queue_create(port,time(0)+60);
	if(!q) {
		fprintf(stderr,"couldn't create work queue: perhaps port %d is already in use?\n",port);
		return 1;
	}

	printf("map: listening for workers on port %d...\n",port);

	while(1) {
		if(feof(file) && work_queue_empty(q)) break;

		while(!feof(file) && work_queue_hungry(q)) {

			char local_infile[WORK_QUEUE_LINE_MAX];
			char local_outfile[WORK_QUEUE_LINE_MAX];

			char remote_infile[WORK_QUEUE_LINE_MAX];
			char remote_outfile[WORK_QUEUE_LINE_MAX];

			char cmdline[WORK_QUEUE_LINE_MAX];

			if(!fgets(local_infile,sizeof(local_infile),file)) break;

			string_chomp(local_infile);
			sprintf(remote_infile,"a.%s",get_file_extension(local_infile));
			sprintf(remote_outfile,"b.%s",outputtype);
			sprintf(local_outfile,"%s/%s",outputdir,string_basename(local_infile));

			char *p = strrchr(local_outfile,'.');
			if(p) *p = 0;

			strcat(local_outfile,".");
			strcat(local_outfile,outputtype);

			if(access(local_outfile,R_OK)==0) {
				printf("skipping:  %s because %s already exists\n",local_infile,local_outfile);
				continue;
			}

			sprintf(cmdline,"./%s %s %s",string_basename(command),remote_infile,remote_outfile);

			t = work_queue_task_create(cmdline);

			work_queue_task_specify_tag(t,local_infile);
			work_queue_task_specify_input_file(t,command,string_basename(command));
			work_queue_task_specify_input_file(t,local_infile,remote_infile);
			work_queue_task_specify_output_file(t,remote_outfile,local_outfile);

			char *f;

			list_first_item(extra_files);
			while((f=list_next_item(extra_files))) {
				work_queue_task_specify_input_file(t,f,string_basename(f));
			}

			work_queue_submit(q,t);
		}

		t = work_queue_wait(q,WORK_QUEUE_WAITFORTASK);
		if(t) {
			if(t->return_status!=0) {
				fprintf(stderr,"failed:   %s\n",t->tag);
				fprintf(stderr,"with output: %s\n",t->output);
				if(retry_failures) {
					work_queue_submit(q,t);
				} else {
					work_queue_task_delete(t);
				}
			} else {
				printf("complete: %s\n",t->tag);
				work_queue_task_delete(t);
			}
		}

	}

	fclose(file);
	work_queue_delete(q);

	return 0;
}

