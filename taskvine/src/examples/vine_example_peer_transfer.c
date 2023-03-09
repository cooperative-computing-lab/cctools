/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "taskvine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>


#define LANDMARK_URL "https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz"
int main(int argc, char *argv[])
{
	struct vine_manager *m;
	struct vine_task *t;
	int i;

	vine_set_runtime_info_path("vine_example_blast_info");
	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", vine_port(m));

	//vine_enable_debug_log(m,"manager.log");
	vine_set_scheduler(m,VINE_SCHEDULE_FILES);
//	vine_enable_perf_log(m, "my.perf.log");
	//vine_enable_transactions_log(m, "my.tr.log");
	
	vine_tune(m, "wait-for-workers", 500);	
			

	if(argv[1] && strcmp(argv[1],"-peer") == 0){
		vine_enable_peer_transfers(m);
		vine_tune(m, "file-source-max-transfers", 2);
	}

	if(argv[2]){
		vine_tune(m, "worker-source-max-transfers", atoi(argv[2]));
	}
 
	struct vine_file *database = vine_declare_untar(m, vine_declare_url(m, LANDMARK_URL, VINE_PEER_SHARE));
	if(argv[3]){
		for(i=0;i<atoi(argv[3]);i++) {
			struct vine_task *t = vine_task_create("ls -l slackware*; sleep 30");
	  
			vine_task_add_input(t,database,"landmark", VINE_NOCACHE );

			int task_id = vine_submit(m, t);

			printf("submitted task (id# %d): %s\n", task_id, vine_task_get_command(t) );
		}
	}

	printf("waiting for tasks to complete...\n");

	while(!vine_empty(m)) {
		t  = vine_wait(m, 5);
		if(t) {
			vine_result_t r = vine_task_get_result(t);
			int id = vine_task_get_id(t);

			if(r==VINE_RESULT_SUCCESS) {
				printf("task %d output: %s\n",id,vine_task_get_stdout(t));
			} else {
				printf("task %d failed: %s\n",id,vine_result_string(r));
			}
			vine_task_delete(t);
		}
	}

	printf("all tasks complete!\n");

	vine_delete(m);

	return 0;
}
