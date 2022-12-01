/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This example shows some of the data handling features of taskvine.
It performs a BLAST search of the "Landmark" model organism database.
It works by constructing tasks that download the blast executable
and landmark database from NCBI, and then performs a short query.

The query is provided by a string (but presented to the task as a file.)
Both the downloads are automatically unpacked, cached, and shared
with all the same tasks on the worker.
*/


#include "taskvine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

const char *query_string = ">P01013 GENE X PROTEIN (OVALBUMIN-RELATED)\n\
		QIKDLLVSSSTDLDTTLVLVNAIYFKGMWKTAFNAEDTREMPFHVTKQESKPVQMMCMNNSFNVATLPAE\n\
		KMKILELPFASGDLSMLVLLPDEVSDLERIEKTINFEKLTEWTNPNTMEKRRVKVYLPQMKIEEKYNLTS\n\
		VLMALGMTDLFIPSANLTGISSAESLKISQAVHGAFMELSEDGIEMAGSTGVIEDIKHSPESEQFRADHP\n\
		FLFLIKHNPTNTIVYFGRYWSP\n\
";

#define BLAST_URL "https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.13.0+-x64-linux.tar.gz"

#define LANDMARK_URL "https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz"

int main(int argc, char *argv[])
{
	struct vine_manager *m;
	struct vine_task *t;
	int i;

	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", vine_port(m));

	vine_enable_debug_log(m,"manager.log");
	vine_set_scheduler(m,VINE_SCHEDULE_FILES);

	for(i=0;i<10;i++) {
		struct vine_task *t = vine_task_create("blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file");
	  
		vine_task_add_input_buffer(t,query_string,strlen(query_string),"query.file", VINE_NOCACHE);
		vine_task_add_input(t,vine_file_untgz(vine_file_url(BLAST_URL)),"blastdir", VINE_CACHE );
		vine_task_add_input(t,vine_file_untgz(vine_file_url(LANDMARK_URL)),"landmark", VINE_CACHE );
		vine_task_set_env_var(t,"BLASTDB","landmark");

		int task_id = vine_submit(m, t);

		printf("submitted task (id# %d): %s\n", task_id, vine_task_get_command(t) );
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
