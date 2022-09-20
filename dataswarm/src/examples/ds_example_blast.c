/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This example shows some of the data handling features of dataswarm.
It performs a BLAST search of the "Landmark" model organism database.
It works by constructing tasks that download the blast executable
and landmark database from NCBI, and then performs a short query.

The query is provided by a string (but presented to the task as a file.)
Both the downloads are automatically unpacked, cached, and shared
with all the same tasks on the worker.
*/


#include "dataswarm.h"

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
	struct ds_manager *m;
	struct ds_task *t;
	int i;

	m = ds_create(DS_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create queue: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", ds_port(m));

	ds_specify_algorithm(m,DS_SCHEDULE_FILES);

	for(i=0;i<10;i++) {
		struct ds_task *t = ds_task_create("blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file");
	  
		ds_task_specify_buffer(t,query_string,strlen(query_string),"query.file", DS_NOCACHE);
		ds_task_specify_url(t,BLAST_URL,"blastdir", DS_INPUT, DS_CACHE|DS_UNPACK );
		ds_task_specify_url(t,LANDMARK_URL,"landmark", DS_INPUT, DS_CACHE|DS_UNPACK );
		ds_task_specify_env(t,"BLASTDB","landmark");

		int taskid = ds_submit(m, t);

		printf("submitted task (id# %d): %s\n", taskid, ds_task_get_command(t) );
	}

	printf("waiting for tasks to complete...\n");

	while(!ds_empty(m)) {
		t  = ds_wait(m, 5);
		if(t) {
			ds_result_t r = ds_task_get_result(t);
			int id = ds_task_get_taskid(t);

			if(r==DS_RESULT_SUCCESS) {
				printf("task %d output: %s\n",id,ds_task_get_output(t));
			} else {
				printf("task %d failed: %s\n",id,ds_result_string(r));
			}
			ds_task_delete(t);
		}
	}

	printf("all tasks complete!\n");

	ds_delete(m);

	return 0;
}
