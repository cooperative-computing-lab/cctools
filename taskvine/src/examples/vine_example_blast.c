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

Each task in the workflow performs a query of the database using
16 (random) query strings generated at the manager.
Both the downloads are automatically unpacked, cached, and shared
with all the same tasks on the worker.
*/

#include "taskvine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#define BLAST_URL "https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST/ncbi-blast-2.13.0+-x64-linux.tar.gz"

#define LANDMARK_URL "https://ftp.ncbi.nlm.nih.gov/blast/db/landmark.tar.gz"

/* Number of characters in each query */
#define QUERY_LENGTH 128

/* Number of queries in each task */
#define QUERY_COUNT 16

/* Number of tasks to generate */
#define TASK_COUNT 1000

/* Permitted letters in an amino acid sequence */
const char* amino_letters = "ACGTUiRYKMSWBDHVN";

/* Make a sequence in a query */
static void make_sequence(char* q, int s) {
	int i;
	int prfix_len = strlen(">query\n");
	int sequence_size = sizeof(char) * (prfix_len + QUERY_LENGTH + 1);
	strcpy(q+s*sequence_size, ">query\n");
	int start = s * sequence_size + prfix_len;
	for (i = start; i < start + QUERY_LENGTH; ++i) {
		*(q+i) = amino_letters[random()%strlen(amino_letters)];
	}
	*(q+i) = '\n';
}

/* Create a query string consisting of
{query_count} sequences of {query_length} characters.
*/
static char* make_query() {
	int query_size = sizeof(char) * (strlen(">query\n") + QUERY_LENGTH + 1) * QUERY_COUNT + 1;
	char* q = malloc(query_size);
	int i;
	for (i = 0; i < QUERY_COUNT; ++i) {
		make_sequence(q, i);
	}
	*(q+query_size-1) = '\0';
	return q;
}

int main(int argc, char *argv[])
{
	struct vine_manager *m;
	struct vine_task *t;
	int i;

	vine_set_runtime_info_path("runtime_info");

	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	
	vine_set_name(m, "blast-example");
	printf("TaskVine listening on %d\n", vine_port(m));

	vine_enable_monitoring(m, 1, 0);
	vine_enable_peer_transfers(m);

	printf("Declaring files...");
	struct vine_file *blast_url = vine_declare_url(m, BLAST_URL, 0);
	struct vine_file *landm_url = vine_declare_url(m, LANDMARK_URL, 0);

	struct vine_file *software = vine_declare_untar(m, blast_url, 0);
	struct vine_file *database = vine_declare_untar(m, landm_url, 0);

	printf("Declaring tasks...");
	char* query_string;
	for(i=0;i<TASK_COUNT;i++) {
		struct vine_task *t = vine_task_create("blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file");
		
		query_string = make_query();
		struct vine_file *query = vine_declare_buffer(m, query_string, strlen(query_string), 0);
		vine_task_add_input(t, query, "query.file", VINE_NOCACHE);
		vine_task_add_input(t,software,"blastdir", VINE_CACHE );
		vine_task_add_input(t,database,"landmark", VINE_CACHE );
		vine_task_set_env_var(t,"BLASTDB","landmark");

		int task_id = vine_submit(m, t);

		printf("submitted task (id# %d): %s\n", task_id, vine_task_get_command(t) );
		free(query_string);
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
