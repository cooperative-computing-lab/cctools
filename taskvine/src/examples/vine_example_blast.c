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
static char *make_query() {
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

	//Logs and other runtime information will be written to this directory.
	vine_set_runtime_info_path("runtime_info");

	//Create the manager. All tasks and files will be declared with respect to
	//this manager.
	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("TaskVine listening on %d\n", vine_port(m));


	//Set the name of the manager to use in the catalog server. Workers can use
	//this name to find the address of the manager and connect to it.
	//Alternatively, workers can connect directly to the manager if they
	//already know the address.
	vine_set_name(m, "blast-example");

	//Enable monitoring of tasks. (0 is no, 1 is yes)
	vine_enable_monitoring(m, /* terminate tasks that exhaust their resources */ 1, /* extra debug info */ 0);

	//For cached files that are used by several tasks, allow workers to copy
	//these files among themselves. If peer transfers are not enabled, files
	//may originate only from the manager or their respective mini tasks (e.g.
	//url declarations).
	// When peer transfers are enabled, it can be disabled on a per file basis
	// with the flag VINE_PEER_NOSHARE when declaring the file.
	vine_enable_peer_transfers(m);

	printf("Declaring files...");
	//These files do not change across vine runs, thus we cache them with VINE_CACHE_ALWAYS,
	//which keeps the files at the workers even when the current manager terminates.
	//The files cached in this way are removed when the worker terminates.
	//For files that should be only kept per workflow, use VINE_CACHE.
	//These declarations only register the files with the manager, but do not
	//associate them with any task (yet).
	struct vine_file *blast_url = vine_declare_url(m, BLAST_URL, VINE_CACHE_ALWAYS);
	struct vine_file *landm_url = vine_declare_url(m, LANDMARK_URL, VINE_CACHE_ALWAYS);

	//An untar declaration is an example of a mini task. The untar declaration
	//takes as an input another file (int this case an url), and unpacks it.
	//The name of the directory to which it appears in each task sandbox is set
	//by the task.
	struct vine_file *software = vine_declare_untar(m, blast_url, VINE_CACHE_ALWAYS);
	struct vine_file *database = vine_declare_untar(m, landm_url, VINE_CACHE_ALWAYS);


	printf("Declaring tasks...");
	char* query_string;
	for(i=0;i<TASK_COUNT;i++) {
		struct vine_task *t = vine_task_create("blastdir/ncbi-blast-2.13.0+/bin/blastp -db landmark -query query.file");

		query_string = make_query();

		//A query is particular to a task, and will be used only for its tasks,
		//therefore we marked with VINE_CACHE_NEVER. It will be deleted from
		//the worker as soon as the task's results are retrieved.
		struct vine_file *query = vine_declare_buffer(m, query_string, strlen(query_string), VINE_CACHE_NEVER);

		//The buffer makes a copy of the string, thus the memory associated
		//with it should be freed.
		free(query_string);

		//Associated the declared files with this tasks. The declared files get
		//a remote name, which is the name the task's command expects.
		vine_task_add_input(t, query, "query.file", 0);
		vine_task_add_input(t, software, "blastdir", 0);
		vine_task_add_input(t, database, "landmark", 0);

		//The blastp command looks for this environment variable to locate its
		//db.
		vine_task_set_env_var(t,"BLASTDB","landmark");

		//Each task is set to use one core. In workers with more than one core,
		//the memory and disk will be divided proportionally among the cores.
		vine_task_set_cores(t, 1);

		//Once the task description is finished, the task is submitted to the
		//manager for execution.
		int task_id = vine_submit(m, t);

		printf("submitted task (id# %d): %s\n", task_id, vine_task_get_command(t) );
	}

	printf("waiting for tasks to complete...\n");

	while(!vine_empty(m)) {
		//Wait for 5 seconds for a task to complete. In this wait, the manager
		//performs all of its functions (e.g., send/retrieve tasks, and connect
		//new workers)
		t  = vine_wait(m, 5);
		if(t) {
			vine_result_t r = vine_task_get_result(t);
			int id = vine_task_get_id(t);

			if(r == VINE_RESULT_SUCCESS) {
				printf("task %d output: %s\n", id, vine_task_get_stdout(t));
			} else {
				printf("task %d failed: %s\n", id, vine_result_string(r));
			}
			vine_task_delete(t);
		}
	}
	printf("all tasks complete!\n");

	//Free the manager structure, and release all the workers.
	vine_delete(m);

	return 0;
}
