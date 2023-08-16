/*
This example shows some of the remote data handling features of taskvine.
It performs an all-to-all comparison of twenty (relatively small) documents
downloaded from the Gutenberg public archive.

A small shell script (vine_example_guteberg_task.sh) is used to perform
a simple text comparison of each pair of files.
*/

#include "taskvine.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#define URL_COUNT 25

const char *urls[URL_COUNT] =
{
"https://www.gutenberg.org/files/1960/1960.txt",
"https://www.gutenberg.org/files/1961/1961.txt",
"https://www.gutenberg.org/files/1962/1962.txt",
"https://www.gutenberg.org/files/1963/1963.txt",
"https://www.gutenberg.org/files/1965/1965.txt",
"https://www.gutenberg.org/files/1966/1966.txt",
"https://www.gutenberg.org/files/1967/1967.txt",
"https://www.gutenberg.org/files/1968/1968.txt",
"https://www.gutenberg.org/files/1969/1969.txt",
"https://www.gutenberg.org/files/1970/1970.txt",
"https://www.gutenberg.org/files/1971/1971.txt",
"https://www.gutenberg.org/files/1972/1972.txt",
"https://www.gutenberg.org/files/1973/1973.txt",
"https://www.gutenberg.org/files/1974/1974.txt",
"https://www.gutenberg.org/files/1975/1975.txt",
"https://www.gutenberg.org/files/1976/1976.txt",
"https://www.gutenberg.org/files/1977/1977.txt",
"https://www.gutenberg.org/files/1978/1978.txt",
"https://www.gutenberg.org/files/1979/1979.txt",
"https://www.gutenberg.org/files/1980/1980.txt",
"https://www.gutenberg.org/files/1981/1981.txt",
"https://www.gutenberg.org/files/1982/1982.txt",
"https://www.gutenberg.org/files/1983/1983.txt",
"https://www.gutenberg.org/files/1985/1985.txt",
"https://www.gutenberg.org/files/1986/1986.txt",
};

const char *compare_script =
"#!/bin/sh\n\
# Perform a simple comparison of the words counts of each document\n\
# which are given as the first ($1) and second ($2) command lines.\n\
cat $1 | tr \" \" \"\\n\" | sort | uniq -c | sort -rn | head -10l > a.tmp\n\
cat $2 | tr \" \" \"\\n\" | sort | uniq -c | sort -rn | head -10l > b.tmp\n\
diff a.tmp b.tmp\nexit 0\n";

int main(int argc, char *argv[])
{
	struct vine_manager *m;
	struct vine_task *t;
	int i,j ;

	m = vine_create(VINE_DEFAULT_PORT);
	if(!m) {
		printf("couldn't create manager: %s\n", strerror(errno));
		return 1;
	}
	printf("listening on port %d...\n", vine_port(m));

	printf("setting up input files...\n");
	struct vine_file *script = vine_declare_buffer(m, compare_script, strlen(compare_script), VINE_CACHE);
	struct vine_file *files[URL_COUNT];

	for(i=0;i<URL_COUNT;i++) {
		files[i] = vine_declare_url(m, urls[i], VINE_CACHE);
	}

	printf("submitting tasks...\n");
	for(i=0;i<URL_COUNT;i++) {
		for(j=0;j<URL_COUNT;j++) {
			struct vine_task *t = vine_task_create("./vine_example_gutenberg_script.sh filea.txt fileb.txt");

			vine_task_add_input(t, script, "vine_example_gutenberg_script.sh", 0);
			vine_task_add_input(t, files[i], "filea.txt", 0);
			vine_task_add_input(t, files[j], "fileb.txt", 0);

			vine_task_set_cores(t,1);

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

/* vim: set noexpandtab tabstop=8: */
