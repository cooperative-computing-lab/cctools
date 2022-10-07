/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

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

const char *urls[] =
{
"http://www.gutenberg.org/files/1960/1960.txt",
"http://www.gutenberg.org/files/1961/1961.txt",
"http://www.gutenberg.org/files/1962/1962.txt",
"http://www.gutenberg.org/files/1963/1963.txt",
"http://www.gutenberg.org/files/1965/1965.txt",
"http://www.gutenberg.org/files/1966/1966.txt",
"http://www.gutenberg.org/files/1967/1967.txt",
"http://www.gutenberg.org/files/1968/1968.txt",
"http://www.gutenberg.org/files/1969/1969.txt",
"http://www.gutenberg.org/files/1970/1970.txt",
"http://www.gutenberg.org/files/1971/1971.txt",
"http://www.gutenberg.org/files/1972/1972.txt",
"http://www.gutenberg.org/files/1973/1973.txt",
"http://www.gutenberg.org/files/1974/1974.txt",
"http://www.gutenberg.org/files/1975/1975.txt",
"http://www.gutenberg.org/files/1976/1976.txt",
"http://www.gutenberg.org/files/1977/1977.txt",
"http://www.gutenberg.org/files/1978/1978.txt",
"http://www.gutenberg.org/files/1979/1979.txt",
"http://www.gutenberg.org/files/1980/1980.txt",
"http://www.gutenberg.org/files/1981/1981.txt",
"http://www.gutenberg.org/files/1982/1982.txt",
"http://www.gutenberg.org/files/1983/1983.txt",
"http://www.gutenberg.org/files/1985/1985.txt",
"http://www.gutenberg.org/files/1986/1986.txt",
"http://www.gutenberg.org/files/1987/1987.txt",
};

int url_count=25;

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

	vine_specify_debug_log(m,"manager.log");
	vine_specify_algorithm(m,VINE_SCHEDULE_FILES);

	for(i=0;i<url_count;i++) {
		for(j=0;j<url_count;j++) {
			struct vine_task *t = vine_task_create("./vine_example_gutenberg_script.sh filea.txt fileb.txt");

			vine_task_specify_input_file(t, "vine_example_gutenberg_script.sh", "vine_example_gutenberg_script.sh", VINE_CACHE);
			vine_task_specify_input_url(t, urls[i], "filea.txt", VINE_CACHE);
			vine_task_specify_input_url(t, urls[j], "fileb.txt", VINE_CACHE);

			vine_task_set_cores(t,1);

			int taskid = vine_submit(m, t);

			printf("submitted task (id# %d): %s\n", taskid, vine_task_get_command(t) );
		}
	}

	printf("waiting for tasks to complete...\n");

	while(!vine_empty(m)) {
		t  = vine_wait(m, 5);
		if(t) {
			vine_result_t r = vine_task_get_result(t);
			int id = vine_task_get_taskid(t);

			if(r==VINE_RESULT_SUCCESS) {
				printf("task %d output: %s\n",id,vine_task_get_output(t));
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
