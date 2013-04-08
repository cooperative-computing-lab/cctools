#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <limits.h>

#include "hash_table.h"

#define MAKEFLOW_PATH "makeflow"
#define MAKEFLOW_BUNDLE_FLAG "-b"
#define MAKEFLOW_FILE_FLAG "-f "

void initialize(char *output_directory, char *input_file, struct hash_table *ht, struct graph *g){
	pid_t pid;

	char expanded_input[PATH_MAX];
	realpath(input_file, expanded_input);

	pid = fork();
	if ( pid < 0 ) {
		fprintf(stderr, "Cannot fork. Exiting...\n");
		exit(1);
	}
	if ( pid == 0 ) {
		/* Child process */
		char * const args[6] = { "linking makeflow" , MAKEFLOW_BUNDLE_FLAG, output_directory, MAKEFLOW_FILE_FLAG, expanded_input, NULL};

		execvp(MAKEFLOW_PATH, args); 
	}
	else {
		/* Parent process */
	}
}

int main(void){
	char *output = "output_dir";
	char *input = "test.mf";

	struct hash_table *names;
	names = hash_table_create(0, NULL);
	
	struct graph *dependencies;
	dependencies = graph_create();

	initialize(output, input, names, dependencies);

	return 0;
}
