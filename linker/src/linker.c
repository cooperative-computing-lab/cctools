#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <limits.h>

#include "hash_table.h"

#define MAKEFLOW_PATH "makeflow"
#define MAKEFLOW_BUNDLE_FLAG "-b"

void initialize( char *output_directory, char *input_file, struct hash_table *ht, struct graph *g ){
	pid_t pid;
	int pipefd[2];
	pipe(pipefd);

	char expanded_input[PATH_MAX];
	realpath(input_file, expanded_input);


	switch ( pid = fork() ){
	case -1:
		fprintf( stderr, "Cannot fork. Exiting...\n" );
		exit(1);
	case 0:
		/* Child process */
		close(pipefd[0]);
		dup2(pipefd[1], 1);
		close(pipefd[1]);

		char * const args[5] = { "linking makeflow" , MAKEFLOW_BUNDLE_FLAG, output_directory, expanded_input, NULL };
		execvp(MAKEFLOW_PATH, args); 
		exit(1);
	default:
		/* Parent process */
		close(pipefd[1]);
		char next;
		char *buffer = (char *) malloc(sizeof(char));
		char *original_name;
		int size = 0;
		while (read(pipefd[0], &next, sizeof(next)) != 0){
			switch ( next ){
				case '\t':
					original_name = (char *)realloc((void *)buffer,size+1);
					*(original_name+size) = '\0';
					buffer = (char *) malloc(sizeof(char));
					size = 0;
					break;
				case '\n':
					buffer = realloc(buffer, size+1);
					*(buffer+size) = '\0';
					hash_table_insert(ht, original_name, (void *) buffer);
					size = 0;
					free(original_name);
					original_name = NULL;
					buffer = NULL;
					break;
				case '\0':
				default:
					buffer = realloc(buffer, size+1);
					*(buffer+size) = next;
					size++;
			}
		}
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
