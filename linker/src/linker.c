#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <limits.h>
#include <string.h>

#include "list.h"

#define MAKEFLOW_PATH "makeflow"
#define MAKEFLOW_BUNDLE_FLAG "-b"

typedef enum {UNKNOWN, PYTHON} file_type;

typedef struct {
	char *original_name;
	char *final_name;
	char *parent;
	char *superparent;
	int  depth;
	file_type type;
} dependency;

char *python_extensions[2] = { "py", "pyc" };

void initialize( char *output_directory, char *input_file, struct list *d){
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
					dependency *new_dependency = (dependency *) malloc(sizeof(dependency));
					new_dependency->original_name = original_name;
					new_dependency->final_name = buffer;
					list_push_tail(d, (void *) new_dependency);
					size = 0;
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

const char *filename_extension(const char *filename) {
	const char *dot = strrchr(filename, '.');
	if(!dot || dot == filename) return "";
	return dot + 1;
}

file_type file_extension_known(const char *filename){
	const char *extension = filename_extension(filename);

	int j;
	for(j=0; j< 2; j++){
		if(!strcmp(python_extensions[j], extension))
			return PYTHON;
	}

	return UNKNOWN;
}

file_type find_driver_for(const char *name){
	file_type type = UNKNOWN;

	if((type = file_extension_known(name))){}

	return type;
}

void find_drivers(struct list *d){
	dependency *dep;
	file_type my_type;
	list_first_item(d);
	while((dep = list_next_item(d))){
		my_type = find_driver_for(dep->final_name);
		printf("%d\n", my_type);
	}
}

int main(void){
	char *output = "output_dir";
	char *input = "test.mf";

	struct list *dependencies;
	dependencies = list_create();

	initialize(output, input, dependencies);

	find_drivers(dependencies);

	return 0;
}
