#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <limits.h>
#include <string.h>

#include "list.h"

#define MAKEFLOW_PATH "makeflow"
#define MAKEFLOW_BUNDLE_FLAG "-b"

typedef enum {UNKNOWN, PERL, PYTHON} file_type;

typedef struct{
	char *original_name;
	char *final_name;
	char *parent;
	char *superparent;
	int  depth;
	file_type type;
} dependency;

char *python_extensions[2] = { "py", "pyc" };
char *perl_extensions[2]   = { "pl", "pm" };

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

void display_dependencies(struct list *d){
	dependency *dep;

	list_first_item(d);
	while((dep = list_next_item(d))){
		printf("%s %s %d %d\n", dep->original_name, dep->final_name, dep->depth, dep->type);
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
		if(!strcmp(perl_extensions[j], extension))
			return PERL;
	}

	return UNKNOWN;
}

file_type shebang_known(const char *filename){
	return 0;
}

file_type unix_file_known(const char *filename){
	return 0;
}

file_type find_driver_for(const char *name){
	file_type type = UNKNOWN;

	if((type = file_extension_known(name))){}
	else if((type = shebang_known(name))){}
	else if((type = unix_file_known(name))){}

	return type;
}

void find_dependencies_for(dependency *dep){
	pid_t pid;
	int pipefd[2];
	pipe(pipefd);

	switch ( pid = fork() ){
	case -1:
		fprintf( stderr, "Cannot fork. Exiting...\n" );
		exit(1);
	case 0:
		/* Child process */
		close(pipefd[0]);
		dup2(pipefd[1], 1);
		close(pipefd[1]);

		char * const args[3] = { "locating dependencies" , dep->original_name, NULL };
		switch ( dep->type ){
			case PYTHON:
				execvp("./python_linker", args); 
			case PERL: 
				execvp("./perl_linker", args);
			case UNKNOWN:
				break;
		}
		exit(1);
	default:
		/* Parent process */
		close(pipefd[1]);
		char next;
		char *buffer = (char *) malloc(sizeof(char));
		int size = 0;
		while (read(pipefd[0], &next, sizeof(next)) != 0){
			switch ( next ){
				case '\n':
					buffer = realloc(buffer, size+1);
					*(buffer+size) = '\0';
					dependency *new_dependency = (dependency *) malloc(sizeof(dependency));
					new_dependency->original_name = buffer;
					printf("%s\n", new_dependency->original_name);
					size = 0;
					buffer = NULL;
					break;
				case '\0':
					break;
				default:
					buffer = realloc(buffer, size+1);
					*(buffer+size) = next;
					size++;
			}
		}
	}
	// Driver returns list of first order formal dependencies as absolute file paths
	// Print each file path returned
}

void find_dependencies(struct list *d){
	dependency *dep;
	list_first_item(d);
	while((dep = list_next_item(d))){
		find_dependencies_for(dep);
	}
}

void find_drivers(struct list *d){
	dependency *dep;
	list_first_item(d);
	while((dep = list_next_item(d))){
		dep->type = find_driver_for(dep->final_name);
	}
}

int main(void){
	char *output = "output_dir";
	char *input = "test.mf";

	struct list *dependencies;
	dependencies = list_create();

	initialize(output, input, dependencies);

	find_drivers(dependencies);
	display_dependencies(dependencies);
	
	find_dependencies(dependencies);

	return 0;
}
