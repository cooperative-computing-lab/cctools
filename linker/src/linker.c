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

struct dependency{
	char *original_name;
	char *final_name;
	struct dependency *parent;
	struct dependency *superparent;
	int  depth;
	file_type type;
};

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
		int depth = 1;
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
					struct dependency *new_dependency = (struct dependency *) malloc(sizeof(struct dependency));
					new_dependency->original_name = original_name;
					new_dependency->final_name = buffer;
					new_dependency->depth = depth;
					new_dependency->parent = NULL;
					new_dependency->superparent = NULL;
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
	struct dependency *dep;

	list_first_item(d);
	while((dep = list_next_item(d))){
		if(dep->parent){
			printf("%s %s %d %d %s %s\n", dep->original_name, dep->final_name, dep->depth, dep->type, dep->parent->final_name, dep->superparent->final_name);
		} else {
			printf("%s %s %d %d\n", dep->original_name, dep->final_name, dep->depth, dep->type);
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

struct list *find_dependencies_for(struct dependency *dep){
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
		char * const args[3] = { "locating dependencies" , dep->final_name, NULL };
		switch ( dep->type ){
			case PYTHON:
				execvp("./python_driver", args);
			case UNKNOWN:
				break;
		}
		exit(1);
	default:
		/* Parent process */
		close(pipefd[1]);
		char next;
		char *buffer = (char *) malloc(sizeof(char));
		char *original_name;
		int size = 0;
		int depth = dep->depth  + 1;
		struct list *new_deps = list_create();

		while (read(pipefd[0], &next, sizeof(next)) != 0){
			switch ( next ){
				case ' ':
					original_name = (char *)realloc((void *)buffer,size+1);
					*(original_name+size) = '\0';
					buffer = (char *) malloc(sizeof(char));
					size = 0;
					break;
				case '\n':
					buffer = realloc(buffer, size+1);
					*(buffer+size) = '\0';
					struct dependency *new_dependency = (struct dependency *) malloc(sizeof(struct dependency));
					new_dependency->original_name = original_name;
					new_dependency->final_name = buffer;
					new_dependency->depth = depth;
					new_dependency->parent = dep;
					if(dep->superparent){
						new_dependency->superparent = dep->superparent;
					} else {
						new_dependency->superparent = dep;
					}
					list_push_tail(new_deps, new_dependency);
					size = 0;
					buffer = NULL;
					break;
				default:
					buffer = realloc(buffer, size+1);
					*(buffer+size) = next;
					size++;
			}
		}
		return new_deps;
	}
}

void find_dependencies(struct list *d){
	struct dependency *dep;
	struct list *new;

	list_first_item(d);
	while((dep = list_next_item(d))){
		new = find_dependencies_for(dep);
		list_first_item(new);
		while((dep = list_next_item(new))){
			dep->type = find_driver_for(dep->final_name);
			list_push_tail(d, dep);
		}
		list_delete(new);
		new = NULL;
	}
}

void find_drivers(struct list *d){
	struct dependency *dep;
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
	find_dependencies(dependencies);

	display_dependencies(dependencies);

	return 0;
}
