#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <limits.h>
#include <string.h>

#include "hash_table.h"

#define MAKEFLOW_PATH "makeflow"
#define MAKEFLOW_BUNDLE_FLAG "-b"

typedef enum {PERL, PYTHON, UNKNOWN} file_type;

void initialize( char *output_directory, char *input_file, struct hash_table *ht){
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

void display_names(struct hash_table *ht){
	char *key;
	char *value;

	hash_table_firstkey(ht);
	while(hash_table_nextkey(ht, &key,(void **) &value)){
		printf("%s -> %s\n", key, value);
	}
}

const char *filename_extension(const char *filename) {
	const char *dot = strrchr(filename, '.');
	if(!dot || dot == filename) return "";
	return dot + 1;
}

int file_extension_known(const char *filename, file_type *my_file){
	const char *extension = filename_extension(filename);
	char *python_extensions[2] = { "py", "pyc" };
	int j;
	for(j=0; j< 2; j++){
		printf("%s -> %s ?= %s\n", filename, extension, python_extensions[j]);
		if(!strcmp(python_extensions[j], extension)){
			*my_file = PYTHON;
			return 1;
		}
	}

	return 0;
}

int shebang_known(const char *filename, file_type *my_file){
	return 0;
}

int unix_file_known(const char *filename, file_type *my_file){
	return 0;
}

file_type find_driver_for(const char *name){
	file_type *type;
	*type = UNKNOWN;

	if(file_extension_known(name, type)){}
	else if(shebang_known(name, type)){}
	else if(unix_file_known(name, type)){}

	file_type my_file = *type;
	return my_file;
}

void find_drivers(struct hash_table *names, struct hash_table *drivers){
	char *key;
	char *value;
	file_type my_type;
	hash_table_firstkey(names);
	while(hash_table_nextkey(names, &key, (void **) &value)){
		my_type = find_driver_for(value);
		hash_table_insert(drivers, value, (void **) my_type);
	}
}

int main(void){
	char *output = "output_dir";
	char *input = "test.mf";

	struct hash_table *names;
	names = hash_table_create(0, NULL);

	struct hash_table *drivers;
	drivers = hash_table_create(0, NULL);
	
	initialize(output, input, names);

	find_drivers(names, drivers);
	
	display_names(drivers);

	return 0;
}
