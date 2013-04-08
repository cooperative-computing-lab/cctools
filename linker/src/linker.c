#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <limits.h>

#define MAKEFLOW_PATH "makeflow"
#define MAKEFLOW_BUNDLE_FLAG "-b"
#define MAKEFLOW_FILE_FLAG "-f "

int main(void){
	pid_t pid;

	char *output = "output_dir";
	char *input = "test.mf";
	char expanded_input[PATH_MAX];
	realpath(input, expanded_input);

	pid = fork();
	if ( pid < 0 ) {
		fprintf(stderr, "Cannot fork. Exiting...\n");
		exit(1);
	}
	if ( pid == 0 ) {
		/* Child process */
		char * const args[6] = { "linking makeflow" , MAKEFLOW_BUNDLE_FLAG, output, MAKEFLOW_FILE_FLAG, expanded_input, NULL};

		execvp(MAKEFLOW_PATH, args); 
	}
	else {
		/* Parent process */
	}
	return 0;
}
