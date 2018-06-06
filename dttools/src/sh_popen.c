#include "sh_popen.h"
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <errno.h>
#include "stringtools.h"
#include "itable.h"
#include "debug.h"
#include "xxmalloc.h"

static struct itable* process_table = 0;

FILE* sh_popen(char* command)
{
	pid_t pid;
	int argc;
	char** argv;
	int fds[2];
	char* cmd;
	int result;
	
	cmd = xxstrdup(command);

	if(string_split_quotes(cmd, &argc, &argv) < 1){
		debug(D_ERROR,"Empty command to sh_popen");
		return 0;
	}
	free(argv); //string_split_quotes doesn't seem to create memory for inner items,
	
	argv = malloc(sizeof(char*)*4);
	argv[0]="/bin/sh";
	argv[1]="-c";
	argv[2] = command;
	argv[3] = NULL;
	argc = 4;

	result = pipe(fds);
	if(result < 0) {
		
		free(argv);
		return 0;
	}

	pid = fork();
	if(pid > 0) {
		free(argv);
		close(fds[1]);

		if(!process_table)
			process_table = itable_create(0);

		itable_insert(process_table, fds[0], (void*) (PTRINT_T) pid);
		return fdopen(fds[0], "r");

	} else if(pid == 0) {

		int i;

		i = dup2(fds[1], STDOUT_FILENO); if(i < 0) perror("Error Dup2 stdout from SH_popen.");
		i = dup2(fds[1], STDERR_FILENO); if(i < 0) perror("Error Dup2 stderr SH_popen.");
		close(fds[1]);
		close(fds[0]);

		i = execvp(argv[0], argv);
		if (i<0) debug(D_ERROR,"Error in execvp from SH_popen: %s",strerror(errno));
		_exit(1);

	} else {
		free(argv);
		return 0;
	}
}

int sh_pclose(FILE* file)
{
	pid_t pid;
	struct process_info* result;

	pid = (PTRINT_T) itable_remove(process_table, fileno(file));

	fclose(file);

	while(1) {
		while((result = process_waitpid(pid,0)) == 0);
		if(WIFEXITED(result->status)) {
			return WEXITSTATUS(result->status);
		} else {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		}
	}

	errno = ECHILD;
	return -1;
}

int sh_system(char* command) {
	int pid;
	int res = 0;
	pid = fork();
	if (pid == 0) {//child
		char* argv[4];
		argv[0] = "/bin/sh";
		argv[1] = "-c";
		argv[2] = command;
		argv[3] = NULL;

		res = execvp(argv[0], argv);
		if (res < 0) {
			debug(D_ERROR,"SH_system past execvp: %s",strerror(errno));
			_exit(1);
		}
	} else if (pid > 0) {//parent
		struct process_info* pres;
		while((pres = process_waitpid(pid, 0)) == 0);
		if (WIFEXITED(pres->status)) {
			return WEXITSTATUS(pres->status);
		}
	} else {
		debug(D_ERROR, "error in forking\n");
	}
	return -1;

}
