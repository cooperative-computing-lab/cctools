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


static struct itable *process_table = 0;

FILE *sh_popen(const char *command)
{
	pid_t pid;
	int argc;
	char **argv;
	int fds[2];
	char cmd[4096];
	int result;

	strcpy(cmd, command);

	if(string_split_quotes(cmd, &argc, &argv) < 1){
		return 0;
	}
	
	argv = malloc(sizeof(char*)*4);
	argv[0]="sh";
	argv[1]="-c";
	argv[2] = (char*)command;
	argv[3] = NULL;
	argc = 4;

	if(argc < 1){
		return 0;
	}

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

		itable_insert(process_table, fds[0], (void *) (PTRINT_T) pid);
		return fdopen(fds[0], "r");

	} else if(pid == 0) {

		int i;

		close(0);
		i = dup2(fds[1], STDOUT_FILENO); if(i < 0) perror("Error Dup2 stdout from SH_popen.");
		i = dup2(fds[1], STDERR_FILENO); if(i < 0) perror("Error Dup2 stderr SH_popen.");
		close(fds[1]);
		close(fds[0]);

		i = execvp(argv[0], argv);
		if (i<0) perror("Error in execvp from SH_popen");
		_exit(1);

	} else {
		free(argv);
		return 0;
	}
}

int sh_pclose(FILE * file)
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

int sh_system(const char* command) {
	int pid;
	int res = 0;
	pid = fork();
	if (pid == 0) {//child
		char* argv[4];
		argv[0] = "sh";
		argv[1] = "-c";
		argv[2] = (char*)command;
		argv[3] = NULL;

		res = execvp(argv[0], argv);
		if (res < 0) {
			perror("SH_system past execvp: ");
		}
	} else if (pid > 0) {//parent
		struct process_info* pres;
		while((pres = process_waitpid(pid, 0)) == 0);
		if (WIFEXITED(pres->status)) {
			return WEXITSTATUS(pres->status);
		}
	} else {
		fprintf(stderr, "error in forking\n");
	}
	return -17;

}
