#include "dpopen.h"

#include <unistd.h>
#include <sys/wait.h>

#include <errno.h>
#include <limits.h>
#include <signal.h>

extern char **environ;

pid_t dpopen(const char *command, FILE ** in, FILE ** out)
{
	pid_t pid;
	int i;
	int result;
	int stdin_fd[2];
	int stdout_fd[2];

	result = pipe(stdin_fd);
	if(result < 0)
		return 0;

	result = pipe(stdout_fd);
	if(result < 0) {
		close(stdin_fd[0]);
		close(stdin_fd[1]);
		return 0;
	}

	pid = fork();
	if(pid > 0) {
		close(stdin_fd[0]);
		close(stdout_fd[1]);

		*in = fdopen(stdin_fd[1], "w");
		if(*in == NULL) {
			close(stdin_fd[1]);
			close(stdout_fd[0]);
			kill(pid, SIGKILL);
			return 0;
		}
		*out = fdopen(stdout_fd[0], "r");
		if(*out == NULL) {
			fclose(*in);
			close(stdout_fd[0]);
			kill(pid, SIGKILL);
			return 0;
		}

		return pid;
	} else if(pid == 0) {
		close(stdin_fd[1]);
		close(stdout_fd[0]);

		close(STDIN_FILENO);
		dup2(stdin_fd[0], STDIN_FILENO);
		close(STDOUT_FILENO);
		dup2(stdout_fd[1], STDOUT_FILENO);
		close(STDERR_FILENO);
		dup2(stdout_fd[1], STDERR_FILENO);

		for(i = STDERR_FILENO + 1; i < _POSIX_OPEN_MAX; i++)
			close(i);

		return execl("/bin/sh", "sh", "-c", command, (char *) 0);
	} else {
		close(stdin_fd[0]);
		close(stdin_fd[1]);
		close(stdout_fd[0]);
		close(stdout_fd[1]);
		return 0;
	}
}

int dpclose(FILE * in, FILE * out, pid_t pid)
{
	if(in)
		fclose(in);
	if(out)
		fclose(out);

	while(1) {
		int status;
		int result = waitpid(pid, &status, 0);
		if(result == pid) {
			return WIFEXITED(status) ? status : -1;
		} else if(errno == EINTR) {
			continue;
		} else {
			return -1;	/* errno is set */
		}
	}
}

/* vim: set noexpandtab tabstop=8: */
