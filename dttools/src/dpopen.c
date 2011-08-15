#include <dpopen.h>

#include <unistd.h>
#include <sys/wait.h>

#include <errno.h>
#include <limits.h>
#include <signal.h>

extern char **environ;

pid_t dpopen(const char *command, FILE ** in, FILE ** out)
{
	return multi_popen(command, in, out, out);
}

pid_t multi_popen(const char *command, FILE ** in, FILE ** out, FILE ** err)
{
	pid_t pid;
	int i;
	int result;
	int stdin_fd[2];
	int stdout_fd[2];
	int stderr_fd[2];

	if(in) {
		result = pipe(stdin_fd);
		if(result < 0)
			return 0;
	}

	if(out) {
		result = pipe(stdout_fd);
		if(result < 0) {
			if(in) {
				close(stdin_fd[0]);
				close(stdin_fd[1]);
			}
			return 0;
		}
	}
	
	if(err && err != out) {
		result = pipe(stderr_fd);
		if(result < 0) {
			if(in) {
				close(stdin_fd[0]);
				close(stdin_fd[1]);
			}
			if(out) {
				close(stdout_fd[0]);
				close(stdout_fd[1]);
			}
			return 0;
		}
	}

	pid = fork();
	if(pid > 0) {
		if(err == out)
			err = NULL;
	
		if(in)
			close(stdin_fd[0]);
		if(out)
			close(stdout_fd[1]);
		if(err)
			close(stderr_fd[1]);

		if(in) {
			*in = fdopen(stdin_fd[1], "w");
		}
		if(out) {
			*out = fdopen(stdout_fd[0], "r");
		}
		if(err) {
			*err = fdopen(stderr_fd[0], "r");
		}

		if( (in && *in == NULL) || (out && *out == NULL) || (err && *err == NULL) ) {
		
			if(in) {
				if(*in != NULL)
					fclose(*in);
				else
					close(stdin_fd[1]);
			}
			if(out) {
				if(*out != NULL)
					fclose(*out);
				else
					close(stdout_fd[0]);
			}
			if(err) {
				if(*err != NULL)
					fclose(*err);
				else
					close(stderr_fd[0]);
			}
			kill(pid, SIGKILL);
			return 0;
		}

		return pid;
	} else if(pid == 0) {
		if(in) {
			close(stdin_fd[1]);
		}
		if(out) {
			close(stdout_fd[0]);
		}
		if(err) {
			if(err != out)
				close(stderr_fd[0]);
			else
				stderr_fd[1] = stdout_fd[1];
		}

		if(in) {
			close(STDIN_FILENO);
			dup2(stdin_fd[0], STDIN_FILENO);
		}
		if(out) {
			close(STDOUT_FILENO);
			dup2(stdout_fd[1], STDOUT_FILENO);
		}
		if(err) {
			close(STDERR_FILENO);
			dup2(stderr_fd[1], STDERR_FILENO);
		}

		for(i = STDERR_FILENO + 1; i < _POSIX_OPEN_MAX; i++)
			close(i);

		return execl("/bin/sh", "sh", "-c", command, (char *) 0);
	} else {
		if(in) {
			close(stdin_fd[0]);
			close(stdin_fd[1]);
		}
		if(out) {
			close(stdout_fd[0]);
			close(stdout_fd[1]);
		}
		if(err && err != out) {
			close(stderr_fd[0]);
			close(stderr_fd[1]);
		}
		return 0;
	}
}

int dpclose(FILE * in, FILE * out, pid_t pid)
{
	return multi_pclose(in, out, NULL, pid);
}

int multi_pclose(FILE * in, FILE * out, FILE * err, pid_t pid)
{
	if(in)
		fclose(in);
	if(out)
		fclose(out);
	if(err)
		fclose(err);

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
