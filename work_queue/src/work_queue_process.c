
#include "work_queue_process.h"
#include "work_queue.h"

#include "debug.h"
#include "errno.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <stdio.h>
#include <sys/resource.h>

struct work_queue_process * work_queue_process_create( struct work_queue_task *t )
{
	struct work_queue_process *p = malloc(sizeof(*p));
	memset(p,0,sizeof(*p));
	p->task = t;
	return p;
}

void work_queue_process_delete( struct work_queue_process *p )
{
	if(p->output_fd) {
		close(p->output_fd);
		unlink(p->output_file_name);
	}

	free(p);
}

static const char task_output_template[] = "./worker.stdout.XXXXXX";

pid_t work_queue_process_execute(const char *cmd, struct work_queue_process *ti)
{
	char working_dir[1024];
	fflush(NULL); /* why is this necessary? */
	
	sprintf(working_dir, "t.%d", ti->task->taskid);
	ti->output_file_name = strdup(task_output_template);
	ti->output_fd = mkstemp(ti->output_file_name);
	if (ti->output_fd == -1) {
		debug(D_WQ, "Could not open worker stdout: %s", strerror(errno));
		return 0;
	}

	ti->execution_start = timestamp_get();

	ti->pid = fork();
	
	if(ti->pid > 0) {
		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task(). 
		setpgid(ti->pid, 0); 
		
		debug(D_WQ, "started process %d: %s", ti->pid, cmd);
		return ti->pid;
	} else if(ti->pid < 0) {
		debug(D_WQ, "couldn't create new process: %s\n", strerror(errno));
		unlink(ti->output_file_name);
		close(ti->output_fd);
		return ti->pid;
	} else {
		if(chdir(working_dir)) {
			fatal("could not change directory into %s: %s", working_dir, strerror(errno));
		}
		
		int fd = open("/dev/null", O_RDONLY);
		if (fd == -1) fatal("could not open /dev/null: %s", strerror(errno));
		int result = dup2(fd, STDIN_FILENO);
		if (result == -1) fatal("could not dup /dev/null to stdin: %s", strerror(errno));

		result = dup2(ti->output_fd, STDOUT_FILENO);
		if (result == -1) fatal("could not dup pipe to stdout: %s", strerror(errno));

		result = dup2(ti->output_fd, STDERR_FILENO);
		if (result == -1) fatal("could not dup pipe to stderr: %s", strerror(errno));

		close(ti->output_fd);

		execlp("sh", "sh", "-c", cmd, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	}
	return 0;
}

