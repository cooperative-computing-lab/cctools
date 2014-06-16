
#include "work_queue_process.h"
#include "work_queue.h"

#include "debug.h"
#include "errno.h"
#include "stringtools.h"
#include "create_dir.h"
#include "delete_dir.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

#include <sys/resource.h>
#include <sys/wait.h>

struct work_queue_process * work_queue_process_create( int taskid )
{
	struct work_queue_process *p = malloc(sizeof(*p));
	memset(p,0,sizeof(*p));
	p->task = work_queue_task_create(0);
	p->task->taskid = taskid;
	p->sandbox = string_format("t.%d",taskid);

	if(!create_dir(p->sandbox,0777)) {
		work_queue_process_delete(p);
		return 0;
	}

	
	return p;
}

void work_queue_process_delete( struct work_queue_process *p )
{
	if(p->task) work_queue_task_delete(p->task);

	if(p->output_fd) {
		close(p->output_fd);
	}

	if(p->output_file_name) {
		unlink(p->output_file_name);
		free(p->output_file_name);
	}

	if(p->sandbox) {
		delete_dir(p->sandbox);
		free(p->sandbox);
	}

	free(p);
}

static const char task_output_template[] = "./worker.stdout.XXXXXX";

pid_t work_queue_process_execute( struct work_queue_process *p )
{
	fflush(NULL); /* why is this necessary? */
	
	p->output_file_name = strdup(task_output_template);
	p->output_fd = mkstemp(p->output_file_name);
	if (p->output_fd == -1) {
		debug(D_WQ, "Could not open worker stdout: %s", strerror(errno));
		return 0;
	}

	p->execution_start = timestamp_get();

	p->pid = fork();
	
	if(p->pid > 0) {
		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task(). 
		setpgid(p->pid, 0); 
		
		debug(D_WQ, "started process %d: %s", p->pid, p->task->command_line);
		return p->pid;
	} else if(p->pid < 0) {
		debug(D_WQ, "couldn't create new process: %s\n", strerror(errno));
		unlink(p->output_file_name);
		close(p->output_fd);
		return p->pid;
	} else {
		if(chdir(p->sandbox)) {
			fatal("could not change directory into %s: %s", p->sandbox, strerror(errno));
		}
		
		int fd = open("/dev/null", O_RDONLY);
		if (fd == -1) fatal("could not open /dev/null: %s", strerror(errno));
		int result = dup2(fd, STDIN_FILENO);
		if (result == -1) fatal("could not dup /dev/null to stdin: %s", strerror(errno));

		result = dup2(p->output_fd, STDOUT_FILENO);
		if (result == -1) fatal("could not dup pipe to stdout: %s", strerror(errno));

		result = dup2(p->output_fd, STDERR_FILENO);
		if (result == -1) fatal("could not dup pipe to stderr: %s", strerror(errno));

		close(p->output_fd);

		execlp("sh", "sh", "-c", p->task->command_line, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	}
	return 0;
}

void  work_queue_process_kill( struct work_queue_process *p )
{
	//make sure a few seconds have passed since child process was created to avoid sending a signal 
	//before it has been fully initialized. Else, the signal sent to that process gets lost.	
	timestamp_t elapsed_time_execution_start = timestamp_get() - p->execution_start;
	
	if (elapsed_time_execution_start/1000000 < 3)
		sleep(3 - (elapsed_time_execution_start/1000000));	
	
	debug(D_WQ, "terminating task %d pid %d",p->task->taskid,p->pid);

	// Send signal to process group of child which is denoted by -ve value of child pid.
	// This is done to ensure delivery of signal to processes forked by the child. 
	kill((-1*p->pid), SIGKILL);
	
	// Reap the child process to avoid zombies.
	waitpid(p->pid, NULL, 0);
}

