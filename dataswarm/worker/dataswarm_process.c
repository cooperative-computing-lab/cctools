
#include "dataswarm_process.h"

#include "debug.h"
#include "errno.h"
#include "macros.h"
#include "stringtools.h"
#include "create_dir.h"
#include "delete_dir.h"
#include "list.h"
#include "path.h"
#include "xxmalloc.h"
#include "jx.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <dirent.h>

#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>

extern const char *UUID_TO_LOCAL_PATH ( const char *uuid );

struct dataswarm_process *dataswarm_process_create( struct dataswarm_task *task )
{
	struct dataswarm_process *p = malloc(sizeof(*p));
	memset(p,0,sizeof(*p));

	p->task = task;

	/* create a unique directory for this task */
	char *cwd = path_getcwd();
	p->sandbox = string_format("%s/task.%s", cwd, p->task->taskid );
	free(cwd);
	if(!create_dir(p->sandbox, 0777)) goto failure;

	/* inside the sandbox, make a unique tempdir for this task */
	p->tmpdir = string_format("%s/cctools-temp.XXXXXX", p->sandbox);
	if(mkdtemp(p->tmpdir) == NULL) goto failure;
	if(chmod(p->tmpdir, 0777) != 0) goto failure;

	return p;

	failure:
	dataswarm_process_delete(p);
	return 0;
}

void dataswarm_process_delete(struct dataswarm_process *p)
{
	if(!p) return;
	// don't free taskid, it's a circular link
	if(p->sandbox) {
		delete_dir(p->sandbox);
		free(p->sandbox);
	}
	if(p->tmpdir) free(p->tmpdir);
	free(p);
}

static void clear_environment() {
	/* Clear variables that we really want the user to set explicitly.
	 * Ideally, we would start with a clean environment, but certain variables,
	 * such as HOME are seldom set explicitly, and some executables rely on them.
	*/

	unsetenv("DISPLAY");
}

static void export_environment( struct dataswarm_process *p )
{
	if(p->task->environment) jx_export(p->task->environment);

	/* we set TMPDIR last on purpose. We do not want a task writing to some other tmp dir. */

	if(p->tmpdir) {
		setenv("TMPDIR", p->tmpdir, 1);
		setenv("TEMP",   p->tmpdir, 1);
		setenv("TMP",    p->tmpdir, 1);
	}
}

static void specify_integer_env_var( struct dataswarm_process *p, const char *name, int64_t value)
{
	char *value_str = string_format("%" PRId64, value);
	setenv(name,value_str,1);
	free(value_str);
}

static void specify_resources_vars(struct dataswarm_process *p)
{
	if(p->task->resources->cores > 0) specify_integer_env_var(p, "CORES", p->task->resources->cores);
	if(p->task->resources->memory > 0) specify_integer_env_var(p, "MEMORY", p->task->resources->memory);
	if(p->task->resources->disk > 0) specify_integer_env_var(p, "DISK", p->task->resources->disk);
}

static int setup_namespace( struct dataswarm_process *p )
{
	struct dataswarm_mount *m;

	for(m=p->task->mounts;m;m=m->next) {
		const char *uuidpath = UUID_TO_LOCAL_PATH(m->uuid);
		if(m->type==DATASWARM_MOUNT_PATH) {
			symlink(uuidpath,m->path);
			// check errors here
		} else if(m->type==DATASWARM_MOUNT_FD) {
			// need to set open flags appropriately here
			int fd = open(uuidpath,O_RDONLY,0);
			// check errors here
			dup2(fd,m->fd);
			close(fd);
		} else {
			// error on invalid type
		}
	}

	return 1;
}

pid_t dataswarm_process_execute( struct dataswarm_process *p )
{
	fflush(NULL);		/* why is this necessary? */

	p->execution_start = timestamp_get();
	p->pid = fork();

	/* should set up stdin/stdout/stderr here */
	/* but we don't have a specification for that yet */

	if(p->pid > 0) {

		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task().
		setpgid(p->pid, 0);
		debug(D_WQ, "started process %d: %s", p->pid, p->task->command);
		return p->pid;

	} else if(p->pid < 0) {

		debug(D_WQ, "couldn't create new process: %s\n", strerror(errno));
		return p->pid;

	} else {
		if(chdir(p->sandbox)) {
			fatal("could not change directory into %s: %s", p->sandbox, strerror(errno));
		}

		// Check errors on these.
		setup_namespace(p);
		clear_environment();
		specify_resources_vars(p);
		export_environment(p);

		execl("/bin/sh", "sh", "-c", p->task->command, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	}
	return 0;
}

void dataswarm_process_kill(struct dataswarm_process *p)
{
	//make sure a few seconds have passed since child process was created to avoid sending a signal
	//before it has been fully initialized. Else, the signal sent to that process gets lost.
	timestamp_t elapsed_time_execution_start = timestamp_get() - p->execution_start;

	if(elapsed_time_execution_start / 1000000 < 3)
		sleep(3 - (elapsed_time_execution_start / 1000000));

	debug(D_WQ, "terminating task %s pid %d", p->task->taskid, p->pid);

	// Send signal to process group of child which is denoted by -ve value of child pid.
	// This is done to ensure delivery of signal to processes forked by the child.
	kill((-1 * p->pid), SIGKILL);

	// Reap the child process to avoid zombies.
	waitpid(p->pid, NULL, 0);
}
