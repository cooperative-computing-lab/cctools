
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

struct dataswarm_process *dataswarm_process_create( struct dataswarm_task *task, struct dataswarm_worker *w )
{
	struct dataswarm_process *p = malloc(sizeof(*p));
	memset(p,0,sizeof(*p));

	p->task = task;
	p->state = DATASWARM_PROCESS_READY;

	/* create a unique directory for this task */
	p->sandbox = string_format("%s/task/%s/sandbox", w->workspace, p->task->taskid );
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

	// XXX move sandbox to deleting dir.

	if(!dataswarm_process_isdone(p)) {
		dataswarm_process_kill(p);
		while(!dataswarm_process_isdone(p)) {
			sleep(1);
		}
	}

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

static int flags_to_unix_mode( dataswarm_flags_t flags )
{
	if(flags==DATASWARM_FLAGS_READ) {
		return O_RDONLY;
	}

	if(flags&DATASWARM_FLAGS_APPEND) {
		return O_RDWR | O_CREAT | O_APPEND;
	} else {
		return O_RDWR | O_CREAT | O_TRUNC;
	}
}

static int setup_mount( struct dataswarm_mount *m, struct dataswarm_process *p, struct dataswarm_worker *w )
{
	const char *mode;
	if(m->flags&(DATASWARM_FLAGS_WRITE|DATASWARM_FLAGS_APPEND)) {
		mode = "rw";
	} else {
		mode = "ro";
	}

	char *blobpath = string_format("%s/blob/%s/%s/data",w->workspace,mode,m->uuid);

	if(m->type==DATASWARM_MOUNT_PATH) {
		int r = symlink(blobpath,m->path);
		if(r<0) {
			debug(D_DATASWARM,"couldn't symlink %s -> %s: %s",m->path,blobpath,strerror(errno));
			free(blobpath);
			return 0;
		}
		free(blobpath);
	} else if(m->type==DATASWARM_MOUNT_FD) {
		int fd = open(blobpath,flags_to_unix_mode(m->flags),0666);
		if(fd<0) {
			debug(D_DATASWARM,"couldn't open %s: %s",blobpath,strerror(errno));
			free(blobpath);
			return 0;
		}
		dup2(fd,m->fd);
		close(fd);
		free(blobpath);
	} else {
		free(blobpath);
		return 0;
	}

	return 1;
}

static int setup_namespace( struct dataswarm_process *p, struct dataswarm_worker *w )
{
	struct dataswarm_mount *m;

	for(m=p->task->mounts;m;m=m->next) {
		if(!setup_mount(m,p,w)) return 0;
	}

	return 1;
}

int dataswarm_process_start( struct dataswarm_process *p, struct dataswarm_worker *w )
{
	/*
	Before forking a process, it is necessary to flush all standard I/O stream,
	otherwise buffered data is carried into the forked child process and can
	result in confusion.
	*/

	fflush(NULL);

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
		p->state = DATASWARM_PROCESS_RUNNING;
		return 1;

	} else if(p->pid < 0) {

		debug(D_WQ, "couldn't create new process: %s\n", strerror(errno));
		return 0;

	} else {
		if(chdir(p->sandbox)) {
			fatal("could not change directory into %s: %s", p->sandbox, strerror(errno));
		}

		// Check errors on these.
		setup_namespace(p,w);
		clear_environment();
		specify_resources_vars(p);
		export_environment(p);

		execl("/bin/sh", "sh", "-c", p->task->command, (char *) 0);
		_exit(127);	// Failed to execute the cmd.
	}

	return 1;
}

int dataswarm_process_isdone( struct dataswarm_process *p )
{
	if(p->state==DATASWARM_PROCESS_RUNNING) {
		// XXX get the rusage too
		pid_t pid = waitpid(p->pid,&p->unix_status,WNOHANG);
		if(pid==p->pid) {
			p->state = DATASWARM_PROCESS_DONE;
			p->execution_end = timestamp_get();
			return 1;
		}
	} else if(p->state==DATASWARM_PROCESS_DONE) {
		return 1;
	} else {
		return 0;
	}

	return 0;
}


void dataswarm_process_kill(struct dataswarm_process *p)
{
	if(p->state!=DATASWARM_PROCESS_RUNNING) return;

	//make sure a few seconds have passed since child process was created to avoid sending a signal
	//before it has been fully initialized. Else, the signal sent to that process gets lost.
	timestamp_t elapsed_time_execution_start = timestamp_get() - p->execution_start;

	if(elapsed_time_execution_start / 1000000 < 3)
		sleep(3 - (elapsed_time_execution_start / 1000000));

	debug(D_WQ, "terminating task %s pid %d", p->task->taskid, p->pid);

	// Send signal to process group of child which is denoted by -ve value of child pid.
	// This is done to ensure delivery of signal to processes forked by the child.
	kill((-1 * p->pid), SIGKILL);

	// Note that we still must wait for the process to be done before deleting the process.
}
