/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_process.h"
#include "vine_manager.h"
#include "vine_gpus.h"
#include "vine_protocol.h"
#include "vine_coprocess.h"
#include "vine_sandbox.h"

#include "vine_file.h"
#include "vine_mount.h"

#include "debug.h"
#include "errno.h"
#include "macros.h"
#include "stringtools.h"
#include "create_dir.h"
#include "list.h"
#include "path.h"
#include "xxmalloc.h"
#include "trash.h"
#include "link.h"
#include "timestamp.h"
#include "domain_name.h"
#include "full_io.h"
#include "hash_table.h"
#include "list.h"

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

/*
Create the task sandbox directory.
Create temporary directories inside as well.
*/

extern char * workspace;

extern struct list *duty_list;
extern struct hash_table *duty_ids;

static int create_sandbox_dir( struct vine_process *p )
{
	p->cache_dir = string_format("%s/cache",workspace);
  	p->sandbox = string_format("%s/t.%d", workspace,p->task->task_id);

	if(!create_dir(p->sandbox, 0777)) return 0;

	char tmpdir_template[1024];
	string_nformat(tmpdir_template, sizeof(tmpdir_template), "%s/cctools-temp-t.%d.XXXXXX", p->sandbox, p->task->task_id);
	if(mkdtemp(tmpdir_template) == NULL) {
		return 0;
	}

	p->tmpdir  = xxstrdup(tmpdir_template);
	if(chmod(p->tmpdir, 0777) != 0) {
		return 0;
	}

	return 1;
}

/*
Create a vine_process and all of the information necessary for invocation.
However, do not allocate substantial resources at this point.
*/

struct vine_process *vine_process_create(struct vine_task *vine_task )
{
	struct vine_process *p = malloc(sizeof(*p));
	memset(p, 0, sizeof(*p));
	p->task = vine_task;
	p->coprocess = NULL;
	if(!create_sandbox_dir(p)) {
		vine_process_delete(p);
		return 0;
	}
	return p;
}


void vine_process_delete(struct vine_process *p)
{
	if(p->task)
		vine_task_delete(p->task);

	if(p->output_fd) {
		close(p->output_fd);
	}

	if(p->output_file_name) {
		trash_file(p->output_file_name);
		free(p->output_file_name);
	}

	if(p->sandbox) {
		trash_file(p->sandbox);
		free(p->sandbox);
	}

	if(p->tmpdir)
		free(p->tmpdir);

	if(p->cache_dir)
		free(p->cache_dir);
	
	free(p);
}

static void clear_environment() {
	/* Clear variables that we really want the user to set explicitly.
	 * Ideally, we would start with a clean environment, but certain variables,
	 * such as HOME are seldom set explicitly, and some executables rely on them.
	*/

	unsetenv("DISPLAY");

}

static void export_environment( struct vine_process *p )
{
	struct list *env_list = p->task->env_list;
	char *name;

	LIST_ITERATE(env_list,name) {
		char *value = strchr(name,'=');
		if(value) {
			*value = 0;
			setenv(name,value+1,1);
			*value='=';
		}
		else {
			/* Without =, we remove the variable */
			unsetenv(name);
		}
	}

	/* we set TMPDIR after env_list on purpose. We do not want a task writing
	 * to some other tmp dir. */
	if(p->tmpdir) {
		setenv("TMPDIR", p->tmpdir, 1);
		setenv("TEMP",   p->tmpdir, 1);
		setenv("TMP",    p->tmpdir, 1);
	}
}

static void set_integer_env_var( struct vine_process *p, const char *name, int64_t value) {
	char *value_str = string_format("%" PRId64, value);
	vine_task_set_env_var(p->task, name, value_str);
	free(value_str);
}

static void set_resources_vars(struct vine_process *p) {
	if(p->task->resources_requested->cores > 0) {
		set_integer_env_var(p, "CORES", p->task->resources_requested->cores);
		set_integer_env_var(p, "OMP_NUM_THREADS", p->task->resources_requested->cores);
	}

	if(p->task->resources_requested->memory > 0) {
		set_integer_env_var(p, "MEMORY", p->task->resources_requested->memory);
	}

	if(p->task->resources_requested->disk > 0) {
		set_integer_env_var(p, "DISK", p->task->resources_requested->disk);
	}

	if(p->task->resources_requested->gpus > 0) {
		set_integer_env_var(p, "GPUS", p->task->resources_requested->gpus);
		char *str = vine_gpus_to_string(p->task->task_id);
		vine_task_set_env_var(p->task,"CUDA_VISIBLE_DEVICES",str);
		free(str);
	}
}

static const char task_output_template[] = "./worker.stdout.XXXXXX";

static char * load_input_file(struct vine_task *t) {
	FILE *fp = fopen("infile", "r");
	if(!fp) {
		fatal("coprocess could not open file 'infile' for reading: %s", strerror(errno));
	}

	fseek(fp, 0L, SEEK_END);
	size_t fsize = ftell(fp);
	fseek(fp, 0L, SEEK_SET);
	// using calloc solves problem of garbage appended to buffer
	char *buf = calloc(fsize + 1, sizeof(*buf));

	int bytes_read = full_fread(fp, buf, fsize);
	if(bytes_read < 0) {
		fatal("error reading file: %s", strerror(errno));
	}

	return buf;
}

/*
Given a unix status returned by wait(), set the process exit code appropriately.
*/

void vine_process_set_exit_status( struct vine_process *p, int status )
{
	if (!WIFEXITED(status)){
		p->exit_code = WTERMSIG(status);
		debug(D_VINE, "task %d (pid %d) exited abnormally with signal %d",p->task->task_id,p->pid,p->exit_code);
	} else {
		p->exit_code = WEXITSTATUS(status);
		debug(D_VINE, "task %d (pid %d) exited normally with exit code %d",p->task->task_id,p->pid,p->exit_code );
	}
}

/*
Execute a task synchronously and return true on success.
*/

int vine_process_execute_and_wait( struct vine_task *task, struct vine_cache *cache, struct link *manager )
{
	struct vine_process *p = vine_process_create(task);

	vine_sandbox_stagein(p,cache,manager);
	
	pid_t pid = vine_process_execute(p);
	if(pid>0) {
		int result, status;
		do {
			result = waitpid(pid,&status,0);
		} while(result!=pid);

		vine_process_set_exit_status(p,status);
	} else {
		p->exit_code = 1;
	}
	
	vine_sandbox_stageout(p,cache,manager);

	/* Remove the task from the process so it is not deleted */
	p->task = 0;

	vine_process_delete(p);
	
	return 1;
}

pid_t vine_process_execute(struct vine_process *p )
{
	// make warning
	fflush(NULL);		/* why is this necessary? */

	p->output_file_name = strdup(task_output_template);
	p->output_fd = mkstemp(p->output_file_name);
	if(p->output_fd == -1) {
		debug(D_VINE, "Could not open worker stdout: %s", strerror(errno));
		return 0;
	}

	p->execution_start = timestamp_get();

	if (!vine_process_get_duty_name(p)) {
		p->pid = fork();
	} else {
		p->coprocess = vine_coprocess_initialize_coprocess(p->task->command_line);
		vine_coprocess_specify_resources(p->coprocess, p->task->resources_requested);
		p->pid = vine_coprocess_start(p->coprocess, p->sandbox);
	}

	if(p->pid > 0) {
		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task().
		setpgid(p->pid, 0);

		debug(D_VINE, "started process %d: %s", p->pid, p->task->command_line);
		return p->pid;

	} else if(p->pid < 0) {

		debug(D_VINE, "couldn't create new process: %s\n", strerror(errno));
		unlink(p->output_file_name);
		close(p->output_fd);
		return p->pid;

	} else {
		if(chdir(p->sandbox)) {
			printf("The sandbox dir is %s", p->sandbox);
			fatal("could not change directory into %s: %s", p->sandbox, strerror(errno));
		}

		int fd = open("/dev/null", O_RDONLY);
		if(fd == -1)
			fatal("could not open /dev/null: %s", strerror(errno));
		int result = dup2(fd, STDIN_FILENO);
		if(result == -1)
			fatal("could not dup /dev/null to stdin: %s", strerror(errno));

		if (p->coprocess == NULL) {
			result = dup2(p->output_fd, STDOUT_FILENO);
			if(result == -1)
				fatal("could not dup pipe to stdout: %s", strerror(errno));

			result = dup2(p->output_fd, STDERR_FILENO);
			if(result == -1)
				fatal("could not dup pipe to stderr: %s", strerror(errno));
			}
		else {
			// load data from input file
			char *input = load_input_file(p->task);

			// call invoke_coprocess_function
		 	char *output = vine_coprocess_run(p->task->command_line, input, p->coprocess);

			// write data to output file
			full_write(p->output_fd, output, strlen(output));

			exit(0);
		}

		close(p->output_fd);

		clear_environment();

		/* overwrite CORES, MEMORY, or DISK variables, if the task used set_* */
		set_resources_vars(p);

		export_environment(p);

		execl("/bin/sh", "sh", "-c", p->task->command_line, (char *) 0);
		_exit(127);	// Failed to execute the cmd.

	}
	return 0;
}

void vine_process_kill(struct vine_process *p)
{
	//make sure a few seconds have passed since child process was created to avoid sending a signal
	//before it has been fully initialized. Else, the signal sent to that process gets lost.
	timestamp_t elapsed_time_execution_start = timestamp_get() - p->execution_start;

	if(elapsed_time_execution_start / 1000000 < 3)
		sleep(3 - (elapsed_time_execution_start / 1000000));

	debug(D_VINE, "terminating task %d pid %d", p->task->task_id, p->pid);

	// Send signal to process group of child which is denoted by -ve value of child pid.
	// This is done to ensure delivery of signal to processes forked by the child.
	kill((-1 * p->pid), SIGKILL);

	// Reap the child process to avoid zombies.
	waitpid(p->pid, NULL, 0);
}

/* The disk needed by a task is shared between the cache and the process
 * sandbox. To account for this overlap, the sandbox size is computed from the
 * stated task size minus those files in the cache directory (i.e., input
 * files). In this way, we can only measure the size of the sandbox when
 * enforcing limits on the process, as a task should never write directly to
 * the cache. */

void vine_process_compute_disk_needed( struct vine_process *p )
{
	struct vine_task *t = p->task;
	struct vine_mount *m;
	struct stat s;

	p->disk = t->resources_requested->disk;

	/* task did not set its disk usage. */
	if(p->disk < 0)
		return;

	if(t->input_mounts) {
		LIST_ITERATE(t->input_mounts,m) {

			if(m->file->type!=VINE_FILE)
				continue;

			if(stat(m->file->cached_name, &s) < 0)
				continue;

			/* p->disk is in MD, st_size in bytes. */
			p->disk -= s.st_size/MEGA;
		}
	}

	if(p->disk < 0) {
		p->disk = -1;
	}

}

int vine_process_measure_disk(struct vine_process *p, int max_time_on_measurement) {
	/* we can't have pointers to struct members, thus we create temp variables here */

	struct path_disk_size_info *state = p->disk_measurement_state;

	int result = path_disk_size_info_get_r(p->sandbox, max_time_on_measurement, &state);

	/* not a memory leak... Either disk_measurement_state was NULL or the same as state. */
	p->disk_measurement_state = state;

	if(state->last_byte_size_complete >= 0) {
		p->sandbox_size = (int64_t) ceil(state->last_byte_size_complete/(1.0*MEGA));
	}
	else {
		p->sandbox_size = -1;
	}

	p->sandbox_file_count = state->last_file_count_complete;

	return result;
}

char * vine_process_get_duty_name( struct vine_process *p) {
	char *duty_name;
	int duty_id;
	HASH_TABLE_ITERATE(duty_ids,duty_name,duty_id) {
		if (duty_id == p->task->task_id) {
			return duty_name;
		}
	}
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
