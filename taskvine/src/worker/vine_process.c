/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_process.h"
#include "vine_manager.h"
#include "vine_gpus.h"
#include "vine_protocol.h"
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
#include "change_process_title.h"

#include "jx.h"
#include "jx_parse.h"

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

/*
Create a vine_process and all of the information necessary for invocation.
However, do not allocate substantial resources at this point.
*/

struct vine_process *vine_process_create( struct vine_task *task, int mini_task )
{
	struct vine_process *p = malloc(sizeof(*p));
	memset(p, 0, sizeof(*p));

	const char *type = mini_task ? "m" : "t";

	p->task = task;

	p->cache_dir = string_format("%s/cache",workspace);
	p->sandbox = string_format("%s/%s.%d", workspace,type,p->task->task_id);
	p->tmpdir = string_format("%s/.taskvine.tmp",p->sandbox);
	p->output_file_name = string_format("%s/.taskvine.stdout",p->sandbox);

	/* Note that create_dir recursively creates parents, so a single one is sufficient. */

	if(!create_dir(p->tmpdir,0777)) {
		vine_process_delete(p);
		return 0;
	}

	return p;
}


void vine_process_delete(struct vine_process *p)
{
	if(p->task)
		vine_task_delete(p->task);
	
	if(p->output_file_name) {
		free(p->output_file_name);
	}

	if(p->library_read_link) link_close(p->library_read_link);
	if(p->library_write_link) link_close(p->library_write_link);
	
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

int vine_process_execute_and_wait( struct vine_process *p, struct vine_cache *cache)
{

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
	return 1;
}

pid_t vine_process_execute(struct vine_process *p )
{
	/* Flush pending stdio buffers prior to forking process, to avoid stale output in child. */
	fflush(NULL);

	/* Various file descriptors for communication between parent and child */
	int pipe_in[2] = {-1,-1};
	int pipe_out[2] = {-1,-1};
	int input_fd = -1;
	int output_fd = -1;
	int error_fd = -1;
	
	if(p->task->provides_library) {
		/* If starting a library, create the pipes for parent-child communication. */

		if(pipe(pipe_in)<0) fatal("couldn't create library pipes: %s\n",strerror(errno));
		if(pipe(pipe_out)<0) fatal("couldn't create library pipes: %s\n",strerror(errno));

		input_fd = pipe_in[0];
		output_fd = pipe_out[1];

		/* Standard error of library goes to output file name for return to manager. */
		error_fd = open(p->output_file_name,O_WRONLY|O_TRUNC|O_CREAT,0777);
		if(output_fd == -1) {
			debug(D_VINE, "Could not open worker stdout: %s", strerror(errno));
			return 0;
		}
	} else {
		/* For other task types, read input from null and send output to assigned file. */
		
		input_fd = open("/dev/null",O_RDONLY);
		output_fd = open(p->output_file_name,O_WRONLY|O_TRUNC|O_CREAT,0777);
		if(output_fd<0) {
			debug(D_VINE, "Could not open worker stdout: %s", strerror(errno));
			return 0;
		}
		error_fd = output_fd;		
	}

	/* Start the performance clock just prior to forking the task. */
	p->execution_start = timestamp_get();

	p->pid = fork();
	if(p->pid > 0) {
		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task().
		setpgid(p->pid, 0);

		debug(D_VINE, "started process %d: %s", p->pid, p->task->command_line);

		/* If we just started a library, then retain links to communicate with it. */
		if(p->task->provides_library) {

			debug(D_VINE, "waiting for library startup message from pid %d\n", p->pid );

			p->library_read_link = link_attach_to_fd(pipe_out[0]);
			p->library_write_link = link_attach_to_fd(pipe_in[1]);

			/* Close the ends of the pipes that the parent process won't use. */
			close(pipe_in[0]);
			close(pipe_out[1]);

			/* Close the error stream that the parent won't use. */
			close(error_fd);

			/* Wait up to 60 seconds for library startup.  This should be asynchronous. */
			time_t stoptime = time(0)+60;
			
			/* Now read back the initialization message so we know it is ready. */
			if (!vine_process_wait_for_library_startup(p,stoptime)) {
				fatal("Unable to setup coprocess");
				/* XXX need better plan for library that fails to start. */
			}

		} else {
			/* For any other task type, drop the fds unused by the parent. */
			close(input_fd);
			close(output_fd);
			close(error_fd);
		}
		
		return p->pid;

	} else if(p->pid < 0) {

		debug(D_VINE, "couldn't create new process: %s\n", strerror(errno));

		close(pipe_in[0]);
		close(pipe_in[1]);
		close(pipe_out[0]);
		close(pipe_out[1]);
		close(input_fd);
		close(output_fd);
		close(error_fd);

		return -1;

	} else {
		if(chdir(p->sandbox)) {
			printf("The sandbox dir is %s", p->sandbox);
			fatal("could not change directory into %s: %s", p->sandbox, strerror(errno));
		}

		/* In the special case of a function-call-task, just load data, communicate with the library, and exit. */
		
		if(p->task->needs_library) {
			change_process_title("vine_worker [function]");

			// load data from input file
			char *input = load_input_file(p->task);

			// communicate with library to invoke the function
		 	char *output = vine_process_invoke_function(p->library_process,p->task->command_line,input,p->sandbox);

			// write data to output file
			full_write(output_fd, output, strlen(output));

			_exit(0);
		}

		/* Otherwise for a normal task or a library, set up file desciptors and execute the command. */

		int result = dup2(input_fd, STDIN_FILENO);
		if(result<0) fatal("could not dup input to stdin: %s", strerror(errno));

		result = dup2(output_fd, STDOUT_FILENO);
		if(result<0) fatal("could not dup output to stdout: %s", strerror(errno));

		result = dup2(error_fd, STDERR_FILENO);
		if(result<0) fatal("could not dup error to stderr: %s", strerror(errno));

		close(input_fd);
		close(output_fd);
		close(error_fd);

		/* For a library task, close the unused sides of the pipes. */
		if(p->task->provides_library) {
			close(pipe_in[1]);
			close(pipe_out[0]);
		}

		/* Remove undesired things from the environment. */
		clear_environment();

		/* Overwrite CORES, MEMORY, or DISK variables, if the task used set_* */
		set_resources_vars(p);

		/* Finally, add things that were explicitly given in the task description. */
		export_environment(p);

		execl("/bin/sh", "sh", "-c", p->task->command_line, (char *) 0);
		_exit(127);	// Failed to execute the cmd.

	}

	return 0;
}

/*
Given a freshly started process, wait for it to initialize and send
back the library startup message with JSON containing the name of
the library, which should match the task's provides_library label.
*/

int vine_process_wait_for_library_startup( struct vine_process *p, time_t stoptime )
{
	char buffer[VINE_LINE_MAX];
	int length = 0;
	
	/* Read a line that gives the length of the response message. */
	link_readline(p->library_read_link, buffer, VINE_LINE_MAX, stoptime);
	sscanf(buffer, "%d", &length);

	/* Now read that length of message and null-terminate it. */
	link_read(p->library_read_link,buffer,length,stoptime);
	buffer[length+1] = 0;

	/* Check that the response is JX and contains the expected name. */
	struct jx * response = jx_parse_string(buffer);

	const char *name = jx_lookup_string(response,"name");

	if(!strcmp(name,p->task->provides_library)) {
		return 1;
	}

	return 0;
}

/*
Invoke a function against a library by sending the invocation message,
and then reading back the result from the necessary pipe.
*/

char *vine_process_invoke_function( struct vine_process *library_process, const char *function_name, const char *function_input, const char *sandbox_path )
{
	/* Set a five minute timeout.  XXX This should be changeable. */
	time_t stoptime = time(0)+300;

	/* This assumes function input is a text string, reconsider? */
	int length = strlen(function_input);

	/* Send the function name, length of data, and sandbox directory. */
	link_printf(library_process->library_write_link, stoptime, "%s %d %s\n", function_name, length, sandbox_path);

	/* Then send the function data itself. */
	/* XXX the library code expects a newline after this, yikes. */
	link_write(library_process->library_write_link, function_input, length, stoptime );
	link_write(library_process->library_write_link, "\n", 1, stoptime );
	
	/* XXX The response should be returned as a variable-length buffer, not a line! */
	
	/* Now read back the response as a single line and give it back. */
	char line[VINE_LINE_MAX];
	if(link_readline(library_process->library_read_link, line, VINE_LINE_MAX, stoptime)) {
		return strdup(line);
	} else {
		return 0;
	}
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

/* vim: set noexpandtab tabstop=4: */
