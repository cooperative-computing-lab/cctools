/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_process.h"
#include "vine_gpus.h"
#include "vine_manager.h"
#include "vine_protocol.h"
#include "vine_sandbox.h"

#include "vine_file.h"
#include "vine_mount.h"
#include "vine_worker.h"
#include "vine_cache.h"

#include "change_process_title.h"
#include "create_dir.h"
#include "debug.h"
#include "domain_name.h"
#include "errno.h"
#include "full_io.h"
#include "hash_table.h"
#include "link.h"
#include "list.h"
#include "macros.h"
#include "path.h"
#include "stringtools.h"
#include "timestamp.h"
#include "trash.h"
#include "xxmalloc.h"

#include "jx.h"
#include "jx_parse.h"

#include <dirent.h>
#include <fcntl.h>
#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

extern struct vine_cache *cache_manager;

/*
Give the letter code used for the process sandbox dir.
*/

static const char *vine_process_sandbox_code(vine_process_type_t type)
{
	switch (type) {
	case VINE_PROCESS_TYPE_STANDARD:
		return "task";
	case VINE_PROCESS_TYPE_MINI_TASK:
		return "mini";
	case VINE_PROCESS_TYPE_LIBRARY:
		return "libr";
	case VINE_PROCESS_TYPE_FUNCTION:
		return "func";
	case VINE_PROCESS_TYPE_TRANSFER:
		return "tran";
	}

	/* Odd return here is used to silence compiler while still retaining typedef check above. */
	return "task";
}

/*
Create a vine_process and all of the information necessary for invocation.
However, do not allocate substantial resources at this point.
*/

struct vine_process *vine_process_create(struct vine_task *task, vine_process_type_t type)
{
	struct vine_process *p = malloc(sizeof(*p));
	memset(p, 0, sizeof(*p));

	p->task = task;
	p->type = type;

	const char *dirtype = vine_process_sandbox_code(p->type);

	p->sandbox = string_format("%s/%s.%d", workspace->workspace_dir, dirtype, p->task->task_id);
	p->tmpdir = string_format("%s/.taskvine.tmp", p->sandbox);
	p->output_file_name = string_format("%s/.taskvine.stdout", p->sandbox);
	p->output_length = 0;

	p->functions_running = 0;
	p->library_ready = 0;

	/* Note that create_dir recursively creates parents, so a single one is sufficient. */

	if (!create_dir(p->tmpdir, 0777)) {
		vine_process_delete(p);
		return 0;
	}

	return p;
}

void vine_process_delete(struct vine_process *p)
{
	if (p->task)
		vine_task_delete(p->task);

	if (p->output_file_name) {
		free(p->output_file_name);
	}

	if (p->library_read_link)
		link_close(p->library_read_link);
	if (p->library_write_link)
		link_close(p->library_write_link);

	if (p->sandbox) {
		trash_file(p->sandbox);
		free(p->sandbox);
	}

	if (p->tmpdir)
		free(p->tmpdir);

	free(p);
}

static void clear_environment()
{
	/* Clear variables that we really want the user to set explicitly.
	 * Ideally, we would start with a clean environment, but certain variables,
	 * such as HOME are seldom set explicitly, and some executables rely on them.
	 */

	unsetenv("DISPLAY");
}

static void export_environment(struct vine_process *p)
{
	struct list *env_list = p->task->env_list;
	char *name;

	LIST_ITERATE(env_list, name)
	{
		char *value = strchr(name, '=');
		if (value) {
			*value = 0;
			setenv(name, value + 1, 1);
			*value = '=';
		} else {
			/* Without =, we remove the variable */
			unsetenv(name);
		}
	}

	/* we set TMPDIR after env_list on purpose. We do not want a task writing
	 * to some other tmp dir. */
	if (p->tmpdir) {
		setenv("TMPDIR", p->tmpdir, 1);
		setenv("TEMP", p->tmpdir, 1);
		setenv("TMP", p->tmpdir, 1);
	}
}

static void set_integer_env_var(struct vine_process *p, const char *name, int64_t value)
{
	char *value_str = string_format("%" PRId64, value);
	vine_task_set_env_var(p->task, name, value_str);
	free(value_str);
}

static void set_resources_vars(struct vine_process *p)
{
	if (p->task->resources_requested->cores > 0) {
		set_integer_env_var(p, "CORES", p->task->resources_requested->cores);
		set_integer_env_var(p, "OMP_NUM_THREADS", p->task->resources_requested->cores);
		set_integer_env_var(p, "OPENBLAS_NUM_THREADS", p->task->resources_requested->cores);
		set_integer_env_var(p, "VECLIB_NUM_THREADS", p->task->resources_requested->cores);
		set_integer_env_var(p, "MKL_NUM_THREADS", p->task->resources_requested->cores);
		set_integer_env_var(p, "NUMEXPR_NUM_THREADS", p->task->resources_requested->cores);
	}

	if (p->task->resources_requested->memory > 0) {
		set_integer_env_var(p, "MEMORY", p->task->resources_requested->memory);
	}

	if (p->task->resources_requested->disk > 0) {
		set_integer_env_var(p, "DISK", p->task->resources_requested->disk);
	}

	if (p->task->resources_requested->gpus > 0) {
		set_integer_env_var(p, "GPUS", p->task->resources_requested->gpus);
		char *str = vine_gpus_to_string(p->task->task_id);
		vine_task_set_env_var(p->task, "CUDA_VISIBLE_DEVICES", str);
		free(str);
	}
}

/*
After a process exit has been observed, record the completion in the process structure.
*/

static void vine_process_complete(struct vine_process *p, int status)
{
	if (!WIFEXITED(status)) {
		p->exit_code = WTERMSIG(status);
		debug(D_VINE, "task %d (pid %d) exited abnormally with signal %d", p->task->task_id, p->pid, p->exit_code);
	} else {
		p->exit_code = WEXITSTATUS(status);
		debug(D_VINE, "task %d (pid %d) exited normally with exit code %d", p->task->task_id, p->pid, p->exit_code);
	}
}

/*
Execute a task synchronously and return true on success.
*/

int vine_process_execute_and_wait(struct vine_process *p)
{
	if (vine_process_execute(p)) {
		vine_process_wait(p);
		return 1;
	} else {
		p->exit_code = 1;
		return 0;
	}
}

/* Send a message containing details of a function call to the relevant library to execute it.
 * @param p 	The relevant vine_process structure encapsulating a function call.
 * @return 		1 if the message is successfully sent to the library, 0 otherwise. */

int vine_process_invoke_function(struct vine_process *p)
{
	char *buffer = string_format("%d %s %s %s", p->task->task_id, p->task->command_line, p->sandbox, p->output_file_name);
	ssize_t result = link_printf(p->library_process->library_write_link, time(0) + options->active_timeout, "%ld\n%s", strlen(buffer), buffer);

	// conservatively assume that the function starts executing as soon as we send it to the library.
	// XXX Alternatively, the library could report when the function started.
	p->execution_start = timestamp_get();

	free(buffer);

	if (result < 0) {
		debug(D_VINE, "failed to communicate with library '%s' task %d", p->task->needs_library, p->library_process->pid);
		return 0;
	} else {
		debug(D_VINE,
				"started task %d as function call '%s' to library '%s' task %d",
				p->task->task_id,
				p->task->command_line,
				p->task->needs_library,
				p->library_process->pid);
		return 1;
	}
}

/*
Start a process executing and if successful, return true.
Otherwise return false.
*/

int vine_process_execute(struct vine_process *p)
{
	/* Special case: invoke function by sending message. */
	if (p->type == VINE_PROCESS_TYPE_FUNCTION) {
		return vine_process_invoke_function(p);
	}

	/* Flush pending stdio buffers prior to forking process, to avoid stale output in child. */
	fflush(NULL);

	/* Various file descriptors for communication between parent and child */
	int pipe_in[2] = {-1, -1};
	int pipe_out[2] = {-1, -1};
	int stdin_fd = -1;
	int stdout_fd = -1;
	int stderr_fd = -1;
	int in_pipe_fd = -1;  // only for library task, fd to send functions to library
	int out_pipe_fd = -1; // only for library task, fd to receive results from library

	/* Setting up input, output, and stderr for various task types. */
	if (p->type == VINE_PROCESS_TYPE_LIBRARY) {
		/* If starting a library, create the pipes for parent-child communication. */
		if (pipe(pipe_in) < 0)
			fatal("couldn't create library pipes: %s\n", strerror(errno));
		if (pipe(pipe_out) < 0)
			fatal("couldn't create library pipes: %s\n", strerror(errno));
		in_pipe_fd = pipe_in[0];
		out_pipe_fd = pipe_out[1];
	}

	/* Read input from null and send output to assigned file. */
	stdin_fd = open("/dev/null", O_RDONLY);
	stdout_fd = open(p->output_file_name, O_WRONLY | O_TRUNC | O_CREAT, 0777);
	if (stdout_fd < 0) {
		debug(D_VINE, "Could not open worker stdout: %s", strerror(errno));
		return 0;
	}
	stderr_fd = stdout_fd;

	/* Start the performance clock just prior to forking the task. */
	p->execution_start = timestamp_get();
	p->pid = fork();

	if (p->pid > 0) {
		// Make child process the leader of its own process group. This allows
		// signals to also be delivered to processes forked by the child process.
		// This is currently used by kill_task().
		setpgid(p->pid, 0);

		/* Start the performance clock just after forking the process. */
		p->execution_start = timestamp_get();

		debug(D_VINE, "started task %d pid %d: %s", p->task->task_id, p->pid, p->task->command_line);

		/* If we just started a library, then retain links to communicate with it. */
		if (p->type == VINE_PROCESS_TYPE_LIBRARY) {

			p->library_read_link = link_attach_to_fd(pipe_out[0]);
			p->library_write_link = link_attach_to_fd(pipe_in[1]);

			/* Close the ends of the pipes that the parent process won't use. */
			close(pipe_in[0]);
			close(pipe_out[1]);
		}
		/* Drop the fds unused by the parent, stderr_fd is stdout_fd so close once. */
		close(stdin_fd);
		close(stdout_fd);

		return 1;

	} else if (p->pid < 0) {

		debug(D_VINE, "couldn't create new process: %s\n", strerror(errno));

		close(pipe_in[0]);
		close(pipe_in[1]);
		close(pipe_out[0]);
		close(pipe_out[1]);
		close(stdin_fd);
		close(stdout_fd);
		close(stderr_fd);

		return 0;

	} else {
		if (chdir(p->sandbox)) {
			printf("The sandbox dir is %s", p->sandbox);
			fatal("could not change directory into %s: %s", p->sandbox, strerror(errno));
		}
		int result = dup2(stdin_fd, STDIN_FILENO);
		if (result < 0)
			fatal("could not dup input to stdin: %s", strerror(errno));

		result = dup2(stdout_fd, STDOUT_FILENO);
		if (result < 0)
			fatal("could not dup output to stdout: %s", strerror(errno));

		result = dup2(stderr_fd, STDERR_FILENO);
		if (result < 0)
			fatal("could not dup error to stderr: %s", strerror(errno));

		/* Close redundant file descriptors after dup()'ing.
		 * Note that stdout_fd is the same as stderr_fd so it's only closed once */
		close(stdin_fd);
		close(stdout_fd);

		/* For a library task, close the unused sides of the pipes. */
		if (p->type == VINE_PROCESS_TYPE_LIBRARY) {
			close(pipe_in[1]);
			close(pipe_out[0]);
		}

		/* Remove undesired things from the environment. */
		clear_environment();

		/* Overwrite CORES, MEMORY, or DISK variables, if the task used set_* */
		set_resources_vars(p);

		/* Finally, add things that were explicitly given in the task description. */
		export_environment(p);

		/* Library task passes the file descriptors to talk to the manager via
		 * the command line plus the worker pid to wake the worker up
		 * so it requires a special execl. */
		if (p->type != VINE_PROCESS_TYPE_LIBRARY) {
			execl("/bin/sh", "sh", "-c", p->task->command_line, (char *)0);
		} else {
			char *final_command = string_format("%s --in-pipe-fd %d --out-pipe-fd %d --task-id %d --library-cores %d --function-slots %d --worker-pid %d",
					p->task->command_line,
					in_pipe_fd,
					out_pipe_fd,
					p->task->task_id,
					(int)p->task->resources_requested->cores,
					p->task->function_slots,
					getppid());
			execl("/bin/sh", "sh", "-c", final_command, (char *)0);
		}
		_exit(127); // Failed to execute the cmd.

		/* NOTREACHED */
		return 0;
	}
}

/*
Non-blocking check to see if a process has completed.
Returns true if complete, false otherwise.
*/

int vine_process_is_complete(struct vine_process *p)
{
	/* A function call doesn't have a Unix process to check. */
	if (p->type == VINE_PROCESS_TYPE_FUNCTION) {
		return 0;
	}

	/* But any other type of process is done when the Unix process completes. */
	int status;
	int result = wait4(p->pid, &status, WNOHANG, &p->rusage);
	if (result == p->pid) {
		vine_process_complete(p, status);
		return 1;
	} else {
		return 0;
	}
}

/*
Wait indefinitely for a process to exit and collect its final disposition.
Return true if the process was found, false otherwise,
*/

int vine_process_wait(struct vine_process *p)
{
	/* A function call cannot be waited for directly. */
	if (p->type == VINE_PROCESS_TYPE_FUNCTION) {
		return 0;
	}

	while (1) {
		int status;
		pid_t pid = waitpid(p->pid, &status, 0);
		if (pid == p->pid) {
			vine_process_complete(p, status);
			return 1;
		} else if (pid < 0 && errno == EINTR) {
			continue;
		} else {
			return 0;
		}
	}
}

/* Receive a message containing a function call id from the library without blocking.
 * @param p			The vine process encapsulating the function call.
 * @param done_task_id          Pointer to location to store completed task id.
 * @param done_exit_code        Pointer to location to the completed task exit code.
 * return 			1 if the operation succeeds, 0 otherwise.
 */
int vine_process_library_get_result(struct vine_process *p, uint64_t *done_task_id, int *done_exit_code)
{
	/* If this is not a library process, don't check. */
	if (p->type != VINE_PROCESS_TYPE_LIBRARY)
		return 0;

	/* If the library is not initialized, don't check. */
	if (!p->library_ready)
		return 0;

	/* If there is no data waiting on the link, don't check. */
	if (!link_usleep(p->library_read_link, 0, 1, 0))
		return 0;

	char buffer[VINE_LINE_MAX]; // Buffer to store length of data from library.
	int ok = 1;

	/* read number of bytes of data first. */
	ok = link_readline(p->library_read_link, buffer, VINE_LINE_MAX, time(0) + options->active_timeout);
	if (!ok) {
		return 0;
	}
	int len_buffer = atoi(buffer);

	/* now read the buffer, which is the task id of the done function invocation. */
	char buffer_data[len_buffer + 1];
	ok = link_read(p->library_read_link, buffer_data, len_buffer, time(0) + options->active_timeout);
	if (ok <= 0) {
		return 0;
	}

	/* null terminate the buffer before treating it as a string. */
	buffer_data[ok] = 0;
	sscanf(buffer_data, "%" SCNu64 " %d", done_task_id, done_exit_code);
	debug(D_VINE, "Received result for function %" PRIu64 ", exit code %d", *done_task_id, *done_exit_code);

	return ok;
}

/*
Send a kill signal to a running process.
Note that the process must still be waited-for to collect its final disposition.
*/

void vine_process_kill(struct vine_process *p)
{
	/* XXX A function call cannot (yet) be killed directly. */
	/* This could be implemented by sending a message to the library process. */
	if (p->type == VINE_PROCESS_TYPE_FUNCTION) {
		return;
	}

	// make sure a few seconds have passed since child process was created to avoid sending a signal
	// before it has been fully initialized. Else, the signal sent to that process gets lost.
	timestamp_t elapsed_time_execution_start = timestamp_get() - p->execution_start;

	if (elapsed_time_execution_start / 1000000 < 3)
		sleep(3 - (elapsed_time_execution_start / 1000000));

	debug(D_VINE, "terminating task %d pid %d", p->task->task_id, p->pid);

	// Send signal to process group of child which is denoted by -ve value of child pid.
	// This is done to ensure delivery of signal to processes forked by the child.
	kill((-1 * p->pid), SIGKILL);
}

/*
Send a kill signal to a running process, and then wait for it to exit.
*/

int vine_process_kill_and_wait(struct vine_process *p)
{
	vine_process_kill(p);
	return vine_process_wait(p);
}

/* The disk needed by a task is shared between the cache and the process
 * sandbox. To account for this overlap, the sandbox size is computed from the
 * stated task size minus those files in the cache directory (i.e., input
 * files). In this way, we can only measure the size of the sandbox when
 * enforcing limits on the process, as a task should never write directly to
 * the cache. */

void vine_process_compute_disk_needed(struct vine_process *p)
{
	struct vine_task *t = p->task;
	struct vine_mount *m;
	struct stat s;

	p->disk = t->resources_requested->disk;

	/* task did not set its disk usage. */
	if (p->disk < 0)
		return;

	if (t->input_mounts) {
		LIST_ITERATE(t->input_mounts, m)
		{

			if (m->file->type != VINE_FILE)
				continue;

			if (stat(m->file->cached_name, &s) < 0)
				continue;

			/* p->disk is in MD, st_size in bytes. */
			p->disk -= s.st_size / MEGA;
		}
	}

	if (p->disk < 0) {
		p->disk = -1;
	}
}

static int vine_process_sandbox_disk_measure(struct vine_process *p, int max_time_on_measurement, struct path_disk_size_info **state);

int vine_process_measure_disk(struct vine_process *p, int max_time_on_measurement)
{
	/* we can't have pointers to struct members, thus we create temp variables here */

	struct path_disk_size_info *state = p->disk_measurement_state;

	int result = vine_process_sandbox_disk_measure(p, max_time_on_measurement, &state);

	/* not a memory leak... Either disk_measurement_state was NULL or the same as state. */
	p->disk_measurement_state = state;

	if (state->last_byte_size_complete >= 0) {
		p->sandbox_size = (int64_t)ceil(state->last_byte_size_complete / (1.0 * MEGA));
	} else {
		p->sandbox_size = -1;
	}

	p->sandbox_file_count = state->last_file_count_complete;

	return result;
}

static int vine_process_sandbox_disk_measure(struct vine_process *p, int max_secs, struct path_disk_size_info **state)
{
	int64_t start_time = time(0);
	int result = 0;

	const char *path = p->sandbox;

	struct DIR_with_name {
		DIR *dir;
		char *name;
	};

	if (!*state) {
		/* if state is null, there is no state, and path is the root of the measurement. */
		*state = calloc(1, sizeof(struct path_disk_size_info));
	}

	struct path_disk_size_info *s = *state; /* shortcut for *state, so we do not need to type (*state)->... */

	/* if no current_dirs, we begin a new measurement. */
	if (!s->current_dirs) {
		s->complete_measurement = 0;

		struct DIR_with_name *here = calloc(1, sizeof(struct DIR_with_name));

		if ((here->dir = opendir(path))) {
			here->name = xxstrdup(path);
			s->current_dirs = list_create();
			s->size_so_far = 0;
			s->count_so_far = 1; /* count the root directory */
			list_push_tail(s->current_dirs, here);
		} else {
			debug(D_DEBUG, "error reading disk usage on directory: %s.\n", path);
			s->size_so_far = -1;
			s->count_so_far = -1;
			s->complete_measurement = 1;
			result = -1;

			free(here);
			goto timeout;
		}
	}

	struct DIR_with_name *tail;
	while ((tail = list_peek_tail(s->current_dirs))) {
		struct dirent *entry;
		struct stat file_info;

		if (!tail->dir) { // only open dir when it's being processed
			tail->dir = opendir(tail->name);
			if (!tail->dir) {
				if (errno == ENOENT) {
					/* Do nothing as a directory might go away. */
					tail = list_pop_tail(s->current_dirs);
					free(tail->name);
					free(tail);
					continue;
				} else {
					debug(D_DEBUG, "error opening directory '%s', errno: %s.\n", tail->name, strerror(errno));
					result = -1;
					goto timeout;
				}
			}
		}
		/* Read out entries from the dir stream. */
		while ((entry = readdir(tail->dir))) {
			if (strcmp(".", entry->d_name) == 0 || strcmp("..", entry->d_name) == 0)
				continue;

			char composed_path[PATH_MAX];
			if (entry->d_name[0] == '/') {
				strncpy(composed_path, entry->d_name, PATH_MAX);
			} else {
				snprintf(composed_path, PATH_MAX, "%s/%s", tail->name, entry->d_name);
			}

			if (lstat(composed_path, &file_info) < 0) {
				if (errno == ENOENT) {
					/* our DIR structure is stale, and a file went away. We simply do nothing. */
				} else {
					debug(D_DEBUG, "error reading disk usage on '%s'.\n", path);
					result = -1;
				}
				continue;
			}

			int skip = 0;
			if (p->task->input_mounts) {
				struct vine_mount *m;
				LIST_ITERATE(p->task->input_mounts, m)
				{

					int offset = 0;
					char *idx = strchr(composed_path, '/');
					while (idx != NULL) {
						offset = idx - composed_path + 1;
						idx = strchr(idx + 1, '/');
					}

					if (strncmp(composed_path + offset, m->remote_name, PATH_MAX) == 0) {
						skip = 1;
						break;
					}
				}
			}

			s->count_so_far++;

			if (!skip) {
				if (S_ISREG(file_info.st_mode)) {
					s->size_so_far += file_info.st_size;
				} else if (S_ISDIR(file_info.st_mode)) {
					/* Only add name of directory, will only open it to read when it's its turn. */
					struct DIR_with_name *branch = calloc(1, sizeof(struct DIR_with_name));
					branch->name = xxstrdup(composed_path);
					list_push_head(s->current_dirs, branch);
				} else if (S_ISLNK(file_info.st_mode)) {
					/* do nothing, avoiding infinite loops. */
				}
			}
			if (max_secs > -1) {
				if (time(0) - start_time >= max_secs) {
					goto timeout;
				}
			}
		}
		/* we are done reading a complete directory, and we go to the next in the queue */
		tail = list_pop_tail(s->current_dirs);
		if (tail->dir) {
			closedir(tail->dir);
		}
		free(tail->name);
		free(tail);
	}

	list_delete(s->current_dirs);
	s->current_dirs = NULL; /* signal that a new measurement is needed, if state structure is reused. */
	s->complete_measurement = 1;

timeout:
	if (s->complete_measurement) {
		/* if a complete measurement has been done, then update
		 * for the found value */
		s->last_byte_size_complete = s->size_so_far;
		s->last_file_count_complete = s->count_so_far;
	} else {
		/* else, we hit a timeout. measurement reported is conservative, from
		 * what we knew, and know so far. */

		s->last_byte_size_complete = MAX(s->last_byte_size_complete, s->size_so_far);
		s->last_file_count_complete = MAX(s->last_file_count_complete, s->count_so_far);
	}

	return result;
}

/* vim: set noexpandtab tabstop=4: */
