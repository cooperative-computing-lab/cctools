
#include "work_queue_process.h"
#include "work_queue.h"
#include "work_queue_internal.h"

#include "debug.h"
#include "errno.h"
#include "macros.h"
#include "stringtools.h"
#include "create_dir.h"
#include "delete_dir.h"
#include "list.h"
#include "disk_alloc.h"
#include "path.h"
#include "xxmalloc.h"

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

#define SMALL_BUFFER_SIZE 256
#define MAX_BUFFER_SIZE 4096
#define DEFAULT_WORK_DIR "/home/worker"
#define CONVERT_IMG "ubuntu/mf_wq"
#define TMP_SCRIPT "tmp.sh"
#define DEFAULT_EXE_APP "#!/bin/sh"

// return 0 on error, 1 otherwise
static int create_task_directories(struct work_queue_process *p) {
	char tmpdir_template[1024];

	p->sandbox = string_format("t.%d", p->task->taskid);
	if(!create_dir(p->sandbox, 0777)) {
		return 0;
	}

	char absolute[1024];
	path_absolute(p->sandbox, absolute, 1);
	free(p->sandbox);
	p->sandbox = xxstrdup(absolute);

	string_nformat(tmpdir_template, sizeof(tmpdir_template), "%s/cctools-temp-t.%d.XXXXXX", p->sandbox, p->task->taskid);
	if(mkdtemp(tmpdir_template) == NULL) {
		return 0;
	}

	p->tmpdir  = xxstrdup(tmpdir_template);
	if(chmod(p->tmpdir, 0777) != 0) {
		return 0;
	}

	return 1;
}

struct work_queue_process *work_queue_process_create(struct work_queue_task *wq_task, int disk_allocation)
{
	struct work_queue_process *p = malloc(sizeof(*p));
	memset(p, 0, sizeof(*p));
	p->task = wq_task;
	p->task->disk_allocation_exhausted = 0;
	//placeholder filesystem until permanent solution
	char *fs = "ext2";

	if(disk_allocation == 1) {
		work_queue_process_compute_disk_needed(p);
		if(p->task->resources_requested->disk > 0) {
			int64_t size = (p->task->resources_requested->disk) * 1024;
			p->sandbox = string_format("t.%d", p->task->taskid);

			if(disk_alloc_create(p->sandbox, fs, size) == 0) {
				p->loop_mount = 1;
				debug(D_WQ, "disk_alloc: %"PRId64"MB\n", size);
				return p;
			}
		}
		if(!create_task_directories(p)) {
			work_queue_process_delete(p);
			return 0;
		}

		p->loop_mount = 0;
		return p;
	}
	else {
		if(!create_task_directories(p)) {
			work_queue_process_delete(p);
			return 0;
		}

		p->loop_mount = 0;
		return p;
	}
}

void work_queue_process_delete(struct work_queue_process *p)
{

	if(p->task)
		work_queue_task_delete(p->task);

	if(p->output_fd) {
		close(p->output_fd);
	}

	if(p->output_file_name) {
		unlink(p->output_file_name);
		free(p->output_file_name);
	}

	if(p->sandbox) {
		if(p->loop_mount == 1) {
			disk_alloc_delete(p->sandbox);
		}
		else {
			delete_dir(p->sandbox);
		}
		free(p->sandbox);
	}

	if(p->tmpdir)
		free(p->tmpdir);

	free(p);
}

static void clear_environment() {
	/* Clear variables that we really want the user to set explicitly.
	 * Ideally, we would start with a clean environment, but certain variables,
	 * such as HOME are seldom set explicitly, and some executables rely on them.
	*/

	unsetenv("DISPLAY");

}

static void export_environment( struct work_queue_process *p )
{
	struct list *env_list = p->task->env_list;
	char *name;
	list_first_item(env_list);
	while((name=list_next_item(env_list))) {
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

static void specify_integer_env_var( struct work_queue_process *p, const char *name, int64_t value) {
	char *value_str = string_format("%" PRId64, value);
	work_queue_task_specify_enviroment_variable(p->task, name, value_str);
	free(value_str);
}

static void specify_resources_vars(struct work_queue_process *p) {
	if(p->task->resources_requested->cores > 0) {
		specify_integer_env_var(p, "CORES", p->task->resources_requested->cores);
	}

	if(p->task->resources_requested->memory > 0) {
		specify_integer_env_var(p, "MEMORY", p->task->resources_requested->memory);
	}

	if(p->task->resources_requested->disk > 0) {
		specify_integer_env_var(p, "DISK", p->task->resources_requested->disk);
	}

	if(p->task->resources_requested->gpus > 0) {
		specify_integer_env_var(p, "GPUS", p->task->resources_requested->gpus);
	}
}

static const char task_output_template[] = "./worker.stdout.XXXXXX";

pid_t work_queue_process_execute(struct work_queue_process *p, int container_mode, ...)
{
	// make warning

	fflush(NULL);		/* why is this necessary? */

	p->output_file_name = strdup(task_output_template);
	p->output_fd = mkstemp(p->output_file_name);
	if(p->output_fd == -1) {
		debug(D_WQ, "Could not open worker stdout: %s", strerror(errno));
		return 0;
	}

	if(p->loop_mount) {
		char *buf = malloc(PATH_MAX);
		char *pwd = getcwd(buf, PATH_MAX);
		char *filename = work_queue_generate_disk_alloc_full_filename(pwd, p->task->taskid);
		p->task->command_line = string_format("export CCTOOLS_DISK_ALLOC=%s; %s", filename, p->task->command_line);
		free(buf);
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
			printf("The sandbox dir is %s", p->sandbox);
			fatal("could not change directory into %s: %s", p->sandbox, strerror(errno));
		}

		int fd = open("/dev/null", O_RDONLY);
		if(fd == -1)
			fatal("could not open /dev/null: %s", strerror(errno));
		int result = dup2(fd, STDIN_FILENO);
		if(result == -1)
			fatal("could not dup /dev/null to stdin: %s", strerror(errno));

		result = dup2(p->output_fd, STDOUT_FILENO);
		if(result == -1)
			fatal("could not dup pipe to stdout: %s", strerror(errno));

		result = dup2(p->output_fd, STDERR_FILENO);
		if(result == -1)
			fatal("could not dup pipe to stderr: %s", strerror(errno));

		close(p->output_fd);

		clear_environment();

		/* overwrite CORES, MEMORY, or DISK variables, if the task used specify_* */
		specify_resources_vars(p);

		export_environment(p);

		va_list arg_lst;
		if(container_mode == NONE) {
			execl("/bin/sh", "sh", "-c", p->task->command_line, (char *) 0);
			_exit(127);	// Failed to execute the cmd.

		} else if(container_mode == UMBRELLA) {
			fatal("UMBRELLA mode have not been implemented\n");

		} else {
			// Write task command into a shell script
			char *tmp_ptr = p->task->command_line;
			int cmd_line_size = 0;
			while(*(++tmp_ptr) != '\0')
				cmd_line_size++;

			FILE *script_fn = fopen(TMP_SCRIPT, "w");
			fprintf(script_fn, "%s\n%s", DEFAULT_EXE_APP, p->task->command_line);
			fclose(script_fn);
			chmod(TMP_SCRIPT, 0755);

			uid_t uid = getuid();
			char uid_str[MAX_BUFFER_SIZE];
			string_nformat(uid_str, sizeof(uid_str), "%d", uid);

			// Get path to sandbox
			char curr_wrk_dir[MAX_BUFFER_SIZE];
			char *wrk_space;

			if((wrk_space = getenv("WORK_QUEUE_SANDBOX")) != NULL) {
				string_nformat(curr_wrk_dir, sizeof(curr_wrk_dir), "%s/%s", wrk_space, p->sandbox);
			} else {
				perror("getenv() error");
			}

			if(container_mode == DOCKER) {
				va_start(arg_lst, container_mode);
				char img_name[MAX_BUFFER_SIZE];
				strncpy(img_name, va_arg(arg_lst, const char *), MAX_BUFFER_SIZE);
				va_end(arg_lst);

				char mnt_flg_val[MAX_BUFFER_SIZE];
				string_nformat(mnt_flg_val, sizeof(mnt_flg_val), "%s:%s", curr_wrk_dir, DEFAULT_WORK_DIR);
				// cmd for running the shell script
				char run_cmd[SMALL_BUFFER_SIZE];
				string_nformat(run_cmd, sizeof(run_cmd), "./%s", TMP_SCRIPT);

				execl("/usr/bin/docker", "/usr/bin/docker", "run", "--rm", "-v", mnt_flg_val, "-w", DEFAULT_WORK_DIR, "-u", uid_str, "-m", "1g", img_name, run_cmd, (char *) 0);
				_exit(127);	// Failed to execute the cmd.

			} else {
				// DOCKER_PRESERVE mode
				va_start(arg_lst, container_mode);
				char container_name[MAX_BUFFER_SIZE];
				strncpy(container_name, va_arg(arg_lst, const char *), MAX_BUFFER_SIZE);
				va_end(arg_lst);

				char sub_proc_sh_fn[MAX_BUFFER_SIZE];
				char sub_proc_sh_fn_path[MAX_BUFFER_SIZE];
				string_nformat(sub_proc_sh_fn, sizeof(sub_proc_sh_fn), "tmp_%s.sh", p->sandbox);
				string_nformat(sub_proc_sh_fn_path, sizeof(sub_proc_sh_fn_path), "%s/%s", wrk_space, sub_proc_sh_fn);

				FILE *sub_proc_script_fn = fopen(sub_proc_sh_fn_path, "w");
				fprintf(sub_proc_script_fn, "%s\ncd %s\n./%s", DEFAULT_EXE_APP, p->sandbox, TMP_SCRIPT);
				fclose(sub_proc_script_fn);
				chmod(sub_proc_sh_fn_path, 0755);

				char run_sh_fn[MAX_BUFFER_SIZE];
				string_nformat(run_sh_fn, sizeof(run_sh_fn), "./%s", sub_proc_sh_fn);

				execl("/usr/bin/docker", "/usr/bin/docker", "exec", container_name, run_sh_fn, (char *) 0);
				_exit(127);	// Failed to execute the cmd.
			}
		}
	}
	return 0;
}

void work_queue_process_kill(struct work_queue_process *p)
{
	//make sure a few seconds have passed since child process was created to avoid sending a signal
	//before it has been fully initialized. Else, the signal sent to that process gets lost.
	timestamp_t elapsed_time_execution_start = timestamp_get() - p->execution_start;

	if(elapsed_time_execution_start / 1000000 < 3)
		sleep(3 - (elapsed_time_execution_start / 1000000));

	debug(D_WQ, "terminating task %d pid %d", p->task->taskid, p->pid);

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
void  work_queue_process_compute_disk_needed( struct work_queue_process *p ) {
	struct work_queue_task *t = p->task;
	struct work_queue_file *f;
	struct stat s;

	p->disk = t->resources_requested->disk;

	/* task did not specify its disk usage. */
	if(p->disk < 0)
		return;

	if(t->input_files) {
		list_first_item(t->input_files);
		while((f = list_next_item(t->input_files))) {
			if(f->type != WORK_QUEUE_FILE && f->type != WORK_QUEUE_FILE_PIECE)
					continue;

			if(stat(f->cached_name, &s) < 0)
				continue;

			/* p->disk is in MD, st_size in bytes. */
			p->disk -= s.st_size/MEGA;
		}
	}

	if(p->disk < 0) {
		p->disk = -1;
	}

}

int work_queue_process_measure_disk(struct work_queue_process *p, int max_time_on_measurement) {
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
