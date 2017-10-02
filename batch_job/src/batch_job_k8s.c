#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"
#include "uuid.h"
#include "path.h"
#include "list.h"
#include "itable.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#define MAX_BUF_SIZE 4096


static cctools_uuid_t *mf_uuid = NULL;
static const char *k8s_image = NULL;
static const char *resources_block = "";
static int count = 1;
static struct itable *k8s_job_info_table = NULL;

static const char *k8s_script = 
#include "batch_job_k8s_script.c"

static const char *k8s_script_file_name = "_temp_k8s_script.sh";

static const char *k8s_config_tmpl = "\
{\n\
    \"apiVersion\": \"v1\",\n\
    \"kind\": \"Pod\",\n\
    \"metadata\": {\n\
        \"labels\": {\n\
            \"app\": \"%s\"\n\
        },\n\
        \"name\": \"%s\"\n\
    },\n\
\n\
    \"spec\": {\n\
        \"containers\": [{\n\
            \"name\": \"%s\",\n\
            \"image\": \"%s\",\n\
			%s\n\
            \"imagePullPolicy\": \"IfNotPresent\",\n\
            \"command\": [\"/bin/bash\", \"-c\"],\n\
            \"args\": [\"echo \\\"%d,pod_created\\\" > %s.log ; tail -f /dev/null \"]\n\
        }],\n\
        \"restartPolicy\": \"Never\"\n\
    }\n\
}\n\
";

static const char *resource_tmpl = "\
\"resources\": {\n\
	\"requests\": {\n\
		\"cpu\": \"%s\",\n\
		\"memory\": \"%s\"\n\
	},\n\
	\"limits\": {\n\
		\"cpu\": \"%s\",\n\
		\"memory\": \"%s\"\n\
	}\n\
},\n\
";

typedef struct k8s_job_info{
	int job_id;
	char *cmd;
	char *extra_input_files;
	char *extra_output_files;
	int is_running;
} k8s_job_info;

static k8s_job_info* create_k8s_job_info(int job_id, const char *cmd, 
		const char *extra_input_files, const char *extra_output_files)
{
	k8s_job_info *new_job_info = malloc(sizeof(k8s_job_info));
	new_job_info->job_id = job_id;
	new_job_info->cmd = xxstrdup(cmd);
	new_job_info->extra_input_files = xxstrdup(extra_input_files);
	new_job_info->extra_output_files = xxstrdup(extra_output_files);
	new_job_info->is_running = 0;

	return new_job_info;
}

static batch_job_id_t batch_job_k8s_submit (struct batch_queue *q, const char *cmd, 
		const char *extra_input_files, const char *extra_output_files, struct jx *envlist, 
		const struct rmsummary *resources )
{
	
	if(mf_uuid == NULL) {
		mf_uuid = malloc(sizeof(*mf_uuid));
		cctools_uuid_create (mf_uuid);
		// The pod id cannot include upper case
		string_tolower(mf_uuid->str);
	}
	
	if(k8s_job_info_table == NULL) 
		k8s_job_info_table = itable_create(0);

	if(k8s_image == NULL) {
		if((k8s_image = batch_queue_get_option(q, "k8s-image")) == NULL) {
			fatal("Please specify the container image by using \"--k8s-image\"");
		}
	}
	
	fflush(NULL);
	pid_t pid = fork();
	
	int job_id = count ++;

	if(pid > 0) {

		debug(D_BATCH, "started job %d: %s", job_id, cmd);
		struct batch_job_info *info = calloc(1, sizeof(*info));
		info->submitted = time(0);
		info->started = time(0);
		itable_insert(q->job_table, job_id, info);
		
		k8s_job_info *curr_job_info = create_k8s_job_info(job_id, cmd,
				extra_input_files, extra_output_files);
		
		itable_insert(k8s_job_info_table, job_id, curr_job_info); 
		return job_id;

	} else if(pid == 0) {
		
		/*
 		 * 1. kubectl create -f $mf_uuid-$count.json (create log)
    	 * 2. "kubectl get pods $mf_uuid-$count" is running
    	 * 3. kubectl cp extra_input_files $mf_uuid-$count:/
 		 */

		if(envlist) {
			jx_export(envlist);
		}

		char *pod_id = string_format("%s-%d", mf_uuid->str, job_id);
		char *k8s_config_fn = string_format("%s.json", pod_id);

		FILE *fd = fopen(k8s_config_fn, "w+");
		
		if(resources) {
			int cores = resources->cores;
			int memory = resources->memory;
			char *k8s_cpu, *k8s_memory;
			if (cores > -1 && memory > -1) {
				k8s_cpu = string_format("%dm", cores * 1000);
				k8s_memory = string_format("%dMi", memory);
			} else {
				// By default, each container will request
				// 0.5 cpu and 1024 MB ram
				k8s_cpu = string_format("%dm", 500);
				k8s_memory = string_format("%dMi", 1024);
			}
			resources_block = string_format(resource_tmpl, 
					k8s_cpu, k8s_memory, k8s_cpu, k8s_memory);	
		}

		fprintf(fd, k8s_config_tmpl, mf_uuid->str, pod_id, pod_id, k8s_image, 
				resources_block, job_id, pod_id);

		fclose(fd);

		if(access(k8s_script_file_name, F_OK | X_OK) == -1) {
			debug(D_BATCH, "Generating k8s script...");
			FILE *f = fopen(k8s_script_file_name, "w");
			fprintf(f, "%s", k8s_script);
			fclose(f);
			// Execute permissions
			chmod(k8s_script_file_name, 0755);
		}

		char *job_id_str = string_format("%d", job_id);

		execlp("/bin/bash", "bash", k8s_script_file_name, "create", pod_id, job_id_str, 
				extra_input_files, cmd, extra_output_files, (char *) NULL);
		_exit(127);

	} else {

		debug(D_BATCH, "couldn't create new process: %s\n", strerror(errno));
		return -1;

	}

	return -1;
}

static int batch_job_k8s_remove (struct batch_queue *q, batch_job_id_t jobid)
{
	
	pid_t pid = fork();
	char *pod_id = string_format("%s-%d", mf_uuid->str, (int)jobid);
	
    if(pid > 0) {

    	int status;

    	debug(D_BATCH, "Trying to remove task %d by deleting pods %s.", 
			(int)jobid, pod_id);
		
		// wait child process to complete

		if(waitpid(pid, &status, 0) == -1) {
			fatal("Failed to remove pods %s", pod_id);
		}

		if(WIFEXITED(status)) {
			debug(D_BATCH, "Successfully delete pods %s", pod_id);
		}
		
		free(pod_id);
		return 0;

    } else if(pid < 0) {

		fatal("couldn't create new process: %s\n", strerror(errno));

    } else {

		char *cmd = string_format("kubectl delete pods %s", pod_id);

		execlp("/bin/bash", "bash", "-c", cmd, (char *) 0);
		_exit(errno);

    }

    return 1;	

}

static void batch_job_k8s_handle_complete_task (char *pod_id, int job_id,
		k8s_job_info *curr_k8s_job_info, int exited_normally, 
		struct batch_job_info *info_out, struct list *running_pod_lst,
		struct batch_queue *q)
{
	struct batch_job_info *info;
	int timeout = 5;
	debug(D_BATCH, "%d is failed to execute.", job_id);
	info = itable_remove(q->job_table, job_id);
	info->exited_normally = exited_normally;
	// TODO get exact exit_code
	if (exited_normally == 0) {
		info->exit_code = 1;
	}
	memcpy(info_out, info, sizeof(*info));
	free(info);

	list_remove(running_pod_lst, pod_id);
	batch_job_k8s_remove(q, job_id);

	if(curr_k8s_job_info->is_running == 0) {
		process_wait(timeout);
	} else {
		process_wait(timeout);
		process_wait(timeout);
	}

}

static batch_job_id_t batch_job_k8s_wait (struct batch_queue * q, 
		struct batch_job_info * info_out, time_t stoptime)
{
	/*
	 * There are 5 states for a k8s job
	 * 1. pod_created
	 * 2. inps_transferred
     * 3. exec_success/exec_failed
     * 4. oups_transferred
	 * 5. job_done 
	 */

	struct list *running_pod_lst = NULL;

	while(1) {
	
		// generate the list of running pod	
		if(running_pod_lst != NULL) {
			list_free(running_pod_lst);	
			list_delete(running_pod_lst);
		}
		
		running_pod_lst = list_create();	
		
		char *cmd = string_format("kubectl get pods -l app=%s | awk \'{if (NR != 1) {print $1\" \"$3}}\' 2>&1 ", 
				mf_uuid->str);
		FILE *cmd_fp;
		char pod_info[128];
		cmd_fp = popen(cmd, "r");

		while(fgets(pod_info, sizeof(pod_info)-1, cmd_fp) != NULL) {
			char *pch, *pod_id, *pod_state;
			pch = strtok(pod_info, " \t");
			pod_id = xxstrdup(pch);
			pod_state = strtok(NULL, " \t");

			int i = 0;
			while(pod_state[i] != '\n' && pod_state[i] != EOF)
				i ++;
			if(pod_state[i] == '\n' || pod_state[i] == EOF) 
				pod_state[i] = '\0';
			

			if(strcmp(pod_state, "Running") == 0) {
				list_push_tail(running_pod_lst, (void *) pod_id);
			} 

			if(strcmp(pod_state, "Failed") == 0) {
				char *curr_job_id = xxstrdup(strrchr(pod_id, '-') + 1);	
				int curr_job_id_int = atoi(curr_job_id);
				k8s_job_info *curr_k8s_job_info = itable_lookup(k8s_job_info_table, curr_job_id_int);
				batch_job_k8s_handle_complete_task(pod_id, curr_job_id_int, 
						curr_k8s_job_info, 1, info_out, running_pod_lst, q);
				return curr_job_id_int;
			}
		}
		pclose(cmd_fp);

		// iterate the running pods
		char *curr_pod_id;
		list_first_item(running_pod_lst);	
		while((curr_pod_id = (char *)list_next_item(running_pod_lst))) {
			
			char *get_log_cmd = string_format("kubectl exec %s -- tail -1 %s.log", 
					curr_pod_id, curr_pod_id);
			FILE *cmd_fp;
			char log_tail_content[64];
			cmd_fp = popen(get_log_cmd, "r");
			fgets(log_tail_content, sizeof(log_tail_content)-1, cmd_fp);
			pclose(cmd_fp);	
			
			// trim the tailing new line
			int i = 0;
			while(log_tail_content[i] != '\n' && log_tail_content[i] != EOF)
				i ++;
			if(log_tail_content[i] == '\n' || log_tail_content[i] == EOF) 
				log_tail_content[i] = '\0';

			free(get_log_cmd);

			// get task state from the log file inside the container
			// struct batch_job_info *info;
			char *task_state;
			strtok(log_tail_content, ",");
			task_state = strtok(NULL, ",");
			
			char *curr_job_id = xxstrdup(strrchr(curr_pod_id, '-') + 1);	
			int curr_job_id_int = atoi(curr_job_id);
			k8s_job_info *curr_k8s_job_info = itable_lookup(k8s_job_info_table, curr_job_id_int);

			if(strcmp(task_state, "pod_created") == 0) {	
				// if the latest status of the pod is 
				// "pod_created", and the job is not running, 
				// then fork/exec to run the job	
				
				if (curr_k8s_job_info->is_running == 0) {
					pid_t pid = fork();
					
					if(pid > 0) {
						
						curr_k8s_job_info->is_running = 1;	
						debug(D_BATCH, "run job %s: %s in pod %s with pid %ld", curr_job_id, curr_k8s_job_info->cmd, 
								curr_pod_id, (long) pid);

					} else if (pid == 0) {

						execlp("/bin/bash", "bash", k8s_script_file_name, "exec", curr_pod_id, curr_job_id, 
								curr_k8s_job_info->extra_input_files, curr_k8s_job_info->cmd, 
								curr_k8s_job_info->extra_output_files, (char *) NULL);
						_exit(127);

					} else {

						fatal("couldn't create new process: %s\n", strerror(errno));

					}
				}

			} else if(strcmp(task_state, "job_done") == 0) {
				
				// if job finished, remove the pod from running_pod_lst
				// and delete the pod on k8s cluster
				
				batch_job_k8s_handle_complete_task(curr_pod_id, curr_job_id_int,
						curr_k8s_job_info, 1, info_out, running_pod_lst, q);
				return curr_job_id_int;

			} else if (strcmp(task_state, "exec_failed") == 0) {

				batch_job_k8s_handle_complete_task(curr_pod_id, curr_job_id_int, 
						curr_k8s_job_info, 0, info_out, running_pod_lst, q);
				return curr_job_id_int;

			} else {
				debug(D_BATCH, "%d is still running with state %s.", curr_job_id_int, task_state);
			}
				
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;

	}
}



static int batch_queue_k8s_create(struct batch_queue *q)
{
	strncpy(q->logfile, "k8s.log", sizeof(q->logfile));
    batch_queue_set_feature(q, "batch_log_name", "%s.k8slog");
    batch_queue_set_feature(q, "batch_log_transactions", "%s.tr");
	return 0;
}

batch_queue_stub_free(k8s);
batch_queue_stub_port(k8s);
batch_queue_stub_option_update(k8s);

batch_fs_stub_chdir(k8s);
batch_fs_stub_getcwd(k8s);
batch_fs_stub_mkdir(k8s);
batch_fs_stub_putfile(k8s);
batch_fs_stub_rename(k8s);
batch_fs_stub_stat(k8s);
batch_fs_stub_unlink(k8s);

const struct batch_queue_module batch_queue_k8s = {
	BATCH_QUEUE_TYPE_K8S,
	"k8s",

	batch_queue_k8s_create,
	batch_queue_k8s_free,
	batch_queue_k8s_port,
	batch_queue_k8s_option_update,

	{
		batch_job_k8s_submit,
		batch_job_k8s_wait,
		batch_job_k8s_remove,
	},

	{
		batch_fs_k8s_chdir,
		batch_fs_k8s_getcwd,
		batch_fs_k8s_mkdir,
		batch_fs_k8s_putfile,
		batch_fs_k8s_rename,
		batch_fs_k8s_stat,
		batch_fs_k8s_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
