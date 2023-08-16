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
#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "jx_match.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <float.h>

#define MAX_BUF_SIZE 4096


static cctools_uuid_t *mf_uuid = NULL;
static const char *k8s_image = NULL;
static const char *resources_block = "";
static int count = 1;
static struct itable *k8s_job_info_table = NULL;

static const char *k8s_script = 
#include "batch_job_k8s_script.c"

static const char *k8s_script_file_name = "_temp_k8s_script.sh";
static const char *kubectl_failed_log = "kubectl_failed.log";
static const char *default_docker_image = "centos";

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
            \"args\": [\"echo %d,pod_created,$(date +\\\"%H%M%%S\\\") > %s.log ; tail -f /dev/null \"]\n\
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
	}\n\
},\n\
";

typedef struct k8s_job_info{
	int job_id;
	char *cmd;
	char *extra_input_files;
	char *extra_output_files;
	int is_running;
	int is_failed;
	char *failed_info;
	int exit_code;
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
	new_job_info->is_failed = 0;

	return new_job_info;
}

struct allocatable_resources{
	double cpu;
	double mem;
};

static struct allocatable_resources *batch_job_k8s_get_allocatable_resources()
{	
	double min_cpu = DBL_MAX;
   	double min_mem = DBL_MAX;

	char *get_nodes_info_cmd = "kubectl get nodes -o json";
	FILE *cmd_fp = popen(get_nodes_info_cmd, "r");
	if(cmd_fp == NULL) {
		return NULL;
	}

	// The format of "kubectl get nodes -o json" is 
	// {
	// 	   ...
	//     "items" : [
	//         {
	//             ...
	//             "status" : {
	//                 ...
	//                 "allocatable_resources" : {
	//                     "cpu": "2",
	//                     "mem": "1024Ki",
	//                     ...
	//                 }
	//                 ...
	//             }
	//             ...
	//          },
	//          ...
	//    ],
	//    ...
	// }
	
	struct jx *cmd_oup = jx_parse_stream(cmd_fp);
	struct jx *node_lst = jx_lookup(cmd_oup, "items");
	struct jx *node_info;
	for(void *i = NULL; (node_info = jx_iterate_array(node_lst, &i));) {
		struct jx *node_status = jx_lookup(node_info, "status");
		if(!node_status) {
			return NULL;
		}

		struct jx *allocatable_resources = jx_lookup(node_status, "allocatable");
		if(!allocatable_resources) {
			return NULL;
		}

		struct jx *allocatable_cpu = jx_lookup(allocatable_resources, "cpu");
		if(!allocatable_cpu) {
			return NULL;
		}

		struct jx *allocatable_memory = jx_lookup(allocatable_resources, "memory");
		if(!allocatable_memory) {
			return NULL;
		}

		char *cpu_str, *mem_str, *mem_str_dup;
		double cpuf, memf;
		if(!jx_match_string(allocatable_cpu, &cpu_str)) {
			return NULL;
		}
	   	cpuf = atof(cpu_str);	

		if(!jx_match_string(allocatable_memory, &mem_str)) {
			return NULL;
		}
		// The format of memroy value is "1024Ki", turncate the last two
		// characters. i.e. "K" and "i"
		mem_str_dup = xxstrdup(mem_str);
		mem_str_dup[strlen(mem_str_dup) - 2] = '\0';	
		memf = atof(mem_str_dup);
		free(mem_str_dup);

		if(cpuf < min_cpu) {
			min_cpu = cpuf;
		}

		if(memf < min_mem) {
			min_mem = memf;
		}
	}

	jx_delete(cmd_oup);

    int st = pclose(cmd_fp);
	if(!WIFEXITED(st)) {
		debug(D_BATCH, "command %s terminated abnormally\n", get_nodes_info_cmd);
		return NULL;
	}

	struct allocatable_resources *min_resource = malloc(sizeof(*min_resource));
	min_resource->cpu = min_cpu;
	min_resource->mem = min_mem;
	
	return min_resource;
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
			debug(D_BATCH, "No Docker image specified, will use %s by default", default_docker_image);
			k8s_image = default_docker_image;
			//fatal("Please specify the container image by using \"--k8s-image\"");
		}
	}
	
	if(access(kubectl_failed_log, F_OK | X_OK) == -1) {
		FILE *f = fopen(kubectl_failed_log, "w");
		// if fopen failed, we will try to resubmit
		if(!f) {
			return -1;
		}
		fclose(f);
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

		if(envlist) {
			jx_export(envlist);
		}

		char *pod_id = string_format("%s-%d", mf_uuid->str, job_id);
		char *k8s_config_fn = string_format("%s.json", pod_id);

		FILE *fd = fopen(k8s_config_fn, "w+");
		if(!fd) {
			return -1;
		}
	
		double cores = 0.0;
		double memory = 0.0;

		if(resources) {
			cores = resources->cores  > -1 ? (double)resources->cores  : cores;
			memory = resources->memory > -1 ? (double)resources->memory : memory;
		}
		
		if(batch_queue_get_option(q, "autosize")) {
			struct allocatable_resources *min_resource = batch_job_k8s_get_allocatable_resources();
			debug(D_BATCH, "Allocatable cpu: %f, Allocatable memory: %f", min_resource->cpu, min_resource->mem);
			// there are always 0.4 cpu used by daemon containers
			cores = min_resource->cpu - 0.4;
			// transfer from Ki to Mi
			memory = min_resource->mem / 1000;
			free(min_resource);
		}
		
		if (cores != 0.0 && memory != 0.0) {
			char *k8s_cpu, *k8s_memory;
				
			k8s_cpu = string_format("%f", cores);
			k8s_memory = string_format("%fMi", memory);

			resources_block = string_format(resource_tmpl, 
					k8s_cpu, k8s_memory, k8s_cpu, k8s_memory);
		}

		fprintf(fd, k8s_config_tmpl, mf_uuid->str, pod_id, pod_id, k8s_image, 
				resources_block, job_id, pod_id);

		fclose(fd);

		if(access(k8s_script_file_name, F_OK | X_OK) == -1) {
			debug(D_BATCH, "Generating k8s script...");
			FILE *f = fopen(k8s_script_file_name, "w");
			if(!f) {
				return -1;
			}
			fprintf(f, "%s", k8s_script);
			fclose(f);
			// Execute permissions
			chmod(k8s_script_file_name, 0755);
		}

		char *job_id_str = string_format("%d", job_id);

		execlp("/bin/sh", "sh", k8s_script_file_name, "create", pod_id, job_id_str, 
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
			debug(D_BATCH, "Failed to remove pods %s", pod_id);
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

		execlp("/bin/sh", "sh", "-c", cmd, (char *) 0);
		_exit(errno);

    }

    return 1;	

}

static void batch_job_k8s_handle_complete_task (char *pod_id, int job_id,
		k8s_job_info *curr_k8s_job_info, int exited_normally, int exit_code,
		struct batch_job_info *info_out, struct list *running_pod_lst,
		struct batch_queue *q)
{
	struct batch_job_info *info;
	int timeout = 5;
	info = itable_remove(q->job_table, job_id);
	info->exited_normally = exited_normally;
	if (exited_normally == 0) {
		info->exit_code = exit_code;
		debug(D_BATCH, "%d is failed to execute.", job_id);
	} else {
		debug(D_BATCH, "%d successfully complete.", job_id);
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

static k8s_job_info *batch_job_k8s_get_kubectl_failed_task()
{
	FILE *kubectl_failed_fp = fopen(kubectl_failed_log, "r");
	if(!kubectl_failed_fp) {
		return NULL;
	}	
	char failed_job_info[128];
	while(fgets(failed_job_info, sizeof(failed_job_info)-1, kubectl_failed_fp) != NULL) {
		char *pch, *job_id, *failed_info, *exit_code; 
		int job_id_int;
		pch = strtok(failed_job_info, ","); 
		job_id = xxstrdup(pch);
		job_id_int = atoi(job_id);
		free(job_id);
		k8s_job_info *curr_job_info = itable_lookup(k8s_job_info_table, job_id_int);
		if(curr_job_info->is_failed == 0) {
			curr_job_info->is_failed = 1;
			pch = strtok(NULL, ",");
			failed_info = xxstrdup(pch);
			pch = strtok(NULL, ",");
			exit_code = xxstrdup(pch);
			curr_job_info->failed_info = failed_info;
			curr_job_info->exit_code = atoi(exit_code);
			fclose(kubectl_failed_fp);
			return curr_job_info;
		} 
	}
	fclose(kubectl_failed_fp);
	return NULL;
}	
// ContainerCreating
// Terminating
static int batch_job_k8s_gen_running_pod_lst(struct list **running_pod_lst, 
		struct list **terminating_pod_lst, struct list **creating_pod_lst)
{
	if(*running_pod_lst != NULL) {
		list_free(*running_pod_lst);	
		list_delete(*running_pod_lst);
	}
	
	if(*terminating_pod_lst != NULL) {
		list_free(*terminating_pod_lst);
		list_delete(*terminating_pod_lst);
	}

	if(*creating_pod_lst != NULL) {
		list_free(*creating_pod_lst);
		list_delete(*creating_pod_lst);
	}

	*running_pod_lst = list_create();	
	*terminating_pod_lst = list_create();
	*creating_pod_lst = list_create();
	
	char *cmd = string_format("kubectl get pods --show-all -l app=%s | awk \'{if (NR != 1) {print $1\" \"$3}}\' 2>&1 ", 
			mf_uuid->str);
	FILE *cmd_fp;
	char pod_info[128];
	cmd_fp = popen(cmd, "r");
	// return -1 if popen failed
	if(!cmd_fp) {
		return -1;
	}

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
			debug(D_BATCH, "%s is Running", pod_id);
			list_push_tail(*running_pod_lst, (void *) pod_id);
		} 

		if(strcmp(pod_state, "Terminating") == 0) {
			debug(D_BATCH, "%s is being terminated", pod_id);
			list_push_tail(*terminating_pod_lst, (void *) pod_id);
		}	

		if(strcmp(pod_state, "ContainerCreating") == 0) {
			debug(D_BATCH, "%s is being created", pod_id);
			list_push_tail(*creating_pod_lst, (void *) pod_id);
		}

		if(strcmp(pod_state, "Failed") == 0 || 
				strcmp(pod_state, "OutOfcpu") == 0 ||
				strcmp(pod_state, "OutOfmemory") == 0) {
			char *curr_job_id = xxstrdup(strrchr(pod_id, '-') + 1);	
			int curr_job_id_int = atoi(curr_job_id);
			return curr_job_id_int;
		}
	}

	int st = pclose(cmd_fp);
	if(!WIFEXITED(st)) {
		debug(D_BATCH, "command %s terminated abnormally\n", cmd);
		return -1;
	}
	
	return 0;
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
	struct list *terminating_pod_lst = NULL;
	struct list *creating_pod_lst = NULL;

	while(1) {

		// 1. Check if there is a task failed because of local kubectl failure
		k8s_job_info *failed_job_info = batch_job_k8s_get_kubectl_failed_task();
		if(failed_job_info != NULL) {
			char *pod_id = string_format("%s-%d", mf_uuid->str, failed_job_info->job_id);
			batch_job_k8s_handle_complete_task(pod_id, failed_job_info->job_id, 
					failed_job_info, 0, failed_job_info->exit_code, info_out, running_pod_lst, q);
			free(pod_id);
			return failed_job_info->job_id;
		}

		// 2. Generate the list of running pod	
		int failed_job_id = batch_job_k8s_gen_running_pod_lst(&running_pod_lst, &terminating_pod_lst, &creating_pod_lst);
		// if failed_job_id == -1, batch_job_k8s_gen_running_pod_lst function call
		// failed, return -1
		if(failed_job_id == -1) {
			return failed_job_id;
		} 
		// if failed_job_id > 0, there is a job failed, return the failed_job_id
		if (failed_job_id > 0) {
			k8s_job_info *curr_k8s_job_info = itable_lookup(k8s_job_info_table, failed_job_id);
			char *pod_id = string_format("%s-%d", mf_uuid->str, failed_job_id);
			batch_job_k8s_handle_complete_task(pod_id, failed_job_id, 
					curr_k8s_job_info, 0, 1, info_out, running_pod_lst, q);
			free(pod_id);
			return failed_job_id;
		} 		

		// 3. Iterate the running pods, execute tasks that have container ready 
		char *curr_pod_id;
		int retry_get_pod_status = 0;
		list_first_item(running_pod_lst);
		debug(D_BATCH, "there are %d of running pods", list_size(running_pod_lst));
		debug(D_BATCH, "there are %d of terminating pods", list_size(terminating_pod_lst));
		debug(D_BATCH, "there are %d of creating pods", list_size(creating_pod_lst));
		while((curr_pod_id = (char *)list_next_item(running_pod_lst))) {
			
			char *get_log_cmd = string_format("kubectl exec %s -- tail -1 %s.log", 
					curr_pod_id, curr_pod_id);
			FILE *cmd_fp;
			char log_tail_content[64];
			cmd_fp = popen(get_log_cmd, "r");
			// return -1 if popen failed
			if(!cmd_fp) {
				return -1;
			}
			fgets(log_tail_content, sizeof(log_tail_content)-1, cmd_fp);
			int ret = pclose(cmd_fp);	
			// If child process terminated abnormally, we will 
			// retry it 5 times
			if(!WIFEXITED(ret)) {
				if(retry_get_pod_status < 5) {
					retry_get_pod_status ++;
					debug(D_BATCH, "command %s terminated abnormally, will retry in 10 seconds\n", get_log_cmd);
					sleep(10);
					continue;
				} else {
					debug(D_BATCH, "command %s terminated abnormally too many times\n", get_log_cmd);
					return -1;
				}
			}
			
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

						execlp("/bin/sh", "sh", k8s_script_file_name, "exec", curr_pod_id, curr_job_id, 
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
						curr_k8s_job_info, 1, 0, info_out, running_pod_lst, q);
				return curr_job_id_int;

			} else if (strcmp(task_state, "exec_failed") == 0) {
				int exit_code_int = atoi(strtok(NULL, ","));
				batch_job_k8s_handle_complete_task(curr_pod_id, curr_job_id_int, 
						curr_k8s_job_info, 0, exit_code_int, info_out, running_pod_lst, q);
				return curr_job_id_int;

			} else {
				debug(D_BATCH, "%d is still running with state %s.", curr_job_id_int, task_state);
			}
				
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;
		
		sleep(10);		

	}
}



static int batch_queue_k8s_create(struct batch_queue *q)
{
	strncpy(q->logfile, "k8s.log", sizeof(q->logfile));
    batch_queue_set_feature(q, "batch_log_name", "%s.k8slog");
    batch_queue_set_feature(q, "batch_log_transactions", "%s.tr");
	return 0;
}

static int batch_queue_k8s_free(struct batch_queue *q)
{
	char *cmd_rm_tmp_files = string_format("rm %s-*.json %s %s", 
			mf_uuid->str, k8s_script_file_name, kubectl_failed_log);
	system(cmd_rm_tmp_files);
	return 0;
	
}

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

/* vim: set noexpandtab tabstop=8: */
