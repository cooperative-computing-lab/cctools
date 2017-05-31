#include "batch_job.h"
#include "batch_job_internal.h"
#include "debug.h"
#include "process.h"
#include "macros.h"
#include "stringtools.h"
#include "uuid.h"
#include "path.h"
#include "list.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#define MAX_BUF_SIZE 4096

static cctools_uuid_t *mf_uuid = NULL;
static const char *k8s_image = NULL;
static int count = 1;

static const char *k8s_config_tmpl = "\
{\n\
    \"apiVersion\": \"v1\",\n\
    \"kind\": \"Pod\",\n\
    \"metadata\": {\n\
        \"labels\": {\n\
            \"app\": \"makeflow\"\n\
        },\n\
        \"name\": \"%s\"\n\
    },\n\
\n\
    \"spec\": {\n\
        \"containers\": [{\n\
            \"name\": \"%s\",\n\
            \"image\": \"%s\",\n\
            \"imagePullPolicy\": \"IfNotPresent\",\n\
            \"command\": [\"/bin/bash\", \"-c\"],\n\
            \"args\": [\"echo \\\"%d,start_container\\\" > %s.log ; tail -f /dev/null \"]\n\
        }],\n\
        \"restartPolicy\": \"Never\"\n\
    }\n\
}\n\
";

static batch_job_id_t batch_job_k8s_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct jx *envlist, const struct rmsummary *resources )
{
	
	if(mf_uuid == NULL) {
		mf_uuid = malloc(sizeof(*mf_uuid));
		cctools_uuid_create (mf_uuid);
		// The pod id cannot include upper case
		string_tolower(mf_uuid->str);
	}

	if(k8s_image == NULL) {
		if((k8s_image = batch_queue_get_option(q, "k8s-image")) == NULL) {
			fatal("Please specify the container image by using \"--k8s-image\"");
		}
	}

	int job_id;
	
	job_id = count ++;


	debug(D_BATCH, "started job %d: %s", job_id, cmd);
	struct batch_job_info *info = calloc(1, sizeof(*info));
	info->submitted = time(0);
	info->started = time(0);
	itable_insert(q->job_table, job_id, info);

	if(envlist) {
		jx_export(envlist);
	}

	/*
 	 * 1. kubectl create -f $mf_uuid-$count.json (create log)
     * 2. "kubectl get pods $mf_uuid-$count" is running
     * 3. kubectl cp extra_input_files $mf_uuid-$count:/
 	 */
	
	char *pod_id = string_format("%s-%d", mf_uuid->str, job_id);	
	char *k8s_config_fn = string_format("%s.json", pod_id);

	FILE *fd = fopen(k8s_config_fn, "w+");
	fprintf(fd, k8s_config_tmpl, pod_id, pod_id, k8s_image, job_id, pod_id);
	fclose(fd);

	char exe_path[MAX_BUF_SIZE];

	if(readlink("/proc/self/exe", exe_path, MAX_BUF_SIZE) == -1) {
		fatal("read \"proc/self/exe\" fail\n");
	}

	char exe_dir_path[MAX_BUF_SIZE];
	path_dirname(exe_path, exe_dir_path);

	char *sh_cmd = string_format("/bin/bash %s/batch_job_k8s_script.sh %s %d \"%s\" \"%s\" \"%s\"", exe_dir_path, pod_id, job_id, extra_input_files, cmd, extra_output_files);

	system(sh_cmd);
	return job_id;
}

static batch_job_id_t batch_job_k8s_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
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
	
		uint64_t curr_job_id;
		void *curr_job_info;
		if(running_pod_lst != NULL) {
			list_free(running_pod_lst);	
			list_delete(running_pod_lst);
		}
		
		running_pod_lst = list_create();	

		// generate the list of running pod	
		itable_firstkey(q->job_table);	

		while(itable_nextkey(q->job_table, &curr_job_id, (void **) &curr_job_info)) {
			char *pod_id = string_format("%s-%d", mf_uuid->str, (int)curr_job_id);
			char *cmd = string_format("kubectl get pods %s | awk \'{if (NR != 1) {print $3}}\' 2>&1 ", pod_id);
			FILE *cmd_fp;
			char pod_state[64];
			cmd_fp = popen(cmd, "r");
			fgets(pod_state, sizeof(pod_state)-1, cmd_fp);
			pclose(cmd_fp);

			int i = 0;
			while(pod_state[i] != '\n' && pod_state[i] != EOF)
				i ++;
			if(pod_state[i] == '\n' || pod_state[i] == EOF) 
				pod_state[i] = '\0';

			free(cmd);
			free(pod_id);
			
			if(strcmp(pod_state, "Running") == 0) {
				uint64_t *job_id_dup = malloc(sizeof(uint64_t));
				*job_id_dup = curr_job_id;
				list_push_tail(running_pod_lst,	(void *)job_id_dup);
			}
		}

		// iterate the running pods
		int *job_id;
		list_first_item(running_pod_lst);	
		while((job_id = (int *)list_next_item(running_pod_lst))) {
			
			char *get_log_cmd = string_format("kubectl exec %s-%d -- tail -1 %s-%d.log", 
					mf_uuid->str, *job_id, mf_uuid->str, *job_id);
			FILE *cmd_fp;
			char log_tail_content[64];
			cmd_fp = popen(get_log_cmd, "r");
			fgets(log_tail_content, sizeof(log_tail_content)-1, cmd_fp);
			pclose(cmd_fp);	

			int i = 0;
			while(log_tail_content[i] != '\n' && log_tail_content[i] != EOF)
				i ++;
			if(log_tail_content[i] == '\n' || log_tail_content[i] == EOF) 
				log_tail_content[i] = '\0';

			free(get_log_cmd);

			struct batch_job_info *info;
			char *task_state;
			strtok(log_tail_content, ",");
			task_state = strtok(NULL, ",");

			if(strcmp(task_state, "job_done") == 0) {
				
				info = itable_remove(q->job_table, *job_id);
				info->exited_normally = 1;
				memcpy(info_out, info, sizeof(*info));
				free(info);
				return *job_id;

			} else if (strcmp(task_state, "exec_failed") == 0) {

				info = itable_remove(q->job_table, *job_id);
				info->exited_normally = 0;
				// TODO get exact exit_code
				info->exit_code = 1;
				debug(D_BATCH, "%d is failed to execute.", *job_id);
				memcpy(info_out, info, sizeof(*info));
				free(info);
				return *job_id;

			} else {

				debug(D_BATCH, "%d is still running with state %s.", *job_id, task_state);

			}
				
		}

		if(stoptime != 0 && time(0) >= stoptime)
			return -1;

	}
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

static int batch_queue_k8s_create (struct batch_queue *q)
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
		batch_fs_k8s_stat,
		batch_fs_k8s_unlink,
	},
};

/* vim: set noexpandtab tabstop=4: */
