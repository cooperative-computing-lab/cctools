
#include "work_queue_process.h"
#include "work_queue.h"

#include "debug.h"
#include "errno.h"
#include "stringtools.h"
#include "create_dir.h"
#include "delete_dir.h"
#include "list.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>

#define SMALL_BUFFER_SIZE 256
#define MAX_BUFFER_SIZE 4096
#define DEFAULT_WORK_DIR "/home/worker"
#define DEFAULT_IMG "debian"
#define CONVERT_IMG "ubuntu/convert"
#define TMP_SCRIPT "tmp.sh"
#define DEFAULT_EXE_APP "#!/bin/sh"
#define CONTAINER_ID_FN "containerID"

// flag value used for switching 
// between docker mode and 
// normal mode

#define WORKER_MODE_WORKER 1
#define WORKER_MODE_DOCKER 3

// TODO check work_queue/src/barch_job_condor.c

struct work_queue_process * work_queue_process_create( int taskid, int worker_mode )
{
	struct work_queue_process *p = malloc(sizeof(*p));
	memset(p,0,sizeof(*p));
	p->task = work_queue_task_create(0);
	p->task->taskid = taskid;

	p->sandbox = string_format("t.%d",taskid);

	if(!create_dir(p->sandbox,0777)) {
		work_queue_process_delete(p, worker_mode);
		return 0;
	}
	
	return p;
}

void work_queue_process_delete( struct work_queue_process *p, int worker_mode )
{  

    if(worker_mode == WORKER_MODE_DOCKER) {

        // Get conatiner ID from the file
        char fn_path[SMALL_BUFFER_SIZE];
        sprintf(fn_path, "%s/%s", p->sandbox,CONTAINER_ID_FN);

        // TODO disable debug, can the parent process
        // just be hold? 
        debug(D_WQ, "The fn_path is %s\n", fn_path);

        if ( access(fn_path, F_OK) == -1 )
            fatal("The file holds the container ID does not exist. ");

        FILE *cont_ID_fn = fopen(fn_path, "r");
        
        fgets(p->container_id, sizeof(p->container_id)/sizeof(char), \
                cont_ID_fn); 
        
        close(fileno(cont_ID_fn));
    
        char check_status[MAX_BUFFER_SIZE];
        
		debug(D_WQ, "THE CONTAINER ID IS: %s", p->container_id);
        sprintf (check_status, "docker inspect --format \"{{.State.Pid}}\" %s", p->container_id);

		debug(D_WQ, "THE CHECK STATUS CMD IS: %s", check_status);

        FILE *container_pid = popen(check_status, "r");
        int dk_pid;
        dk_pid=fgetc(container_pid);
         
        if (dk_pid != '0') {
            // stop the container
            char stop_cmd[MAX_BUFFER_SIZE];
            sprintf (stop_cmd, "docker stop %s", p->container_id);
            system(stop_cmd);
        }
        
        // delete the container
        char delete_cmd[MAX_BUFFER_SIZE];
        sprintf (delete_cmd, "docker rm %s", p->container_id);
        system(delete_cmd);
    }

    if(p->task) work_queue_task_delete(p->task);

	if(p->output_fd) {
		close(p->output_fd);
	}

	if(p->output_file_name) {
		unlink(p->output_file_name);
		free(p->output_file_name);
	}

	if(p->sandbox) {
		//delete_dir(p->sandbox);
		free(p->sandbox);
	}

	free(p);
}

static const char task_output_template[] = "./worker.stdout.XXXXXX";

pid_t work_queue_process_execute( struct work_queue_process *p, int worker_mode )
{   
    // make warning 

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

	    debug(D_WQ, "CHECKPOINT%d\n", 2);
	    if(chdir(p->sandbox)) {
                printf("The sandbox dir is %s", p->sandbox);
			fatal("could not change directory into %s: %s", \
                        p->sandbox, strerror(errno));
	    }

            int fd = open("/dev/null", O_RDONLY);
            if (fd == -1) fatal("could not open /dev/null: %s", strerror(errno));
            int result = dup2(fd, STDIN_FILENO);
            if (result == -1) fatal("could not dup /dev/null to stdin: %s", \
            strerror(errno));
            
            result = dup2(p->output_fd, STDOUT_FILENO);
            if (result == -1) fatal("could not dup pipe to stdout: %s", \
            strerror(errno));
            
            result = dup2(p->output_fd, STDERR_FILENO);
            if (result == -1) fatal("could not dup pipe to stderr: %s", \
            strerror(errno));
            
            close(p->output_fd);

	    if(worker_mode == WORKER_MODE_DOCKER) {

	        debug(D_WQ, "CHECKPOINT%d\n", 1);
                // Get path to sandbox
                char curr_wrk_dir[MAX_BUFFER_SIZE];
                char *wrk_space;
                if ((wrk_space = getenv("WORK_QUEUE_SANDBOX")) != NULL) {
                    // TODO disable debug
                    sprintf(curr_wrk_dir, "%s/%s", wrk_space, p->sandbox);
                } else
                    perror("getenv() error");
                
                
                char mnt_flg_val[MAX_BUFFER_SIZE];
                sprintf(mnt_flg_val, "%s:%s", curr_wrk_dir, DEFAULT_WORK_DIR);
                
                // Write task command into a shell script
                char *tmp_ptr = p->task->command_line;
                int cmd_line_size = 0;
                while(*(++tmp_ptr) != '\0')
                    cmd_line_size ++;

                FILE *script_fn = fopen(TMP_SCRIPT, "w");
                fprintf(script_fn, "%s\n%s", DEFAULT_EXE_APP, p->task->command_line);
                fclose(script_fn);
                chmod(TMP_SCRIPT, 0755);

                // cmd for running the shell script
                char run_cmd[SMALL_BUFFER_SIZE];
                sprintf(run_cmd, "./%s", TMP_SCRIPT);

                uid_t uid = getuid();
                char uid_str[MAX_BUFFER_SIZE];
                sprintf(uid_str, "%d", uid);

                execl("/usr/bin/docker", "/usr/bin/docker", "run", "-v", \
                        mnt_flg_val, "-w", DEFAULT_WORK_DIR, "-u", uid_str, "--cidfile", \
                        CONTAINER_ID_FN, DEFAULT_IMG, run_cmd, (char *) 0);
                _exit(127); // Failed to execute the cmd.

            } else {

	        execlp("sh", "sh", "-c", p->task->command_line, (char *) 0);
	        _exit(127);	// Failed to execute the cmd.

            }
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

