/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>

#include "work_queue_coprocess.h"
#include "work_queue_resources.h"
#include "work_queue_protocol.h"
#include "rmsummary.h"
#include "rmonitor_poll.h"

#include "debug.h"
#include "domain_name_cache.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "link.h"
#include "timestamp.h"
#include "process.h"
#include "stringtools.h"
#include "xxmalloc.h"

static time_t coprocess_connect_timeout = 60;   // max time to connect, one minute
static time_t coprocess_execute_timeout = 3600; // max time to execute, one hour

/* Read back a fixed size message consisting of a length header, then the data itself. */

char * work_queue_coprocess_read_message( struct link *link, time_t stoptime )
{
	char line[WORK_QUEUE_LINE_MAX];
	int length;
	
	if(!link_readline(link, line, WORK_QUEUE_LINE_MAX, stoptime)) {
		return 0;
	}
			
	sscanf(line, "%d", &length);

	char *buffer = malloc(length+1);
	
	int result = link_read(link,buffer,length,stoptime);

	if(result==length) {
		buffer[length] = 0;
		return buffer;
	} else {
		free(buffer);
		return 0;
	}
}

int work_queue_coprocess_setup(struct work_queue_coprocess *coprocess)
{
	char *name = NULL;

	char * buffer = work_queue_coprocess_read_message(coprocess->read_link,time(0)+coprocess_connect_timeout);
	if(!buffer) {
		fatal("Unable to get information from coprocess\n");
	}

	struct jx *item, *coprocess_json = jx_parse_string(buffer);
	void *i = NULL;
	const char *key;
	while ((item = jx_iterate_values(coprocess_json, &i))) {
		key = jx_get_key(&i);
		if (key == NULL) {
			continue;
		}
		if (!strcmp(key, "name")) {
            if(item->type == JX_STRING) {
                name = string_format("wq_worker_coprocess:%s", item->u.string_value);
            }
		}
		else if (!strcmp(key, "port")) {
			char *temp_port = jx_print_string(item);
			coprocess->port = atoi(temp_port);
			free(temp_port);
		}
		else {
			debug(D_WQ, "Unable to recognize key %s\n", key);
		}
	}

	jx_delete(coprocess_json);

    if(!name) {
		fatal("couldn't find \"name\" in coprocess configuration\n");
    }

	coprocess->name = name;

	free(buffer);
	
    return 0;
}

char *work_queue_coprocess_start(struct work_queue_coprocess *coprocess) {
	if (pipe(coprocess->pipe_in) || pipe(coprocess->pipe_out)) { // create pipes to communicate with the coprocess
		fatal("couldn't create coprocess pipes: %s\n", strerror(errno));
	}
	coprocess->pid = fork();
	if(coprocess->pid > 0) {
		coprocess->read_link = link_attach_to_fd(coprocess->pipe_out[0]);
		coprocess->write_link = link_attach_to_fd(coprocess->pipe_in[1]);
		if (work_queue_coprocess_setup(coprocess)) {
			fatal("Unable to setup coprocess");
		}
		if (close(coprocess->pipe_in[0]) || close(coprocess->pipe_out[1])) {
			fatal("coprocess error parent: %s\n", strerror(errno));
		}
		debug(D_WQ, "coprocess running command %s\n", coprocess->command);
		coprocess->state = WORK_QUEUE_COPROCESS_READY;
        return coprocess->name;
	}
    else if(coprocess->pid == 0) {
        if ( (close(coprocess->pipe_in[1]) < 0) || (close(coprocess->pipe_out[0]) < 0) ) {
            fatal("coprocess error: %s\n", strerror(errno));
        }

        if (dup2(coprocess->pipe_in[0], 0) < 0) {
            fatal("coprocess could not attach to stdin: %s\n", strerror(errno));
        }

        if (dup2(coprocess->pipe_out[1], 1) < 0) {
            fatal("coprocess could not attach pipe to stdout: %s\n", strerror(errno));
        }
        execl("/bin/sh", "sh", "-c", coprocess->command, (char *) 0);
        fatal("failed to execute %s: %s\n", coprocess->command, strerror(errno));
	}
    else {
        fatal("couldn't create fork coprocess: %s\n", strerror(errno));
    }

    return NULL;
}

void work_queue_coprocess_terminate(struct work_queue_coprocess *coprocess) {
    process_kill_waitpid(coprocess->pid, 30);
	coprocess->state = WORK_QUEUE_COPROCESS_DEAD;
}

void work_queue_coprocess_shutdown(struct work_queue_coprocess *coprocess_info, int num_coprocesses) {
	for (int coprocess_index = 0; coprocess_index < num_coprocesses; coprocess_index++) {
		work_queue_coprocess_terminate(coprocess_info + coprocess_index);
	}
}

int work_queue_coprocess_check(struct work_queue_coprocess *coprocess)
{
	struct process_info *p = process_waitpid(coprocess->pid, 0);
	if (!p) {
        return 0;
	}

    free(p);
    return 1;
}

/* Invoke a function by connecting, sending the invocation, and reading back the result. */

char *work_queue_coprocess_run(const char *function_name, const char *function_input, struct work_queue_coprocess *coprocess, int task_id)
{
	int connect_stoptime = time(0) + coprocess_connect_timeout;
	int execute_stoptime = time(0) + coprocess_execute_timeout;

	/* Connect to the coprocess if we haven't already done so. */
	if(!coprocess->network_link) {
		coprocess->network_link = link_connect("127.0.0.1",coprocess->port,connect_stoptime);
		if(!coprocess->network_link) {
			debug(D_WQ,"failed to connect to coprocess: %s",strerror(errno));
			return 0;
		}
	}

	/* Send the invocation header indicating the function name and length of input. */
	link_printf(coprocess->network_link, connect_stoptime, "%s %d %ld\n", function_name, task_id, strlen(function_input));
	/* Followed by the function_input itself. */
	link_write(coprocess->network_link, function_input, strlen(function_input), connect_stoptime );

	/* Read back the result buffer with a longer timeout. */
	char *result = work_queue_coprocess_read_message(coprocess->network_link,execute_stoptime);

	/* If the invocation did not work, close the link and return failure. */
	if(!result) {
		link_close(coprocess->network_link);
		coprocess->network_link = 0;
	}

	return result;
}

struct work_queue_coprocess *work_queue_coprocess_find_state(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses, work_queue_coprocess_state_t state) {
	for (int i = 0; i < number_of_coprocesses; i++) {
		if ( (coprocess_info + i)->state == state) {
			return coprocess_info + i;
		}
	}
	return NULL;
}

struct work_queue_coprocess *work_queue_coprocess_initalize_all_coprocesses(int coprocess_cores, int coprocess_memory, int coprocess_disk, int coprocess_gpus, struct work_queue_resources *total_resources, struct work_queue_resources *coprocess_resources, char *coprocess_command, int number_of_coprocess_instances) {
	if (number_of_coprocess_instances <= 0) return NULL;
	int coprocess_cores_normalized  = ( (coprocess_cores > 0)  ? coprocess_cores  : total_resources->cores.total);
	int coprocess_memory_normalized = ( (coprocess_memory > 0) ? coprocess_memory : total_resources->memory.total);
	int coprocess_disk_normalized   = ( (coprocess_disk > 0)   ? coprocess_disk   : total_resources->disk.total);
	int coprocess_gpus_normalized   = ( (coprocess_gpus > 0)   ? coprocess_gpus   : total_resources->gpus.total);

	coprocess_resources->cores.total = coprocess_cores_normalized;
	coprocess_resources->memory.total = coprocess_memory_normalized;
	coprocess_resources->disk.total = coprocess_disk_normalized;
	coprocess_resources->gpus.total = coprocess_gpus_normalized;

	struct work_queue_coprocess * coprocess_info = malloc(sizeof(struct work_queue_coprocess) * number_of_coprocess_instances);
	memset(coprocess_info, 0, sizeof(struct work_queue_coprocess) * number_of_coprocess_instances);
	for (int coprocess_num = 0; coprocess_num < number_of_coprocess_instances; coprocess_num++){
		struct work_queue_coprocess *curr_coprocess = &coprocess_info[coprocess_num];
		coprocess_info[coprocess_num] = (struct work_queue_coprocess) {NULL, NULL, -1, -1, WORK_QUEUE_COPROCESS_UNINITIALIZED, {-1, -1}, {-1, -1}, NULL, NULL, NULL, 0, NULL};
		curr_coprocess->command = xxstrdup(coprocess_command);
		curr_coprocess->coprocess_resources = work_queue_resources_create();
		curr_coprocess->coprocess_resources->cores.total  = coprocess_cores_normalized;
		curr_coprocess->coprocess_resources->memory.total = coprocess_memory_normalized;
		curr_coprocess->coprocess_resources->disk.total   = coprocess_disk_normalized;
		curr_coprocess->coprocess_resources->gpus.total   = coprocess_gpus_normalized;
		work_queue_coprocess_start(curr_coprocess);
	}
	return coprocess_info;
}

void work_queue_coprocess_shutdown_all_coprocesses(struct work_queue_coprocess *coprocess_info, struct work_queue_resources *coprocess_resources, int number_of_coprocess_instances) {
	if (number_of_coprocess_instances <= 0) return;
	work_queue_coprocess_shutdown(coprocess_info, number_of_coprocess_instances);
	for (int coprocess_num = 0; coprocess_num < number_of_coprocess_instances; coprocess_num++){
		struct work_queue_coprocess *curr_coprocess = &coprocess_info[coprocess_num];
		link_detach(curr_coprocess->read_link);
		link_detach(curr_coprocess->write_link);
		link_detach(curr_coprocess->network_link);
		free(curr_coprocess->name);
		free(curr_coprocess->command);
		work_queue_resources_delete(curr_coprocess->coprocess_resources);
	}
	free(coprocess_info);
	work_queue_resources_delete(coprocess_resources);
}

void work_queue_coprocess_measure_resources(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses) {
	if (number_of_coprocesses <= 0) return;
	for (int i = 0; i < number_of_coprocesses; i++)
	{
		struct work_queue_coprocess *curr_coprocess = &coprocess_info[i];
		if (curr_coprocess->state == WORK_QUEUE_COPROCESS_DEAD || curr_coprocess->state == WORK_QUEUE_COPROCESS_UNINITIALIZED) {
			continue;
		}
		struct rmsummary *resources = rmonitor_measure_process(curr_coprocess->pid);

		if(!resources)
			continue;

		debug(D_WQ, "Measuring resources of coprocess with pid %d\n", curr_coprocess->pid);
		debug(D_WQ, "cores: %lf, memory: %lf, disk: %lf, gpus: %lf\n", resources->cores, resources->memory + resources->swap_memory, resources->disk, resources->gpus);
		debug(D_WQ, "Max resources available to coprocess:\ncores: %"PRId64 " memory: %"PRId64 " disk: %"PRId64 " gpus: %"PRId64 "\n", curr_coprocess->coprocess_resources->cores.total, curr_coprocess->coprocess_resources->memory.total, curr_coprocess->coprocess_resources->disk.total, curr_coprocess->coprocess_resources->gpus.total);
		curr_coprocess->coprocess_resources->cores.inuse = resources->cores;
		curr_coprocess->coprocess_resources->memory.inuse = resources->memory + resources->swap_memory;
		curr_coprocess->coprocess_resources->disk.inuse = resources->disk;
		curr_coprocess->coprocess_resources->gpus.inuse = resources->gpus;

	}
}

int work_queue_coprocess_enforce_limit(struct work_queue_coprocess *coprocess) {
	if (coprocess == NULL || coprocess->state == WORK_QUEUE_COPROCESS_DEAD || coprocess->state == WORK_QUEUE_COPROCESS_UNINITIALIZED) {
		return 1;
	}
	else if (
		coprocess->coprocess_resources->cores.inuse  > coprocess->coprocess_resources->cores.total || 
		coprocess->coprocess_resources->memory.inuse > coprocess->coprocess_resources->memory.total ||
		coprocess->coprocess_resources->disk.inuse   > coprocess->coprocess_resources->disk.total ||
		coprocess->coprocess_resources->gpus.inuse   > coprocess->coprocess_resources->gpus.total) {
		debug(D_WQ, "Coprocess with pid %d has exceeded limits, killing coprocess\n", coprocess->pid);
		work_queue_coprocess_terminate(coprocess);
		return 0;
	}
	else {
		return 1;
	}
}

void work_queue_coprocess_update_state(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses) {
	for (int i = 0; i < number_of_coprocesses; i++) {
		if (work_queue_coprocess_check(coprocess_info + i)) {
			int status;
			int result = waitpid(coprocess_info[i].pid, &status, 0);
			if(result==0) {
				fatal("Coprocess instace %d has both terminated and not terminated\n", i);
			} else if(result<0) {
				debug(D_WQ, "Waiting on coprocess with pid %d returned an error: %s", coprocess_info[i].pid, strerror(errno));
			} else if(result>0) {
				if (!WIFEXITED(status)){
					debug(D_WQ, "Coprocess instance %d (pid %d) exited abnormally with signal %d", i, coprocess_info[i].pid, WTERMSIG(status));
				}
				else {
					debug(D_WQ, "Coprocess instance %d (pid %d) exited normally with exit code %d", i, coprocess_info[i].pid, WEXITSTATUS(status));
				}
			}
			coprocess_info[i].state = WORK_QUEUE_COPROCESS_DEAD;
		}
	}
	for (int i = 0; i < number_of_coprocesses; i++)
	{
		if (coprocess_info[i].state == WORK_QUEUE_COPROCESS_DEAD) {
			if (coprocess_info[i].num_restart_attempts >= 10) {
				debug(D_WQ, "Coprocess instance %d has died more than 10 times, no longer attempting to restart\n", i);
			}
			if (close(coprocess_info[i].pipe_in[1]) || close(coprocess_info[i].pipe_out[0])) {
				fatal("Unable to close pipes from dead coprocess: %s\n", strerror(errno));
			}
			debug(D_WQ, "Attempting to restart coprocess instance %d\n", i);
			work_queue_coprocess_start(coprocess_info + i);
			coprocess_info[i].num_restart_attempts++;
		}
	}
}
