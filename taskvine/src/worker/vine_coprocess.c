/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "vine_coprocess.h"
#include "vine_resources.h"
#include "vine_protocol.h"
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

#define COPROCESS_CORES_DEFAULT 1
#define COPROCESS_MEMORY_DEFAULT 500
#define COPROCESS_DISK_DEFAULT 500
#define COPROCESS_GPUS_DEFAULT 0

static int coprocess_max_timeout = 1000 * 60 * 5; // set max timeout to 5 minutes

int vine_coprocess_write_to_link(char *buffer, int len, int timeout, struct link* link)
{
	timestamp_t curr_time = timestamp_get();
	timestamp_t stoptime = curr_time + timeout;

	int bytes_sent = link_printf(link, stoptime, "%s %d\n", buffer, len);
	if(bytes_sent < 0) {
		fatal("could not send input data size: %s", strerror(errno));
	}

	// send actual data
	bytes_sent = link_write(link, buffer, len, stoptime);
	if(bytes_sent < 0) {
		fatal("could not send input data: %s", strerror(errno));
	}
	return bytes_sent;
}

int vine_coprocess_read_from_link(char *buffer, int len, int timeout, struct link* link){

	timestamp_t curr_time = timestamp_get();
	timestamp_t stoptime = curr_time + timeout;

	char len_buffer[VINE_LINE_MAX];
	int length, poll_result, bytes_read = 0;

	struct link_info coprocess_link_info[] = {{link, LINK_READ, stoptime}};
	poll_result = link_poll(coprocess_link_info, sizeof(coprocess_link_info) / sizeof(coprocess_link_info[0]), stoptime);
	if (poll_result == 0) {
		debug(D_WQ, "No data to read from coprocess\n");
		return 0;
	}
	
	link_readline(link, len_buffer, VINE_LINE_MAX, stoptime);
	sscanf(len_buffer, "%d", &length);
	
	int current_bytes_read = 0;
	while (1)
	{
		current_bytes_read = link_read(link, buffer + bytes_read, length - bytes_read, stoptime);
		if (current_bytes_read < 0) {
			debug(D_WQ, "Read from coprocess link failed\n");
			return -1;
		}
		else if (current_bytes_read == 0) {
			debug(D_WQ, "Read from coprocess link failed: pipe closed\n");
			return -1;
		}
		bytes_read += current_bytes_read;
		if (bytes_read == length) {
			break;
		}
	}

	if (bytes_read < 0)
	{
		debug(D_WQ, "Read from coprocess failed: %s\n", strerror(errno));
		return -1;
	}
	buffer[bytes_read] = '\0';

	return bytes_read;
}

int vine_coprocess_setup(struct vine_coprocess *coprocess)
{
	int bytes_read = 0;
	char buffer[VINE_LINE_MAX];
    char *name = NULL;

	bytes_read = vine_coprocess_read_from_link(buffer, VINE_LINE_MAX, coprocess_max_timeout, coprocess->read_link);
	if (bytes_read < 0) {
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
                name = string_format("duty_coprocess:%s", item->u.string_value);
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

    return 0;
}

int vine_coprocess_start(struct vine_coprocess *coprocess, char *sandbox) {
	if (pipe(coprocess->pipe_in) || pipe(coprocess->pipe_out)) { // create pipes to communicate with the coprocess
		fatal("couldn't create coprocess pipes: %s\n", strerror(errno));
	}
	coprocess->pid = fork();
	if(coprocess->pid > 0) {
		coprocess->read_link = link_attach_to_fd(coprocess->pipe_out[0]);
		coprocess->write_link = link_attach_to_fd(coprocess->pipe_in[1]);
		if (vine_coprocess_setup(coprocess)) {
			fatal("Unable to setup coprocess");
		}
		if (close(coprocess->pipe_in[0]) || close(coprocess->pipe_out[1])) {
			fatal("coprocess error parent: %s\n", strerror(errno));
		}
		debug(D_WQ, "coprocess running command %s\n", coprocess->command);
		coprocess->state = VINE_COPROCESS_READY;
        return coprocess->pid;
	}
    else if(coprocess->pid == 0) {
		if(sandbox && chdir(sandbox)) {
			fatal("could not change directory into %s: %s", sandbox, strerror(errno));
		}

        if (dup2(coprocess->pipe_in[0], 0) < 0) {
            fatal("coprocess could not attach to stdin: %s\n", strerror(errno));
        }

        if (dup2(coprocess->pipe_out[1], 1) < 0) {
            fatal("coprocess could not attach pipe to stdout: %s\n", strerror(errno));
        }
        execlp(coprocess->command, coprocess->command, (char *) 0);
        fatal("failed to execute %s: %s\n", coprocess->command, strerror(errno));
	}
    else {
        fatal("couldn't create fork coprocess: %s\n", strerror(errno));
    }

    return 0;
}

void vine_coprocess_terminate(struct vine_coprocess *coprocess) {
    process_kill_waitpid(coprocess->pid, 30);
	coprocess->state = VINE_COPROCESS_DEAD;
}

void vine_coprocess_shutdown(struct list *coprocess_list) {
	struct vine_coprocess *coprocess;
	LIST_ITERATE(coprocess_list,coprocess){
		vine_coprocess_terminate(coprocess);
	}
}

int vine_coprocess_check(struct vine_coprocess *coprocess)
{
	struct process_info *p = process_waitpid(coprocess->pid, 0);
	if (!p) {
        return 0;
	}

    free(p);
    return 1;
}

char *vine_coprocess_run(const char *function_name, const char *function_input, struct vine_coprocess *coprocess) {
	char addr[DOMAIN_NAME_MAX];
	int timeout = 60000000; // one minute, can be changed

	if(!domain_name_lookup("localhost", addr)) {
		fatal("could not lookup address of localhost");
	}

	timestamp_t curr_time = timestamp_get();
	time_t stoptime = curr_time + timeout;

	int connected = 0;
	int tries = 0;
	// retry connection for ~30 seconds
	while(!connected && tries < 30) {
		coprocess->network_link = link_connect(addr, coprocess->port, stoptime);
		if(coprocess->network_link) {
			connected = 1;
		} else {
			tries++;
			sleep(1);
		}
	}
	// if we can't connect at all, abort
	if(!coprocess->network_link) {
		fatal("connection error: %s", strerror(errno));
	}

	curr_time = timestamp_get();
	stoptime = curr_time + timeout;
	int bytes_sent = link_printf(coprocess->network_link, stoptime, "%s %ld\n", function_name, strlen(function_input));
	if(bytes_sent < 0) {
		fatal("could not send input data size: %s", strerror(errno));
	}

	char *buffer = calloc(VINE_LINE_MAX, sizeof(char));
	strcpy(buffer, function_input);
	vine_coprocess_write_to_link(buffer, strlen(function_input), timeout, coprocess->network_link);

	memset(buffer, 0, VINE_LINE_MAX * sizeof(char));
	if (vine_coprocess_read_from_link(buffer, VINE_LINE_MAX, timeout, coprocess->network_link) < 0) {
		free(buffer);
		return NULL;
	}

	return buffer;
}

struct vine_coprocess *vine_coprocess_find_state(struct list *coprocess_list, vine_coprocess_state_t state, char *coprocess_name) {
	struct vine_coprocess *coprocess;
	LIST_ITERATE(coprocess_list,coprocess){
		if (coprocess->state == state && !strcmp(coprocess->name, coprocess_name)) {
			return coprocess;
		}
	}
	return NULL;
}

struct vine_coprocess *vine_coprocess_initialize_coprocess(char *coprocess_command) {
	struct vine_coprocess *coprocess = malloc(sizeof(struct vine_coprocess));
	*coprocess = (struct vine_coprocess) {NULL, NULL, -1, -1, VINE_COPROCESS_UNINITIALIZED, {-1, -1}, {-1, -1}, NULL, NULL, NULL, 0, NULL};
	coprocess->command = xxstrdup(coprocess_command);
	return coprocess;
}

void vine_coprocess_specify_resources(struct vine_coprocess *coprocess, int coprocess_cores, int coprocess_memory, int coprocess_disk, int coprocess_gpus) {
	int coprocess_cores_normalized  = ( (coprocess_cores > 0)  ? coprocess_cores  : COPROCESS_CORES_DEFAULT);
	int coprocess_memory_normalized = ( (coprocess_memory > 0) ? coprocess_memory : COPROCESS_MEMORY_DEFAULT);
	int coprocess_disk_normalized   = ( (coprocess_disk > 0)   ? coprocess_disk   : COPROCESS_DISK_DEFAULT);
	int coprocess_gpus_normalized   = ( (coprocess_gpus > 0)   ? coprocess_gpus   : COPROCESS_GPUS_DEFAULT);
	
	coprocess->coprocess_resources = vine_resources_create();
	coprocess->coprocess_resources->cores.total  = coprocess_cores_normalized;
	coprocess->coprocess_resources->memory.total = coprocess_memory_normalized;
	coprocess->coprocess_resources->disk.total   = coprocess_disk_normalized;
	coprocess->coprocess_resources->gpus.total   = coprocess_gpus_normalized;
}

/*
struct vine_coprocess *vine_coprocess_initalize_all_coprocesses(int coprocess_cores, int coprocess_memory, int coprocess_disk, int coprocess_gpus, struct vine_resources *total_resources, char *coprocess_command, int number_of_coprocess_instances) {
	if (number_of_coprocess_instances <= 0) return NULL;
	int coprocess_cores_normalized  = ( (coprocess_cores > 0)  ? coprocess_cores  : total_resources->cores.total);
	int coprocess_memory_normalized = ( (coprocess_memory > 0) ? coprocess_memory : total_resources->memory.total);
	int coprocess_disk_normalized   = ( (coprocess_disk > 0)   ? coprocess_disk   : total_resources->disk.total);
	int coprocess_gpus_normalized   = ( (coprocess_gpus > 0)   ? coprocess_gpus   : total_resources->gpus.total);

	struct vine_coprocess * coprocess_info = malloc(sizeof(struct vine_coprocess) * number_of_coprocess_instances);
	memset(coprocess_info, 0, sizeof(struct vine_coprocess) * number_of_coprocess_instances);
	for (int coprocess_num = 0; coprocess_num < number_of_coprocess_instances; coprocess_num++){
		struct vine_coprocess *curr_coprocess = &coprocess_info[coprocess_num];
		coprocess_info[coprocess_num] = (struct vine_coprocess) {NULL, NULL, -1, -1, VINE_COPROCESS_UNINITIALIZED, {-1, -1}, {-1, -1}, NULL, NULL, NULL, 0, NULL};
		curr_coprocess->command = xxstrdup(coprocess_command);
		curr_coprocess->coprocess_resources = vine_resources_create();
		curr_coprocess->coprocess_resources->cores.total  = coprocess_cores_normalized;
		curr_coprocess->coprocess_resources->memory.total = coprocess_memory_normalized;
		curr_coprocess->coprocess_resources->disk.total   = coprocess_disk_normalized;
		curr_coprocess->coprocess_resources->gpus.total   = coprocess_gpus_normalized;
		vine_coprocess_start(curr_coprocess);
	}
	return coprocess_info;
}
*/

void vine_coprocess_shutdown_all_coprocesses(struct list *coprocess_list) {
	struct vine_coprocess *coprocess;
	if (list_size(coprocess_list) == 0) return;
	vine_coprocess_shutdown(coprocess_list);
	LIST_ITERATE(coprocess_list, coprocess){
		link_detach(coprocess->read_link);
		link_detach(coprocess->write_link);
		link_detach(coprocess->network_link);
		free(coprocess->name);
		free(coprocess->command);
		vine_resources_delete(coprocess->coprocess_resources);
	}
}

void vine_coprocess_measure_resources(struct list *coprocess_list) {
	struct vine_coprocess *coprocess;
	if (list_size(coprocess_list) == 0) return;
	LIST_ITERATE(coprocess_list, coprocess) {
		if (coprocess->state == VINE_COPROCESS_DEAD || coprocess->state == VINE_COPROCESS_UNINITIALIZED) {
			continue;
		}
		struct rmsummary *resources = rmonitor_measure_process(coprocess->pid);

		debug(D_WQ, "Measuring resources of coprocess with pid %d\n", coprocess->pid);
		debug(D_WQ, "cores: %lf, memory: %lf, disk: %lf, gpus: %lf\n", resources->cores, resources->memory + resources->swap_memory, resources->disk, resources->gpus);
		debug(D_WQ, "Max resources available to coprocess:\ncores: %"PRId64 " memory: %"PRId64 " disk: %"PRId64 " gpus: %"PRId64 "\n", coprocess->coprocess_resources->cores.total, coprocess->coprocess_resources->memory.total, coprocess->coprocess_resources->disk.total, coprocess->coprocess_resources->gpus.total);
		coprocess->coprocess_resources->cores.inuse = resources->cores;
		coprocess->coprocess_resources->memory.inuse = resources->memory + resources->swap_memory;
		coprocess->coprocess_resources->disk.inuse = resources->disk;
		coprocess->coprocess_resources->gpus.inuse = resources->gpus;

	}
}

int vine_coprocess_enforce_limit(struct vine_coprocess *coprocess) {
	if (coprocess == NULL || coprocess->state == VINE_COPROCESS_DEAD || coprocess->state == VINE_COPROCESS_UNINITIALIZED) {
		return 1;
	}
	else if (
		coprocess->coprocess_resources->cores.inuse  > coprocess->coprocess_resources->cores.total || 
		coprocess->coprocess_resources->memory.inuse > coprocess->coprocess_resources->memory.total ||
		coprocess->coprocess_resources->disk.inuse   > coprocess->coprocess_resources->disk.total ||
		coprocess->coprocess_resources->gpus.inuse   > coprocess->coprocess_resources->gpus.total) {
		debug(D_WQ, "Coprocess with pid %d has exceeded limits, killing coprocess\n", coprocess->pid);
		vine_coprocess_terminate(coprocess);
		return 0;
	}
	else {
		return 1;
	}
}

/*
void vine_coprocess_update_state(struct vine_coprocess *coprocess_info, int number_of_coprocesses) {
	for (int i = 0; i < number_of_coprocesses; i++) {
		if (vine_coprocess_check(coprocess_info + i)) {
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
			coprocess_info[i].state = VINE_COPROCESS_DEAD;
		}
	}
	for (int i = 0; i < number_of_coprocesses; i++)
	{
		if (coprocess_info[i].state == VINE_COPROCESS_DEAD) {
			if (coprocess_info[i].num_restart_attempts >= 10) {
				debug(D_WQ, "Coprocess instance %d has died more than 10 times, no longer attempting to restart\n", i);
			}
			if (close(coprocess_info[i].pipe_in[1]) || close(coprocess_info[i].pipe_out[0])) {
				fatal("Unable to close pipes from dead coprocess: %s\n", strerror(errno));
			}
			debug(D_WQ, "Attempting to restart coprocess instance %d\n", i);
			vine_coprocess_start(coprocess_info + i);
			coprocess_info[i].num_restart_attempts++;
		}
	}
}
*/
