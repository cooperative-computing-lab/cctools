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

extern struct vine_resources *total_resources;

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
		debug(D_VINE, "No data to read from coprocess\n");
		return 0;
	}
	
	link_readline(link, len_buffer, VINE_LINE_MAX, stoptime);
	sscanf(len_buffer, "%d", &length);
	
	int current_bytes_read = 0;
	while (1)
	{
		current_bytes_read = link_read(link, buffer + bytes_read, length - bytes_read, stoptime);
		if (current_bytes_read < 0) {
			debug(D_VINE, "Read from coprocess link failed\n");
			return -1;
		}
		else if (current_bytes_read == 0) {
			debug(D_VINE, "Read from coprocess link failed: pipe closed\n");
			return -1;
		}
		bytes_read += current_bytes_read;
		if (bytes_read == length) {
			break;
		}
	}

	if (bytes_read < 0)
	{
		debug(D_VINE, "Read from coprocess failed: %s\n", strerror(errno));
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
	debug(D_VINE, "Recieved configuration from coprocess: %s\n", buffer);

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
                name = string_format("library_coprocess:%s", item->u.string_value);
            }
		}
		else {
			debug(D_VINE, "Unable to recognize key %s\n", key);
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
		debug(D_VINE, "coprocess running command %s\n", coprocess->command);
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
		setpgid(0, 0);
        execl("/bin/sh", "sh", "-c", coprocess->command, (char *) 0);
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
	int timeout = 60000000; // one minute, can be changed

	timestamp_t curr_time = timestamp_get();
	time_t stoptime = curr_time + timeout;

	int bytes_sent = link_printf(coprocess->write_link, stoptime, "%s %ld\n", function_name, strlen(function_input));
	if(bytes_sent < 0) {
		fatal("could not send input data size: %s", strerror(errno));
	}

	bytes_sent = link_printf(coprocess->write_link, stoptime, "%s\n", function_input);
	if(bytes_sent < 0) {
		fatal("could not send input data: %s", strerror(errno));
	}

	char *buffer = calloc(VINE_LINE_MAX, sizeof(char));
	memset(buffer, 0, VINE_LINE_MAX * sizeof(char));
	link_readline(coprocess->read_link, buffer, VINE_LINE_MAX, stoptime);

	return buffer;
}

struct vine_coprocess *vine_coprocess_find_state(struct list *coprocess_list, vine_coprocess_state_t state, char *coprocess_name) {
	struct vine_coprocess *coprocess;
	LIST_ITERATE(coprocess_list,coprocess){
		if (coprocess->state == state && !strcmp(coprocess->name, coprocess_name)) {
			debug(D_VINE, "Found coprocess with valid state with pid: %d\n", coprocess->pid);
			return coprocess;
		}
	}
	debug(D_VINE, "Found no valid coprocesses for state\n");
	return NULL;
}

struct vine_coprocess *vine_coprocess_initialize_coprocess(char *coprocess_command) {
	struct vine_coprocess *coprocess = malloc(sizeof(struct vine_coprocess));
	*coprocess = (struct vine_coprocess) {NULL, NULL, -1, -1, VINE_COPROCESS_UNINITIALIZED, {-1, -1}, {-1, -1}, NULL, NULL, NULL, 0, NULL};
	coprocess->command = xxstrdup(coprocess_command);
	return coprocess;
}

void vine_coprocess_specify_resources(struct vine_coprocess *coprocess) {
	coprocess->coprocess_resources = vine_resources_create();
	coprocess->coprocess_resources->cores.total  = total_resources->cores.total;
	coprocess->coprocess_resources->memory.total = total_resources->memory.total;
	coprocess->coprocess_resources->disk.total   = total_resources->disk.total;
	coprocess->coprocess_resources->gpus.total   = total_resources->gpus.total;	
}

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
		if (!resources) {
			return;
		}
		debug(D_VINE, "Measuring resources of coprocess with pid %d\n", coprocess->pid);
		debug(D_VINE, "cores: %lf, memory: %lf, disk: %lf, gpus: %lf\n", resources->cores, resources->memory + resources->swap_memory, resources->disk, resources->gpus);
		debug(D_VINE, "Max resources available to coprocess:\ncores: %"PRId64 " memory: %"PRId64 " disk: %"PRId64 " gpus: %"PRId64 "\n", coprocess->coprocess_resources->cores.total, coprocess->coprocess_resources->memory.total, coprocess->coprocess_resources->disk.total, coprocess->coprocess_resources->gpus.total);
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
		debug(D_VINE, "Coprocess with pid %d has exceeded limits, killing coprocess\n", coprocess->pid);
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
				debug(D_VINE, "Waiting on coprocess with pid %d returned an error: %s", coprocess_info[i].pid, strerror(errno));
			} else if(result>0) {
				if (!WIFEXITED(status)){
					debug(D_VINE, "Coprocess instance %d (pid %d) exited abnormally with signal %d", i, coprocess_info[i].pid, WTERMSIG(status));
				}
				else {
					debug(D_VINE, "Coprocess instance %d (pid %d) exited normally with exit code %d", i, coprocess_info[i].pid, WEXITSTATUS(status));
				}
			}
			coprocess_info[i].state = VINE_COPROCESS_DEAD;
		}
	}
	for (int i = 0; i < number_of_coprocesses; i++)
	{
		if (coprocess_info[i].state == VINE_COPROCESS_DEAD) {
			if (coprocess_info[i].num_restart_attempts >= 10) {
				debug(D_VINE, "Coprocess instance %d has died more than 10 times, no longer attempting to restart\n", i);
			}
			if (close(coprocess_info[i].pipe_in[1]) || close(coprocess_info[i].pipe_out[0])) {
				fatal("Unable to close pipes from dead coprocess: %s\n", strerror(errno));
			}
			debug(D_VINE, "Attempting to restart coprocess instance %d\n", i);
			vine_coprocess_start(coprocess_info + i);
			coprocess_info[i].num_restart_attempts++;
		}
	}
}
*/
