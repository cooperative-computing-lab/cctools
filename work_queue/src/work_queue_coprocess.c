/*
Copyright (C) 2022- The University of Notre Dame
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

static int coprocess_max_timeout = 1000 * 60 * 5; // set max timeout to 5 minutes

int work_queue_coprocess_write(char *buffer, int len, int timeout, struct work_queue_coprocess *coprocess)
{
	struct pollfd read_poll = {coprocess->pipe_in[1], POLLOUT, 0};
	int poll_result = poll(&read_poll, 1, timeout);
	if (poll_result < 0)
	{
		if (errno == EINTR)
		{
			debug(D_WQ, "Polling coprocess interrupted, trying again");
			return -1;
		}
		debug(D_WQ, "Polling coprocess pipe failed: %s\n", strerror(errno));
		return -2;
	}
	if (poll_result == 0) // check for timeout
	{
		debug(D_WQ, "writing to coprocess timed out\n");
		return -3;
	}
	if ( !(read_poll.revents & POLLOUT)) // check we have data
	{
		debug(D_WQ, "Data able to be written to pipe: %s\n", strerror(errno));
		return -2;
	}
	int bytes_written = write(coprocess->pipe_in[1], buffer, len);
	if (bytes_written < 0)
	{
		if (errno == EINTR)
		{
			debug(D_WQ, "Write to coprocess interrupted\n");
			return 0;
		}
		debug(D_WQ, "Write to coprocess failed: %s\n", strerror(errno));
		return -2;
	}
	return bytes_written;
}

int work_queue_coprocess_read(char *buffer, int len, int timeout, struct work_queue_coprocess *coprocess){
	struct pollfd read_poll = {coprocess->pipe_out[0], POLLIN, 0};
	int poll_result = poll(&read_poll, 1, timeout);
	if (poll_result < 0)
	{
		if (errno == EINTR)
		{
			debug(D_WQ, "Polling coprocess interrupted, trying again");
			return -1;
		}
		debug(D_WQ, "Polling coprocess pipe failed: %s\n", strerror(errno));
		return -2;
	}
	if (poll_result == 0) // check for timeout
	{
		debug(D_WQ, "reading from coprocess timed out\n");
		return -3;
	}
	if ( !(read_poll.revents & POLLIN)) // check we have data
	{
		debug(D_WQ, "Data not returned from pipe: %s\n", strerror(errno));
		return -2;
	}

	int bytes_read = read(coprocess->pipe_out[0], buffer, len - 1);
	if (bytes_read < 0)
	{
		if (errno == EINTR)
		{
			debug(D_WQ, "Read from coprocess interrupted\n");
			return 0;
		}
		debug(D_WQ, "Read from coprocess failed: %s\n", strerror(errno));
		return -2;
	}
	buffer[bytes_read] = '\0';
	return bytes_read;
}

int work_queue_coprocess_setup(struct work_queue_coprocess *coprocess)
{
	int json_offset, json_length = -1, cumulative_bytes_read = 0, buffer_offset = 0;
	char buffer[WORK_QUEUE_LINE_MAX];
	char *envelope_size = NULL;
    char *name = NULL;

	while (1)
	{
		int curr_bytes_read = work_queue_coprocess_read(buffer + buffer_offset, 4096 - buffer_offset, coprocess_max_timeout, coprocess);
		if (curr_bytes_read < 0) {
			fatal("Unable to get information from coprocess\n");
		}
		if (curr_bytes_read == -1)
		{
			continue;
		}
		else if (curr_bytes_read == -3)
		{
			break;
		}
		cumulative_bytes_read += curr_bytes_read;

		envelope_size = memchr(buffer, '\n', cumulative_bytes_read);
		if ( envelope_size != NULL )
		{
			if (json_length == -1)
			{
				json_length = atoi(buffer);
			}
			if (json_length != -1)
			{
				json_offset = (int) (envelope_size - buffer) + 1;
				while ( json_offset < cumulative_bytes_read )
				{
					if (buffer[json_offset] == '\n')
					{
						buffer_offset = -1;
					}
					json_offset++;
				}
			}
		}
		if (buffer_offset == -1)
		{
			break;
		}
		buffer_offset += curr_bytes_read;
	}

	if ( ( (envelope_size - buffer + 1) + json_length + 1 ) != cumulative_bytes_read)
	{
		return -1;
	}

	struct jx *item, *coprocess_json = jx_parse_string(envelope_size + 1);
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

    return 0;
}

char *work_queue_coprocess_start(struct work_queue_coprocess *coprocess) {
	if (pipe(coprocess->pipe_in) || pipe(coprocess->pipe_out)) { // create pipes to communicate with the coprocess
		fatal("couldn't create coprocess pipes: %s\n", strerror(errno));
	}
	coprocess->pid = fork();
	if(coprocess->pid > 0) {
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
        execlp(coprocess->command, coprocess->command, (char *) 0);
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

char *work_queue_coprocess_run(const char *function_name, const char *function_input, struct work_queue_coprocess *coprocess) {
	char addr[DOMAIN_NAME_MAX];
	int len;
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
		coprocess->link = link_connect(addr, coprocess->port, stoptime);
		if(coprocess->link) {
			connected = 1;
		} else {
			tries++;
			sleep(1);
		}
	}
	// if we can't connect at all, abort
	if(!coprocess->link) {
		fatal("connection error: %s", strerror(errno));
	}

	len = strlen(function_input);
	curr_time = timestamp_get();
	stoptime = curr_time + timeout;
	// send the length of the data first
	int bytes_sent = link_printf(coprocess->link, stoptime, "%s %d\n", function_name, len);
	if(bytes_sent < 0) {
		fatal("could not send input data size: %s", strerror(errno));
	}

	// send actual data
	bytes_sent = link_write(coprocess->link, function_input, len, stoptime);
	if(bytes_sent < 0) {
		fatal("could not send input data: %s", strerror(errno));
	}

	curr_time = timestamp_get();
	stoptime = curr_time + timeout;
	// read in the length of the response
	char line[WORK_QUEUE_LINE_MAX];
	int length;
	link_readline(coprocess->link, line, sizeof(line), stoptime);
	sscanf(line, "output %d", &length);

	char *output = calloc(length + 1, sizeof(char));
	
	// read the response
	link_read(coprocess->link, output, length, stoptime);

	return output;
}

struct work_queue_coprocess *work_queue_coprocess_find_state(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses, work_queue_coprocess_state_t state) {
	for (int i = 0; i < number_of_coprocesses; i++) {
		if ( (coprocess_info + i)->state == state) {
			return coprocess_info + i;
		}
	}
	return NULL;
}

struct work_queue_coprocess *work_queue_coprocess_initalize_all_coprocesses(int coprocess_cores, int coprocess_memory, int coprocess_disk, int coprocess_gpus, struct work_queue_resources *total_resources, char *coprocess_command, int number_of_coprocess_instances) {
	int coprocess_cores_normalized  = ( (coprocess_cores > 0)  ? coprocess_cores  : total_resources->cores.total);
	int coprocess_memory_normalized = ( (coprocess_memory > 0) ? coprocess_memory : total_resources->memory.total);
	int coprocess_disk_normalized   = ( (coprocess_disk > 0)   ? coprocess_disk   : total_resources->disk.total);
	int coprocess_gpus_normalized   = ( (coprocess_gpus > 0)   ? coprocess_gpus   : total_resources->gpus.total);

	struct work_queue_coprocess * coprocess_info = malloc(sizeof(struct work_queue_coprocess) * number_of_coprocess_instances);
	memset(coprocess_info, 0, sizeof(struct work_queue_coprocess) * number_of_coprocess_instances);
	for (int coprocess_num = 0; coprocess_num < number_of_coprocess_instances; coprocess_num++){
		coprocess_info[coprocess_num] = (struct work_queue_coprocess) {NULL, NULL, -1, -1, WORK_QUEUE_COPROCESS_UNINITIALIZED, {-1, -1}, {-1, -1}, NULL, 0, NULL};
		coprocess_info[coprocess_num].command = xxstrdup(coprocess_command);
		coprocess_info[coprocess_num].coprocess_resources = work_queue_resources_create();
		coprocess_info[coprocess_num].coprocess_resources->cores.total  = coprocess_cores_normalized;
		coprocess_info[coprocess_num].coprocess_resources->memory.total = coprocess_memory_normalized;
		coprocess_info[coprocess_num].coprocess_resources->disk.total   = coprocess_disk_normalized;
		coprocess_info[coprocess_num].coprocess_resources->gpus.total   = coprocess_gpus_normalized;
		work_queue_coprocess_start(&coprocess_info[coprocess_num]);
	}
	return coprocess_info;
}

void work_queue_coprocess_shutdown_all_coprocesses(struct work_queue_coprocess *coprocess_info, int number_of_coprocess_instances) {
	work_queue_coprocess_shutdown(coprocess_info, number_of_coprocess_instances);
	for (int coprocess_num = 0; coprocess_num < number_of_coprocess_instances; coprocess_num++){
		free(coprocess_info[coprocess_num].name);
		free(coprocess_info[coprocess_num].command);
		work_queue_resources_delete(coprocess_info[coprocess_num].coprocess_resources);
	}
	free(coprocess_info);
}

void work_queue_coprocess_measure_resources(struct work_queue_coprocess *coprocess_info, int number_of_coprocesses) {
	for (int i = 0; i < number_of_coprocesses; i++)
	{
		if (coprocess_info[i].state == WORK_QUEUE_COPROCESS_DEAD || coprocess_info[i].state == WORK_QUEUE_COPROCESS_UNINITIALIZED) {
			continue;
		}
		struct rmsummary *resources = rmonitor_measure_process(coprocess_info[i].pid);

		debug(D_WQ, "Measuring resources of coprocess with pid %d\n", coprocess_info[i].pid);
		debug(D_WQ, "cores: %lf, memory: %lf, disk: %lf, gpus: %lf\n",    	resources->cores, 
																			resources->memory + resources->swap_memory,
																			resources->disk,
																			resources->gpus);
		debug(D_WQ, "Max resources available to coprocess:\ncores: %"PRId64 " memory: %PRId64" " disk: %PRId64" " gpus: %"PRId64 "\n",  
																				coprocess_info[i].coprocess_resources->cores.total,
																				coprocess_info[i].coprocess_resources->memory.total,
																				coprocess_info[i].coprocess_resources->disk.total,
																				coprocess_info[i].coprocess_resources->gpus.total);
		coprocess_info[i].coprocess_resources->cores.inuse = resources->cores;
		coprocess_info[i].coprocess_resources->memory.inuse = resources->memory + resources->swap_memory;
		coprocess_info[i].coprocess_resources->disk.inuse = resources->disk;
		coprocess_info[i].coprocess_resources->gpus.inuse = resources->gpus;

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
		fprintf(stdout, "Coprocess with pid %d has exceeded limits, killing coprocess\n", coprocess->pid);
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