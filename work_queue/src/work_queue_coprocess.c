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
#include "work_queue_protocol.h"

#include "debug.h"
#include "domain_name_cache.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "link.h"
#include "timestamp.h"
#include "process.h"
#include "stringtools.h"

static pid_t coprocess_pid = 0;
static int coprocess_in[2];
static int coprocess_out[2];
static int coprocess_max_timeout = 1000 * 60 * 5; // set max timeout to 5 minutes


int work_queue_coprocess_write(char *buffer, int len, int timeout)
{
	struct pollfd read_poll = {coprocess_in[1], POLLOUT, 0};
	int poll_result = poll(&read_poll, 1, timeout);
	if (poll_result < 0)
	{
		debug(D_WQ, "Write to coprocess failed: %s\n", strerror(errno));
		return -1;
	}
	if (poll_result == 0) // check for timeout
	{
		debug(D_WQ, "writing to coprocess timed out\n");
		return -1;
	}
	if ( !(read_poll.revents & POLLOUT)) // check we have data
	{
		debug(D_WQ, "Data able to be written to pipe: %s\n", strerror(errno));
		return -1;
	}
	int bytes_written = write(coprocess_in[1], buffer, len);
	if (bytes_written < 0)
	{
		debug(D_WQ, "Read from coprocess failed: %s\n", strerror(errno));
		return -2;
	}
	return bytes_written;
}

int work_queue_coprocess_read(char *buffer, int len, int timeout){
	struct pollfd read_poll = {coprocess_out[0], POLLIN, 0};
	int poll_result = poll(&read_poll, 1, timeout);
	if (poll_result < 0)
	{
		debug(D_WQ, "Read from coprocess failed: %s\n", strerror(errno));
		return -1;
	}
	if (poll_result == 0) // check for timeout
	{
		debug(D_WQ, "reading from coprocess timed out\n");
		return -3;
	}
	if ( !(read_poll.revents & POLLIN)) // check we have data
	{
		debug(D_WQ, "Data not returned from pipe: %s\n", strerror(errno));
		return -1;
	}

	int bytes_read = read(coprocess_out[0], buffer, len - 1);
	if (bytes_read < 0)
	{
		debug(D_WQ, "Read from coprocess failed: %s\n", strerror(errno));
		return -2;
	}
	buffer[bytes_read] = '\0';
	return bytes_read;
}

char *work_queue_coprocess_setup(int *coprocess_port)
{
	int json_offset, json_length = -1, cumulative_bytes_read = 0, buffer_offset = 0;
	char buffer[WORK_QUEUE_LINE_MAX];
	char *envelope_size;
    char *name = NULL;

	while (1)
	{
		int curr_bytes_read = work_queue_coprocess_read(buffer + buffer_offset, 4096 - buffer_offset, coprocess_max_timeout);
		if (curr_bytes_read < 0) {
			fatal("Unable to get information from coprocess\n");
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
				debug(D_WQ, "json_length %d\n", json_length);
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
		return NULL;
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
			*coprocess_port = atoi(temp_port);
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
    return name;
}

char *work_queue_coprocess_start(char *command, int *coprocess_port) {
	if (pipe(coprocess_in) || pipe(coprocess_out)) { // create pipes to communicate with the coprocess
		fatal("couldn't create coprocess pipes: %s\n", strerror(errno));
		return NULL;
	}
	coprocess_pid = fork();

	if(coprocess_pid > 0) {
		char *name = work_queue_coprocess_setup(coprocess_port);

		if (close(coprocess_in[0]) || close(coprocess_out[1])) {
			fatal("coprocess error parent: %s\n", strerror(errno));
		}
		debug(D_WQ, "coprocess running command %s\n", command);

        return name;
	}
    else if(coprocess_pid == 0) {
        if ( (close(coprocess_in[1]) < 0) || (close(coprocess_out[0]) < 0) ) {
            fatal("coprocess error: %s\n", strerror(errno));
        }

        if (dup2(coprocess_in[0], 0) < 0) {
            fatal("coprocess could not attach to stdin: %s\n", strerror(errno));
        }

        if (dup2(coprocess_out[1], 1) < 0) {
            fatal("coprocess could not attach pipe to stdout: %s\n", strerror(errno));
        }

        execlp(command, command, (char *) 0);
        fatal("failed to execute %s: %s\n", command, strerror(errno));
	}
    else {
        fatal("couldn't create fork coprocess: %s\n", strerror(errno));
    }

    return NULL;
}

void work_queue_coprocess_terminate() {
    process_kill_waitpid(coprocess_pid, 30);
}

int work_queue_coprocess_check()
{
	struct process_info *p = process_waitpid(coprocess_pid, 0);
	if (!p) {
        return 0;
	}

    free(p);
    return 1;
}

char *work_queue_coprocess_run(const char *function_name, const char *function_input, int coprocess_port) {
	char addr[DOMAIN_NAME_MAX];
	char buf[BUFSIZ];
	int len;
	int timeout = 60000000; // one minute, can be changed

	if(!domain_name_lookup("localhost", addr)) {
		fatal("could not lookup address of localhost");
	}

	timestamp_t curr_time = timestamp_get();
	time_t stoptime = curr_time + timeout;

	int connected = 0;
	struct link *link;
	int tries = 0;
	// retry connection for ~30 seconds
	while(!connected && tries < 30) {
		link = link_connect(addr, coprocess_port, stoptime);
		if(link) {
			connected = 1;
		} else {
			tries++;
			sleep(1);
		}
	}
	// if we can't connect at all, abort
	if(!link) {
		fatal("connection error: %s", strerror(errno));
	}

	len = strlen(function_input);
	curr_time = timestamp_get();
	stoptime = curr_time + timeout;
	// send the length of the data first
	int bytes_sent = link_printf(link, stoptime, "%s %d\n", function_name, len);
	if(bytes_sent < 0) {
		fatal("could not send input data size: %s", strerror(errno));
	}

	// send actual data
	bytes_sent = link_write(link, function_input, len, stoptime);
	if(bytes_sent < 0) {
		fatal("could not send input data: %s", strerror(errno));
	}

	memset(buf, 0, sizeof(buf));

	curr_time = timestamp_get();
	stoptime = curr_time + timeout;
	// read in the length of the response
	char line[WORK_QUEUE_LINE_MAX];
	int length;
	link_readline(link, line, sizeof(line), stoptime);
	sscanf(line, "output %d", &length);

	// read the response
	link_read(link, buf, length, stoptime);

	char *output = calloc(strlen(buf) + 1, sizeof(char));
	memcpy(output, buf, strlen(buf));

	return output;
}

