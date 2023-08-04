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

struct vine_coprocess {
    char *command;
    char *name;
    int port;
    int pid;
    vine_coprocess_state_t state;
    int pipe_in[2];
    int pipe_out[2];
    struct link *read_link;
    struct link *write_link;
    struct link *network_link;
    int num_restart_attempts;
    struct vine_resources *coprocess_resources;
};

extern struct vine_resources *total_resources;

static int coprocess_max_timeout = 1000 * 60 * 5; // set max timeout to 5 minutes

vine_coprocess_state_t vine_coprocess_state( struct vine_coprocess *c )
{
	return c->state;
}

void vine_coprocess_state_set( struct vine_coprocess *c, vine_coprocess_state_t state )
{
	c->state = state;
}

const char * vine_coprocess_name( struct vine_coprocess *c )
{
	return c->name;
}

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

