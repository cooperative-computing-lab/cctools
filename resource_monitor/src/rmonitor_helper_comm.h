/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <inttypes.h>
#include "timestamp.h"

#ifndef RMONITOR_HELPER_COMM_H
#define RMONITOR_HELPER_COMM_H

#define RESOURCE_MONITOR_HELPER_ENV_VAR    "CCTOOLS_RESOURCE_MONITOR_HELPER"
#define RESOURCE_MONITOR_HELPER_STOP_SHORT "CCTOOLS_RESOURCE_MONITOR_STOP_SHORT"
#define RESOURCE_MONITOR_ROOT_PROCESS      "CCTOOLS_RESOURCE_ROOT_PROCESS"
#define RESOURCE_MONITOR_PROCESS_START     "CCTOOLS_RESOURCE_PROCESS_START"
#define RESOURCE_MONITOR_INFO_ENV_VAR      "CCTOOLS_RESOURCE_MONITOR_INFO"

// in useconds
#define RESOURCE_MONITOR_SHORT_TIME      250000

enum rmonitor_msg_type { BRANCH, WAIT, END_WAIT, END, CHDIR, OPEN_INPUT, OPEN_OUTPUT, READ, WRITE, RX, TX, SNAPSHOT };

/* BRANCH: pid of parent
 * END:    pid of child that ended
 * CHDIR:  new working directory
 * OPEN_INPUT:  path of the file opened, or "" if not a regular file.
 * OPEN_OUTPUT: path of the file opened, or "" if not a regular file.
 * READ:   Number of bytes read.
 * WRITE:  Number of bytes written.
 * RX:     Number of bytes received.
 * TX:     Number of bytes sent.
 * SNAPSHOT: snapshot name
 */

struct rmonitor_msg
{
	enum rmonitor_msg_type type;
	pid_t                  origin;
	int                    error;
	timestamp_t            start;
	timestamp_t            end;
	union {
		pid_t    p;
		uint64_t n;
		char     s[1024];
	}                     data;
};

int rmonitor_helper_init(char *path_from_cmdline, int *fd, int stop_short_running);

const char *str_msgtype(enum rmonitor_msg_type n);

int send_monitor_msg(struct rmonitor_msg *msg);
int recv_monitor_msg(int fd, struct rmonitor_msg *msg);

#endif
