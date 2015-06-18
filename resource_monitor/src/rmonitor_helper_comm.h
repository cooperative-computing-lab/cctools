/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <inttypes.h>

#ifndef RMONITOR_HELPER_COMM_H
#define RMONITOR_HELPER_COMM_H

#define RESOURCE_MONITOR_HELPER_ENV_VAR "CCTOOLS_RESOURCE_MONITOR_HELPER"
#define RESOURCE_MONITOR_INFO_ENV_VAR   "CCTOOLS_RESOURCE_MONITOR_INFO"

enum rmonitor_msg_type { BRANCH, WAIT, END_WAIT, END, CHDIR, OPEN, READ, WRITE };

/* BRANCH: pid of parent
 * END:    pid of child that ended
 * CHDIR:  new working directory
 * OPEN:   path of the file opened, or "" if not a regular file.
 * READ:   Number of bytes read.
 * WRITE:  Number of bytes written.
 */

struct rmonitor_msg
{
	enum rmonitor_msg_type type;
	pid_t                 origin;
	union {
		pid_t    p;
		uint64_t n;
		char     s[1024];
	}                     data;
};

int rmonitor_helper_init(char *path_from_cmdline, int *fd);

const char *str_msgtype(enum rmonitor_msg_type n);

int send_monitor_msg(struct rmonitor_msg *msg);
int recv_monitor_msg(int fd, struct rmonitor_msg *msg);

#endif
