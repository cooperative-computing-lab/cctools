/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <inttypes.h>

#ifndef RMONITOR_HELPER_COMM_H
#define RMONITOR_HELPER_COMM_H

#define RMONITOR_HELPER_ENV_VAR "CCTOOLS_RESOURCE_MONITOR_HELPER"
#define RMONITOR_INFO_ENV_VAR   "CCTOOLS_RESOURCE_MONITOR_INFO"

enum monitor_msg_type { BRANCH, END, CHDIR, OPEN, READ, WRITE };

/* BRANCH: pid of parent 
 * END:    pid of child that ended
 * CHDIR:  new working directory
 * OPEN:   path of the file opened, or "" if not a regular file.
 * READ:   Number of bytes read.
 * WRITE:  Number of bytes written.
 */

struct monitor_msg
{
	enum monitor_msg_type type;
	pid_t                 origin;
	union {
		pid_t    p; 
		uint64_t n;
		char     s[1024]; 
	}                     data;
};

int monitor_helper_init(const char *path_from_cmdline, int *fd);

const char *str_msgtype(enum monitor_msg_type n);

int send_monitor_msg(struct monitor_msg *msg);
int recv_monitor_msg(int fd, struct monitor_msg *msg);

#endif
