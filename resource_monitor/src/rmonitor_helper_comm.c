/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>

#include "stringtools.h"
#include "xxmalloc.h"

#include "rmonitor_helper_comm.h"

#include "debug.h"
//#define debug fprintf
//#define D_RMON stderr


const char *str_msgtype(enum rmonitor_msg_type n)
{
	switch(n)
	{
		case BRANCH:
			return "branch";
			break;
		case END:
			return "end";
			break;
		case END_WAIT:
			return "end_wait";
			break;
		case WAIT:
			return "wait";
			break;
		case CHDIR:
			return "chdir";
			break;
		case OPEN_INPUT:
			return "open-input-file";
			break;
		case OPEN_OUTPUT:
			return "open-output-file";
			break;
		case READ:
			return "read";
			break;
		case WRITE:
			return "write";
			break;
		default:
			return "unknown";
			break;
	};
}

char *rmonitor_helper_locate(char *default_path)
{
	char *helper_path;

	debug(D_RMON,"locating helper library...\n");

	debug(D_RMON,"trying library from $%s.\n", RESOURCE_MONITOR_HELPER_ENV_VAR);
	helper_path = getenv(RESOURCE_MONITOR_HELPER_ENV_VAR);
	if(helper_path)
	{
		if(access(helper_path, R_OK|X_OK) == 0)
			return xxstrdup(helper_path);
	}

	if(default_path)
	{
		debug(D_RMON,"trying library at default path...\n");
		if(access(default_path, R_OK|X_OK) == 0)
			return xxstrdup(default_path);
	}

	debug(D_RMON,"trying library at default location.\n");
	free(helper_path);
	helper_path = string_format("%s/lib/librmonitor_helper.so", INSTALL_PATH);
	if(access(helper_path, R_OK|X_OK) == 0)
		return helper_path;

	return NULL;
}

int recv_monitor_msg(int fd, struct rmonitor_msg *msg)
{
	return recv(fd, msg, sizeof(struct rmonitor_msg), 0);
}

int send_monitor_msg(struct rmonitor_msg *msg)
{
	int rc;
	char *servname;
	struct addrinfo *addr, *addri, hints;

	servname = getenv(RESOURCE_MONITOR_INFO_ENV_VAR);
	if(!servname)
	{
		debug(D_RMON,"couldn't find socket info.\n");
		return -1;
	}
	debug(D_RMON, "found socket info at %s.", servname);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_flags = AI_NUMERICSERV;
	rc = getaddrinfo(NULL, servname, &hints, &addr);
	if (rc)
		fatal("could not getaddrinfo: %s", gai_strerror(rc));

	for (addri = addr; addri; addri = addri->ai_next) {
		int fd = socket(addri->ai_family, addri->ai_socktype, addri->ai_protocol);
		if (fd == -1) {
			debug(D_DEBUG, "skipping, could not create socket: %s", strerror(errno));
			continue;
		}

		debug(D_RMON, "sending message from %d to port %s: %s(%d)", getpid(), servname, str_msgtype(msg->type), msg->error);
		ssize_t count = sendto(fd, msg, sizeof(struct rmonitor_msg), 0, addr->ai_addr, addr->ai_addrlen);
		if (count == -1) {
			debug(D_DEBUG, "sendto failed: %s", strerror(errno));
			close(fd);
			continue;
		}
		debug(D_RMON, "message sent from %d to port %s. %zd bytes.", getpid(), servname, count);
		close(fd);
		return count;
	}
	freeaddrinfo(addr);

	return -1;
}

int rmonitor_open_socket(int *fd, int *port)
{
	int rc;
	int low = 1024;
	int high = 32767;

	const char *lowstr = getenv("TCP_LOW_PORT");
	if (lowstr)
		low = atoi(lowstr);
	const char *highstr = getenv("TCP_HIGH_PORT");
	if (highstr)
		high = atoi(highstr);

	if(high < low)
	{
		debug(D_RMON, "high port %d is less than low port %d in range", high, low);
		return 0;
	}

	for(*port = low; *port <= high; *port +=1) {
		struct addrinfo *addr, *addri, hints;
		char servname[128];

		memset(&hints, 0, sizeof(hints));
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_DGRAM;
		hints.ai_flags = AI_PASSIVE|AI_NUMERICSERV;
		snprintf(servname, sizeof(servname), "%d", *port);
		rc = getaddrinfo(NULL, servname, &hints, &addr);
		if (rc)
			fatal("could not getaddrinfo: %s", gai_strerror(rc));

		for (addri = addr; addri; addri = addri->ai_next) {
			*fd = socket(addri->ai_family, addri->ai_socktype, addri->ai_protocol);
			if (*fd == -1) {
				debug(D_DEBUG, "skipping, could not create socket: %s", strerror(errno));
				continue;
			}

			if (bind(*fd, addr->ai_addr, addr->ai_addrlen) == -1) {
				debug(D_DEBUG, "bind failed: %s", strerror(errno));
				close(*fd);
				*fd = -1;
				break; /* try different port */
			}

			debug(D_RMON,"socket open at port %d", *port);

			freeaddrinfo(addr);
			return 0;
		}
		freeaddrinfo(addr);
	}

	debug(D_RMON,"couldn't find open port for socket.");

	return 0;
}

 /* We use datagrams to send information to the monitor from the
  * great grandchildren processes */
int rmonitor_helper_init(char *lib_default_path, int *fd)
{
	int  port;
	char *helper_path = rmonitor_helper_locate(lib_default_path);
	char helper_absolute[PATH_MAX + 1];
	char *rmonitor_port;

	realpath(helper_path, helper_absolute);

	if(access(helper_absolute,R_OK|X_OK)==0) {
		debug(D_RMON, "found helper in %s\n", helper_absolute);
		rmonitor_open_socket(fd, &port);
	}
	else {
		debug(D_RMON,"couldn't find helper library %s but continuing anyway.", helper_path);
		port = -1;
	}

	if(port > 0)
	{
		rmonitor_port = string_format("%d", port);

		debug(D_RMON,"setting LD_PRELOAD to %s\n", helper_absolute);
		setenv("LD_PRELOAD", helper_absolute, 1);

		debug(D_RMON,"setting %s to %s\n", RESOURCE_MONITOR_INFO_ENV_VAR, rmonitor_port);
		setenv(RESOURCE_MONITOR_INFO_ENV_VAR, rmonitor_port, 1);

		free(rmonitor_port);
	}
	else
	{
		*fd = -1;
	}

	free(helper_path);
	return port;
}


/* vim: set noexpandtab tabstop=4: */
