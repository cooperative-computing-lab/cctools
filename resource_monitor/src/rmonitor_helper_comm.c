/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "stringtools.h"
#include "xxmalloc.h"

#include "rmonitor_helper_comm.h"

#include "debug.h"
// #define debug fprintf
// #define D_RMON stderr

const char *str_msgtype(enum rmonitor_msg_type n)
{
	switch (n) {
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
	case RX:
		return "received";
		break;
	case TX:
		return "sent";
		break;
	case SNAPSHOT:
		return "snapshot";
		break;
	default:
		return "unknown";
		break;
	};
}

char *rmonitor_helper_locate(char *default_path)
{
	char *helper_path;

	debug(D_RMON, "locating helper library...\n");

	debug(D_RMON, "trying library from $%s.\n", RESOURCE_MONITOR_HELPER_ENV_VAR);
	helper_path = getenv(RESOURCE_MONITOR_HELPER_ENV_VAR);
	if (helper_path) {
		if (access(helper_path, R_OK | X_OK) == 0)
			return xxstrdup(helper_path);
	}

	if (default_path) {
		debug(D_RMON, "trying library at default path...\n");
		if (access(default_path, R_OK | X_OK) == 0)
			return xxstrdup(default_path);
	}

	debug(D_RMON, "trying library at default location.\n");
	free(helper_path);
	helper_path = string_format("%s/lib/librmonitor_helper.so", INSTALL_PATH);
	if (access(helper_path, R_OK | X_OK) == 0)
		return helper_path;

	return NULL;
}

int recv_monitor_msg(int fd, struct rmonitor_msg *msg)
{
	int status = recv(fd, msg, sizeof(struct rmonitor_msg), MSG_DONTWAIT);
	return status;
}

int find_localhost_addr(int port, struct addrinfo **addr)
{
	char *hostname = NULL; /* localhost */
	char *portname = string_format("%d", port);
	struct addrinfo info, *res;

	memset(&info, 0, sizeof(info));

	info.ai_family = AF_INET;
	info.ai_socktype = SOCK_DGRAM;
	info.ai_protocol = 0;
	info.ai_flags = AI_ADDRCONFIG;

	int status = getaddrinfo(hostname, portname, &info, &res);
	if (status != 0)
		debug(D_RMON, "couldn't resolve socket address: %s\n", strerror(errno));

	free(portname);

	*addr = res;

	return status;
}

int rmonitor_server_open_socket(int *fd, int *port)
{
	struct addrinfo *addr;

	int low = 1024;
	int high = 32767;

	const char *lowstr = getenv("TCP_LOW_PORT");
	if (lowstr)
		low = atoi(lowstr);
	const char *highstr = getenv("TCP_HIGH_PORT");
	if (highstr)
		high = atoi(highstr);

	if (high < low) {
		debug(D_RMON, "high port %d is less than low port %d in range", high, low);
		return 0;
	}

	*fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (*fd < 0) {
		debug(D_RMON, "couldn't open socket for reading.");
		return 0;
	}

	for (*port = low; *port <= high; *port += 1) {
		int status = find_localhost_addr(*port, &addr);

		if (!bind(*fd, addr->ai_addr, addr->ai_addrlen)) {
			free(addr);
			debug(D_RMON, "socket open at port %d\n", *port);
			return *port;
		}

		if (status == 0)
			free(addr);
	}

	debug(D_RMON, "couldn't find open port for socket.");

	return 0;
}

int rmonitor_client_open_socket(int *fd, struct addrinfo **addr)
{
	int port;
	char *socket_info;
	struct addrinfo *res;

	socket_info = getenv(RESOURCE_MONITOR_INFO_ENV_VAR);
	if (!socket_info) {
		debug(D_RMON, "couldn't find socket info.\n");
		return -1;
	}

	sscanf(socket_info, "%d", &port);
	debug(D_RMON, "found socket info at %d.\n", port);

	int status = find_localhost_addr(port, &res);

	if (status != 0) {
		debug(D_RMON, "couldn't read socket information.");
		return -1;
	}

	*fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (*fd < 0) {
		debug(D_RMON, "couldn't open socket for writing.");
		freeaddrinfo(res);
		return -1;
	}

	struct timeval read_timeout = {.tv_sec = 10, .tv_usec = 0};
	setsockopt(*fd, SOL_SOCKET, SO_RCVTIMEO, (const void *)&read_timeout, sizeof(read_timeout));

	*addr = res;

	return 0;
}

/* We use datagrams to send information to the monitor from the
 * great grandchildren processes */
int rmonitor_helper_init(char *lib_default_path, int *fd, int stop_short_running)
{
	int port;
	char *helper_path = rmonitor_helper_locate(lib_default_path);
	char helper_absolute[PATH_MAX + 1];
	char *rmonitor_port;

	realpath(helper_path, helper_absolute);

	if (access(helper_absolute, R_OK | X_OK) == 0) {
		debug(D_RMON, "found helper in %s\n", helper_absolute);
		rmonitor_server_open_socket(fd, &port);
	} else {
		debug(D_RMON, "couldn't find helper library %s but continuing anyway.", helper_path);
		port = -1;
	}

	if (port > 0) {
		rmonitor_port = string_format("%d", port);

		char *prev_ldpreload = getenv("LD_PRELOAD");
		char *ld_preload = string_format("%s%s%s", helper_absolute, prev_ldpreload ? ":" : "", prev_ldpreload ? prev_ldpreload : "");

		debug(D_RMON, "setting LD_PRELOAD to %s\n", ld_preload);

		if (stop_short_running) {
			setenv(RESOURCE_MONITOR_HELPER_STOP_SHORT, "1", 1);
		}

		/* Each process sets this variable to its start time after a fork,
		 * except for the first process, which for which we set here. */
		char *start_time = string_format("%" PRId64, timestamp_get());
		setenv(RESOURCE_MONITOR_PROCESS_START, start_time, 1);
		free(start_time);

		setenv("LD_PRELOAD", ld_preload, 1);

		debug(D_RMON, "setting %s to %s\n", RESOURCE_MONITOR_INFO_ENV_VAR, rmonitor_port);
		setenv(RESOURCE_MONITOR_INFO_ENV_VAR, rmonitor_port, 1);

		free(ld_preload);
		free(rmonitor_port);
	} else {
		*fd = -1;
	}

	free(helper_path);
	return port;
}

int send_monitor_msg(struct rmonitor_msg *msg)
{
	static int fd = -1;
	static struct addrinfo *addr = NULL;

	if (fd < 0) {
		int status = rmonitor_client_open_socket(&fd, &addr);
		if (status < 0) {
			return status;
		}
	}

	int count;
	count = sendto(fd, msg, sizeof(struct rmonitor_msg), 0, addr->ai_addr, addr->ai_addrlen);

	return count;
}

/* vim: set noexpandtab tabstop=4: */
