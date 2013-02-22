/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

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

#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "rmonitor_helper_comm.h"

//#define debug fprintf
//#define D_DEBUG stderr


const char *str_msgtype(enum monitor_msg_type n)
{
	switch(n)
	{
		case BRANCH:
			return "branch";
			break;
		case END:
			return "end";
			break;
		case CHDIR:
			return "chdir";
			break;
		case OPEN:
			return "open-file";
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

char *monitor_helper_locate(const char *path_from_cmdline)
{
	char *helper_path, *path_from_env;

	debug(D_DEBUG,"locating helper library...\n");

	path_from_env = getenv(RMONITOR_HELPER_ENV_VAR);
	if(path_from_cmdline)	
	{
		helper_path = xxstrdup(path_from_cmdline);
		debug(D_DEBUG,"trying library from path provided at command line.\n");
	}
	else if(path_from_env)
	{
		helper_path = xxstrdup(path_from_env);
		debug(D_DEBUG,"trying library from $%s.\n", RMONITOR_HELPER_ENV_VAR);
	} 
	else 
	{
		helper_path = string_format("%s/lib/libmonitor_helper.so", INSTALL_PATH);
		debug(D_DEBUG,"trying library at default location.\n");
	}

	return helper_path;
}

int recv_monitor_msg(int fd, struct monitor_msg *msg)
{
	return recv(fd, msg, sizeof(struct monitor_msg), 0);
}

int find_localhost_addr(int port, struct addrinfo **addr)
{
	char *hostname = NULL; /* localhost */
	char *portname = string_format("%d", port);
	struct addrinfo info, *res;

	memset(&info, 0, sizeof(info));

	info.ai_family=AF_INET;
	info.ai_socktype=SOCK_DGRAM;
	info.ai_protocol=0;
	info.ai_flags=AI_ADDRCONFIG;

	int status = getaddrinfo(hostname, portname, &info, &res);
	if( status != 0)
		debug(D_DEBUG, "couldn't resolve socket address: %s\n", strerror(errno));

	*addr = res;

	return status;
}


int send_monitor_msg(struct monitor_msg *msg)
{
	int port;
	int fd;
	char *socket_info;
	struct addrinfo *addr;

	socket_info = getenv(RMONITOR_INFO_ENV_VAR);
	if(!socket_info)
	{
		debug(D_DEBUG,"couldn't find socket info.\n");
		return -1;
	}

	sscanf(socket_info, "%d", &port); 
	debug(D_DEBUG, "found socket info at %d.\n", port);

	find_localhost_addr(port, &addr);

	fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
	if(fd < 0)
	{
		debug(D_DEBUG,"couldn't open socket for writing.");
		return -1;
	}

	int count;
	debug(D_DEBUG, "sending message from %d to port %d\n", getpid(), port);
	count = sendto(fd, msg, sizeof(struct monitor_msg), 0, addr->ai_addr, addr->ai_addrlen);
	debug(D_DEBUG, "message sent from %d to port %d. %d bytes.\n", getpid(), port, count);

	freeaddrinfo(addr);

	return count;
}

int monitor_open_socket(int *fd, int *port)
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

    if(high < low)
	{
		debug(D_DEBUG, "high port %d is less than low port %d in range", high, low);
		return 0;
	}

	*fd = socket(AF_INET, SOCK_DGRAM, 0);
	if(*fd < 0)
	{
		debug(D_DEBUG,"couldn't open socket for reading.");
		return 0;
	}

	for(*port = low; *port <= high; *port +=1) {
		find_localhost_addr(*port, &addr);

		if(!bind(*fd, addr->ai_addr, addr->ai_addrlen))
		{
			debug(D_DEBUG,"socket open at port %d\n", *port);
			return *port;
		}
	}

		debug(D_DEBUG,"couldn't find open port for socket.");

		return 0;
}

 /* We use datagrams to send information to the monitor from the
  * great grandchildren processes */
int monitor_helper_init(const char *libpath_from_cmdline, int *fd)
{
	int  port;
	char *helper_path = monitor_helper_locate(libpath_from_cmdline);
	char *monitor_port;

	if(access(helper_path,R_OK|X_OK)==0) {
		debug(D_DEBUG, "found helper in %s\n", helper_path);
		monitor_open_socket(fd, &port);
	}
	else {
		debug(D_DEBUG,"couldn't find helper library %s but continuing anyway.", helper_path);
		port = -1;
	}

	if(port > 0)
	{
		monitor_port = string_format("%d", port);

		debug(D_DEBUG,"setting LD_PRELOAD to %s\n", helper_path);
		setenv("LD_PRELOAD", helper_path, 1);

		debug(D_DEBUG,"setting %s to %s\n", RMONITOR_INFO_ENV_VAR, monitor_port);
		setenv(RMONITOR_INFO_ENV_VAR, monitor_port, 1);

		free(monitor_port);
	}
	else
	{
		*fd = -1;
	}

	free(helper_path);

	return port;
}

