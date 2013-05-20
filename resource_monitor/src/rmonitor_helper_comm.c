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
//#define D_RMON stderr


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
		case WAIT:
			return "wait";
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

char *monitor_helper_locate(void)
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

	debug(D_RMON,"trying library at local directory.\n");
	helper_path = string_format("./librmonitor_helper.so");
	if(access(helper_path, R_OK|X_OK) == 0)	
		return helper_path;	

	debug(D_RMON,"trying library at default location.\n");
	free(helper_path);
	helper_path = string_format("%s/lib/librmonitor_helper.so", INSTALL_PATH);
	if(access(helper_path, R_OK|X_OK) == 0)	
		return helper_path;	

	return NULL;
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
		debug(D_RMON, "couldn't resolve socket address: %s\n", strerror(errno));

	*addr = res;

	return status;
}


int send_monitor_msg(struct monitor_msg *msg)
{
	int port;
	int fd;
	char *socket_info;
	struct addrinfo *addr;

	socket_info = getenv(RESOURCE_MONITOR_INFO_ENV_VAR);
	if(!socket_info)
	{
		debug(D_RMON,"couldn't find socket info.\n");
		return -1;
	}

	sscanf(socket_info, "%d", &port); 
	debug(D_RMON, "found socket info at %d.\n", port);

	find_localhost_addr(port, &addr);

	fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
	if(fd < 0)
	{
		debug(D_RMON,"couldn't open socket for writing.");
		return -1;
	}

	int count;
	debug(D_RMON, "sending message from %d to port %d\n", getpid(), port);
	count = sendto(fd, msg, sizeof(struct monitor_msg), 0, addr->ai_addr, addr->ai_addrlen);
	debug(D_RMON, "message sent from %d to port %d. %d bytes.\n", getpid(), port, count);

	freeaddrinfo(addr);
	close(fd);

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
		debug(D_RMON, "high port %d is less than low port %d in range", high, low);
		return 0;
	}

	*fd = socket(AF_INET, SOCK_DGRAM, 0);
	if(*fd < 0)
	{
		debug(D_RMON,"couldn't open socket for reading.");
		return 0;
	}

	for(*port = low; *port <= high; *port +=1) {
		find_localhost_addr(*port, &addr);

		if(!bind(*fd, addr->ai_addr, addr->ai_addrlen))
		{
			debug(D_RMON,"socket open at port %d\n", *port);
			return *port;
		}
	}

		debug(D_RMON,"couldn't find open port for socket.");

		return 0;
}

 /* We use datagrams to send information to the monitor from the
  * great grandchildren processes */
int monitor_helper_init(char *libpath_from_cmdline, int *fd)
{
	int  port;
	char *helper_path = monitor_helper_locate();
	char helper_absolute[PATH_MAX + 1];
	char *monitor_port;

	realpath(helper_path, helper_absolute);

	if(access(helper_absolute,R_OK|X_OK)==0) {
		debug(D_RMON, "found helper in %s\n", helper_absolute);
		monitor_open_socket(fd, &port);
	}
	else {
		debug(D_RMON,"couldn't find helper library %s but continuing anyway.", helper_path);
		port = -1;
	}

	if(port > 0)
	{
		monitor_port = string_format("%d", port);

		debug(D_RMON,"setting LD_PRELOAD to %s\n", helper_absolute);
		setenv("LD_PRELOAD", helper_absolute, 1);

		debug(D_RMON,"setting %s to %s\n", RESOURCE_MONITOR_INFO_ENV_VAR, monitor_port);
		setenv(RESOURCE_MONITOR_INFO_ENV_VAR, monitor_port, 1);

		free(monitor_port);
	}
	else
	{
		*fd = -1;
	}

	free(helper_path);
	return port;
}

