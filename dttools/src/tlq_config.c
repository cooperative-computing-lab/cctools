#include "tlq_config.h"
#include "debug.h"
#include "link.h"
#include "xxmalloc.h"
#include <errno.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

char *tlq_config_url( int port, const char *log_path ) {
	char buffer[256];
	struct link *server;
	strcpy(buffer, log_path);
	server = link_connect("127.0.0.1", port, time(0) + 10);
	if(!server) {
		debug(D_NOTICE, "error opening local INET socket: %d - %s", errno, strerror(errno));
		return NULL;
	}
	
	ssize_t serv_write = link_write(server, buffer, sizeof(buffer), time(0) + 10);
	if(serv_write < 0) debug(D_NOTICE, "error writing to local INET socket: %d - %s", errno, strerror(errno));
	bzero(buffer, 256);
	int serv_read = link_read(server, buffer, sizeof(buffer), time(0) + 10);
	if(serv_read < 0) debug(D_NOTICE, "error reading from local INET socket: %d - %s", errno, strerror(errno));
	return xxstrdup(buffer);
}

/* vim: set noexpandtab tabstop=4: */
