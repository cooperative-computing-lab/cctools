/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#include "vine_transfer_server.h"
#include "vine_protocol.h"
#include "vine_transfer.h"

#include "link.h"
#include "process.h"
#include "url_encode.h"
#include "debug.h"


#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/wait.h>

/* The initial timeout to wait for a command is short, to avoid unnecessary hangs */
static int command_timeout = 5;

/* The timeout to handle a valid transfer is much higher, to avoid false failures. */
static int transfer_timeout = 3600;

/* The server link from which connections are accepted */
static struct link *transfer_link = 0;

/* Pid of process handling peer transfers. */

pid_t transfer_server_pid = 0;

/* Handle a single request for a transfer request from a peer. */

static void vine_transfer_handler( struct link *lnk, struct vine_cache *cache )
{
	char line[VINE_LINE_MAX];
	char filename_encoded[VINE_LINE_MAX];
	char filename[VINE_LINE_MAX];

	if(link_readline(lnk,line,sizeof(line),time(0)+command_timeout)) {
		if(sscanf(line,"get %s",filename_encoded)==1) {
			url_decode(filename_encoded,filename,sizeof(filename));
			vine_transfer_put_any(lnk,cache,filename,VINE_TRANSFER_MODE_ANY,time(0)+transfer_timeout);
		} else {
			debug(D_VINE,"invalid peer transfer message: %s\n",line);
		}
	}
}

static void vine_transfer_process( struct vine_cache *cache )
{
	static int child_count = 0;
	
	/* 
	If link is real, fork. Check if we are at the max
	child count. If we are over, or link_accept timed out,
	do a blocking wait on an exited child. If we are under 
	the limit, collect all exited tasks and return to recv
	*/
	while(1) {
		struct link *lnk = link_accept(transfer_link,time(0)+10);

		child_count++;
		pid_t p = fork();
		if(p==0)
		{	
			if(lnk)
			{
				vine_transfer_handler(lnk,cache);
				link_close(lnk);
			}
			_exit(0);
		} 
		else 
		{
			if(child_count < VINE_TRANSFER_PROC_MAX_CHILD) {
				while(waitpid(-1, NULL, WNOHANG) > 0)
				{
					child_count--;
				}
				continue;
			}
		}

		debug(D_VINE, "Transfer Server: waiting on exited child. Reached %d", child_count);
		if(waitpid(-1, NULL, 0) > 0)
		{
			child_count--;
		}
	}
}

void vine_transfer_server_start( struct vine_cache *cache )
{
	transfer_link = link_serve(0);

	transfer_server_pid = fork();
	if(transfer_server_pid==0) {
		// consider closing additional resources here?
		vine_transfer_process(cache);
		_exit(0);
	} else if(transfer_server_pid>0) {
		char addr[LINK_ADDRESS_MAX];
		int port;
		vine_transfer_server_address(addr,&port);
		debug(D_VINE,"started transfer server pid %d listening on %s:%d",transfer_server_pid,addr,port);
		// in parent, keep going
	} else {
		fatal("unable to fork transfer server: %s",strerror(errno));
	}
}

void vine_transfer_server_stop()
{
	int status;

	debug(D_VINE,"stopping transfer server pid %d",transfer_server_pid);

	link_close(transfer_link);
	kill(transfer_server_pid,SIGKILL);
	waitpid(transfer_server_pid,&status,0);

	transfer_server_pid = 0;
	transfer_link = 0;	
}

void vine_transfer_server_address( char *addr, int *port )
{
	link_address_local(transfer_link,addr,port);
}

