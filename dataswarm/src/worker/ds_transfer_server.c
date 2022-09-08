
#include "ds_transfer_server.h"
#include "ds_protocol.h"
#include "ds_transfer.h"

#include "link.h"
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

static void ds_transfer_handler( struct link *lnk, struct ds_cache *cache )
{
	char line[DS_LINE_MAX];
	char filename_encoded[DS_LINE_MAX];
	char filename[DS_LINE_MAX];

	if(link_readline(lnk,line,sizeof(line),time(0)+command_timeout)) {
		if(sscanf(line,"get %s",filename_encoded)==1) {
			url_decode(filename_encoded,filename,sizeof(filename));
			ds_transfer_put_any(lnk,cache,filename,time(0)+transfer_timeout);
		} else {
			debug(D_DS,"invalid peer transfer message: %s\n",line);
		}
	}
}

static void ds_transfer_process( struct ds_cache *cache )
{
	while(1) {
		struct link *lnk = link_accept(transfer_link,time(0)+60);
		if(lnk) {
			ds_transfer_handler(lnk,cache);
			link_close(lnk);
		}
	}
}

void ds_transfer_server_start( struct ds_cache *cache )
{
	transfer_link = link_serve(0);

	transfer_server_pid = fork();
	if(transfer_server_pid==0) {
		// consider closing additional resources here?
		ds_transfer_process(cache);
		_exit(0);
	} else if(transfer_server_pid>0) {
		char addr[LINK_ADDRESS_MAX];
		int port;
		ds_transfer_server_address(addr,&port);
		debug(D_DS,"started transfer server pid %d listening on %s:%d",transfer_server_pid,addr,port);
		// in parent, keep going
	} else {
		fatal("unable to fork transfer server: %s",strerror(errno));
	}
}

void ds_transfer_server_stop()
{
	int status;

	debug(D_DS,"stopping transfer server pid %d",transfer_server_pid);

	link_close(transfer_link);
	kill(transfer_server_pid,SIGKILL);
	waitpid(transfer_server_pid,&status,0);

	transfer_server_pid = 0;
	transfer_link = 0;	
}

void ds_transfer_server_address( char *addr, int *port )
{
	link_address_local(transfer_link,addr,port);
}

