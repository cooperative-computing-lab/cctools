/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "network.h"

#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <sys/un.h>

#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <netdb.h>
#include <errno.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>


/*
When a network connection is dropped, we do not want to deal with a signal,
but we want the current system call to abort.  To accomplish this, we
send SIGPIPE to a dummy function instead of just blocking or ignoring it.
*/

static void signal_swallow( int num )
{
}

int network_serve( int port )
{
	struct sockaddr_in address;
	int success;
	int fd;
	int on;

	signal( SIGPIPE, signal_swallow );

	fd = socket( PF_INET, SOCK_STREAM, 0 );
	if(fd<0) return -1;

	on = 1;
	setsockopt( fd, SOL_SOCKET, SO_REUSEADDR, (void*)&on, sizeof(on) );

	if(port>0) {
		address.sin_family = AF_INET;
		address.sin_port = htons( port );
		address.sin_addr.s_addr = htonl(INADDR_ANY);

		success = bind( fd, (struct sockaddr *) &address, sizeof(address) );
		if(success<0) {
			close(fd);
			return -1;
		}
	}

	success = listen( fd, 5 );
	if(success<0) {
		close(fd);
		return -1;
	}

	return fd;
}

int network_serve_local( const char *path )
{
	struct sockaddr_un address;
	int success;
	int fd;

	signal( SIGPIPE, signal_swallow );

	unlink(path);

	fd = socket( PF_UNIX, SOCK_STREAM, 0 );
	if(fd<0) return -1;

	address.sun_family = AF_UNIX;
	strcpy(address.sun_path,path);

	success = bind( fd, (struct sockaddr *) &address, sizeof(address) );
	if(success<0) {
		close(fd);
		return -1;
	}

	success = listen( fd, 5 );
	if(success<0) {
		close(fd);
		return -1;
	}

	return fd;
}

int network_accept( int master )
{
	return accept(master,0,0);
}

int network_connect( const char *host, int port )
{
	network_address addr;
	struct sockaddr_in address;
	int success;
	int fd;

	signal(SIGPIPE,signal_swallow);

	if(!network_name_to_address(host,&addr)) return -1;

	address.sin_family = AF_INET;
	address.sin_port = htons(port);
	address.sin_addr.s_addr = htonl(addr);

	fd = socket( AF_INET, SOCK_STREAM, 0 );
	if(fd<0) return -1;

	success = connect( fd, (struct sockaddr *) &address, sizeof(address) );
	if(success<0) {
		close(fd);
		return -1;
	}

	return fd;
}

int network_connect_local( const char *path )
{
	struct sockaddr_un address;
	int success;
	int fd;

	signal(SIGPIPE,signal_swallow);

	fd = socket( PF_UNIX, SOCK_STREAM, 0 );
	if(fd<0) return -1;

	address.sun_family = AF_UNIX;
	strcpy(address.sun_path,path);

	success = connect( fd, (struct sockaddr *) &address, sizeof(address) );
	if(success<0) {
		close(fd);
		return -1;
	}

	return fd;
}

int network_tune( int fd, network_tune_mode mode )
{
	int onoff;
	int success;

	switch( mode ) {
		case NETWORK_TUNE_INTERACTIVE:
			onoff=1;
			break;
		case NETWORK_TUNE_BULK:
			onoff=0;
			break;
		default:
			return 0;
	}

	success = setsockopt( fd, IPPROTO_TCP, TCP_NODELAY, (void*)&onoff, sizeof(onoff) );
	if(success!=0) return 0;

	return 1;
}

int network_sleep( int fd, int micros )
{
	int result;
	fd_set rfds;
	struct timeval timeout;
	struct timeval *t;

	if(micros!=-1) {
		timeout.tv_usec = micros%1000000;
		timeout.tv_sec = micros/1000000;
		t = &timeout;
	} else {
		t = 0;
	}

	FD_ZERO(&rfds);
	FD_SET(fd,&rfds);

	result = select(fd+1,&rfds,0,0,t);

	if(result>=0 && FD_ISSET(fd,&rfds)) {
		return 1;
	} else {
		return 0;
	}
}

int network_ok( int fd )
{
	int result;
	fd_set efds;
	struct timeval timeout;

	timeout.tv_sec = 0;
	timeout.tv_usec = 0;

	FD_ZERO(&efds);
	FD_SET(fd,&efds);

	result = select(fd+1,0,0,&efds,&timeout);

	if(result<0 || FD_ISSET(fd,&efds)) {
		return 0;
	} else {
		return 1;
	}
}

int network_read( int fd, char *data, int length )
{
	int total=0;
	int piece;

	while(total!=length) {
		piece = read(fd,&data[total],(length-total));
		if(piece>0) {
			total+=piece;
		} else {
			if(piece==-1 && errno==EINTR) {
				continue;
			} else if(piece==0) {
				errno = EPIPE;
				break;
			} else {
				break;
			}
		}
	}

	if( total==length ) {
		return 1;
	} else {
		return 0;
	}
}

int network_write( int fd, const char *data, int length )
{
	int total=0;
	int piece;

	while(total!=length) {
		piece = write(fd,&data[total],(length-total));
		if(piece>0) {
			total+=piece;
		} else {
			if(piece==-1 && errno==EINTR) {
				continue;
			} else if(piece==0) {
				errno = EPIPE;
				break;
			} else {
				break;
			}
		}
	}

	if( total==length ) {
		return 1;
	} else {
		return 0;
	}
}

void network_close( int fd )
{
	close(fd);
}

#if defined(__GLIBC__) || defined(CCTOOLS_OPSYS_DARWIN)
	#define SOCKLEN_T socklen_t
#else
	#define SOCKLEN_T int
#endif

int network_address_local( int fd, network_address *host, int *port )
{
	struct sockaddr_in addr;
	SOCKLEN_T length;
	int result;

	length = sizeof(addr);
	result = getsockname( fd, (struct sockaddr*) &addr, &length );
	if(result!=0) return 0;

	*port = ntohs(addr.sin_port);
	*host = ntohl(addr.sin_addr.s_addr);

	if(!*host) {
		return network_address_get(host);
	} else {
		return 1;
	}
}

int network_address_remote( int fd, network_address *host, int *port )
{
	struct sockaddr_in addr;
	SOCKLEN_T length;
	int result;

	length = sizeof(addr);
	result = getpeername( fd, (struct sockaddr*) &addr, &length );
	if(result!=0) return 0;

	/* Convert this into an INET localhost */
	if(addr.sin_family==AF_UNIX) {
		*port = 0;
		*host = 0x7f000001;
	} else {
		*port = ntohs(addr.sin_port);
		*host = ntohl(addr.sin_addr.s_addr);
	}

	return 1;
}

void network_address_to_string( network_address addr, char *str )
{
	unsigned char *bytes;
	bytes = (unsigned char*) &addr;

	addr = htonl(addr);

	sprintf(str,"%u.%u.%u.%u",
		(int)bytes[0],
		(int)bytes[1],
		(int)bytes[2],
		(int)bytes[3]);
}

int network_string_to_address( const char *str, network_address *addr )
{
	int fields;
	int a,b,c,d;
	unsigned char *bytes;

	fields = sscanf(str,"%d.%d.%d.%d",&a,&b,&c,&d);
	if(fields!=4) return 0;

	bytes = (unsigned char *) addr;
	bytes[0] = (unsigned char) a;
	bytes[1] = (unsigned char) b;
	bytes[2] = (unsigned char) c;
	bytes[3] = (unsigned char) d;

	*addr = ntohl(*addr);

	return 1;
}

int network_address_to_name( network_address addr, char *name )
{
	struct hostent *h;

	addr = htonl(addr);

	h = gethostbyaddr( (char*)&addr, sizeof(addr), AF_INET );

	if(h) {
		strcpy(name,h->h_name);
		return 1;
	} else {
		return 0;
	}
}

int network_name_to_address( const char *name, network_address *addr )
{
	struct hostent *h;

	h = gethostbyname( name );

	if(h) {
		memcpy(addr,h->h_addr,sizeof(*addr));
		*addr = ntohl(*addr);
		return 1;
	} else {
		return 0;
	}
}

int network_name_canonicalize( const char *name, char *cname, network_address *addr )
{
	/* First go forward to get the address */
	if(!network_name_to_address(name,addr)) return 0;

	/* Then, backwards to get the canonical name */
	if(!network_address_to_name(*addr,cname)) return 0;

	return 1;
}

/*
We will cache our address and name so we don't have to do this more than once.
*/

static network_address my_addr;
static char my_name[NETWORK_NAME_MAX];
static char my_addr_string[NETWORK_ADDR_MAX];

static int network_nameaddr_init()
{
		struct utsname name;
	int result;
	static int init_done=0;

	if(init_done) return 1;

	result = uname( &name );
	if(result<0) return 0;

	if(!network_name_canonicalize(name.nodename,my_name,&my_addr)) return 0;
	network_address_to_string(my_addr,my_addr_string);

	/*
	An improperly configured machine may have the loopback
	address (127.0.0.1) bound to the hostname, which gives us a
	practically unusable address.  Warn once if this happens.
	*/

	if(my_addr==0x7f000001) {
		fprintf(stderr,"warning: local hostname '%s' is bound to the loopback address 127.0.0.1\n",name.nodename);
	}

	return 1;
}

int network_address_get( network_address *addr )
{
	if(!network_nameaddr_init()) return 0;
	*addr = my_addr;
	return 1;
}

int network_name_get( char *name )
{
	if(!network_nameaddr_init()) return 0;
	strcpy(name,my_name);
	return 1;
}

int network_string_get( char *str )
{
	if(!network_nameaddr_init()) return 0;
	strcpy(str,my_addr_string);
	return 1;
}


/* vim: set noexpandtab tabstop=8: */
