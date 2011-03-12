/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "link.h"
#include "domain_name.h"
#include "stringtools.h"
#include "macros.h"
#include "full_io.h"
#include "debug.h"

#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
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

#include <assert.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/poll.h>

#define BUFFER_SIZE 65536

struct link {
	int fd;
	int read;
	int written;
	time_t last_used;
	char buffer[BUFFER_SIZE];
	size_t buffer_start;
	size_t buffer_length;
	char raddr[LINK_ADDRESS_MAX];
	int rport;
};

static int link_send_window = 65536;
static int link_recv_window = 65536;
static int link_override_window = 0;

void link_window_set( int send_buffer, int recv_buffer )
{
	link_send_window = send_buffer;
	link_recv_window = recv_buffer;
}

void link_window_get( struct link *l, int *send_buffer, int *recv_buffer )
{
	socklen_t length = sizeof(*send_buffer);
	getsockopt(l->fd, SOL_SOCKET, SO_SNDBUF, (void*)send_buffer,&length);
	getsockopt(l->fd, SOL_SOCKET, SO_RCVBUF, (void*)recv_buffer,&length);
}

static void link_window_configure( struct link *l )
{
	const char *s = getenv("TCP_WINDOW_SIZE");
	if(s) {
		link_send_window = atoi(s);
		link_recv_window = atoi(s);
		link_override_window = 1;
	}

	if(link_override_window) {
		setsockopt( l->fd, SOL_SOCKET, SO_SNDBUF, (void*)&link_send_window, sizeof(link_send_window) );
		setsockopt( l->fd, SOL_SOCKET, SO_RCVBUF, (void*)&link_recv_window, sizeof(link_recv_window) );
	}
}

/*
When a link is dropped, we do not want to deal with a signal,
but we want the current system call to abort.  To accomplish this, we
send SIGPIPE to a dummy function instead of just blocking or ignoring it.
*/

static void signal_swallow( int num )
{
}

static int link_squelch()
{
	signal(SIGPIPE,signal_swallow);
	return 1;
}

int link_nonblocking( struct link *link, int onoff )
{
	int result;

	result = fcntl(link->fd,F_GETFL);
	if(result<0) return 0;

	if(onoff) {
		result |= O_NONBLOCK;
	} else {
		result &= ~(O_NONBLOCK);
	}

	result = fcntl(link->fd,F_SETFL,result);
	if(result<0) return 0;

	return 1;
}

static int errno_is_temporary( int e )
{
	if( e==EINTR || e==EWOULDBLOCK || e==EAGAIN || e==EINPROGRESS || e==EALREADY || e==EISCONN ) {
		return 1;
	} else {
		return 0;
	}
}

static int link_internal_sleep( struct link *link, struct timeval *timeout, int reading, int writing )
{
        int result;
        fd_set rfds, wfds;

        FD_ZERO(&rfds);
        if(reading) FD_SET(link->fd,&rfds);
                                                                                
        FD_ZERO(&wfds);
        if(writing) FD_SET(link->fd,&wfds);
                                                                                
        while(1) {
                result = select(link->fd+1,&rfds,&wfds,0,timeout);
                if(result>0) {
                        if( reading && FD_ISSET(link->fd,&rfds)) return 1;
                        if( writing && FD_ISSET(link->fd,&wfds)) return 1;
                } else if( result==0 ) {
                        return 0;
                } else if( errno_is_temporary(errno) ) {
                        continue;
                } else {
                        return 0;
                }
	}
}

int link_sleep( struct link *link, time_t stoptime, int reading, int writing )

{
	int timeout;
	struct timeval tm,*tptr;
	
	if(stoptime==LINK_FOREVER) {
		tptr = 0;
	} else {
		timeout = stoptime-time(0);
		if(timeout<0) {
			errno = ECONNRESET;
			return 0;
		}
		tm.tv_sec = timeout;
		tm.tv_usec = 0;
		tptr = &tm;
	}

	return link_internal_sleep(link,tptr,reading,writing);
}

int link_usleep( struct link *link, int usec, int reading, int writing )
{
        struct timeval tm;

	tm.tv_sec = 0;
	tm.tv_usec = usec;

        return link_internal_sleep(link,&tm,reading,writing);
}

static struct link * link_create()
{
	struct link *link;

	link = malloc(sizeof(*link));
	if(!link) return 0;

	link->read = link->written = 0;
	link->last_used = time(0);
	link->fd = -1;
	link->buffer_start = 0;
	link->buffer_length = 0;
	link->raddr[0] = 0;
	link->rport = 0;

	return link;
}

struct link * link_attach( int fd )
{
	struct link *l = link_create();
	if(!l) return 0;

	l->fd = fd;

	if(link_address_remote(l,l->raddr,&l->rport)) {
		debug(D_TCP,"attached to %s:%d",l->raddr,l->rport);
		return l;
	} else {
		l->fd = -1;
		link_close(l);
		return 0;
	}
}

struct link * link_serve( int port )
{
	return link_serve_address(0,port);
}

struct link * link_serve_address( const char *addr, int port )
{
	struct link *link=0;
	struct sockaddr_in address;
	int success;
	int value;

	link = link_create();
	if(!link) goto failure;

	link->fd = socket( AF_INET, SOCK_STREAM, 0 );
	if(link->fd<0) goto failure;

	value = 1;
	setsockopt( link->fd, SOL_SOCKET, SO_REUSEADDR, (void*)&value, sizeof(value) );

	link_window_configure(link);

	if(addr!=0 || port!=LINK_PORT_ANY) {

		memset(&address,0,sizeof(address));
#if !defined(CCTOOLS_OPSYS_LINUX)
		address.sin_len = sizeof(address);
#endif
		address.sin_family = AF_INET;
		address.sin_port = htons( port );

		if(addr) {
			string_to_ip_address(addr,(unsigned char*)&address.sin_addr.s_addr);
		} else {
			address.sin_addr.s_addr = htonl(INADDR_ANY);
		}

		success = bind( link->fd, (struct sockaddr *) &address, sizeof(address) );
		if(success<0) goto failure;
	}

	success = listen( link->fd, 5 );
	if(success<0) goto failure;

	if(!link_nonblocking(link,1)) goto failure;

	debug(D_TCP,"listening on port %d",port);
	return link;

	failure:
	if(link) link_close(link);
	return 0;
}

struct link * link_accept( struct link * master, time_t stoptime )
{
	struct link *link=0;

	link = link_create();
	if(!link) goto failure;

	while(1) {
		if(!link_sleep(master,stoptime,1,0)) goto failure;
		link->fd = accept(master->fd,0,0);
		break;
	}

	if(!link_nonblocking(link,1)) goto failure;
	if(!link_address_remote(link,link->raddr,&link->rport)) goto failure;
	link_squelch(link);

	debug(D_TCP,"got connection from %s:%d",link->raddr,link->rport);

	return link;

	failure:
	if(link) link_close(link);
	return 0;
}

struct link * link_connect( const char *addr, int port, time_t stoptime )
{
	struct sockaddr_in address;
	struct link *link = 0;
	int result;
	int save_errno;

	link = link_create();
	if(!link) goto failure;

	link_squelch();

	memset(&address,0,sizeof(address));
#if !defined(CCTOOLS_OPSYS_LINUX)
	address.sin_len = sizeof(address);
#endif
	address.sin_family = AF_INET;
	address.sin_port = htons(port);

	if(!string_to_ip_address(addr,(unsigned char *)&address.sin_addr)) goto failure;

	link->fd = socket( AF_INET, SOCK_STREAM, 0 );
	if(link->fd<0) goto failure;

	link_window_configure(link);

	/* sadly, cygwin does not do non-blocking connect correctly */
#ifdef CCTOOLS_OPSYS_CYGWIN
	if(!link_nonblocking(link,0)) goto failure;
#else
	if(!link_nonblocking(link,1)) goto failure;
#endif

	debug(D_TCP,"connecting to %s:%d",addr,port);

	do {
	        result = connect( link->fd, (struct sockaddr *) &address, sizeof(address) );

		/* On some platforms, errno is not set correctly. */
		/* If the remote address can be found, then we are really connected. */
		/* Also, on bsd-derived systems, failure to connect is indicated by a second connect returning EINVAL. */

		if( result<0 && !errno_is_temporary(errno) ) {
			if(errno==EINVAL) errno = ECONNREFUSED;
			break;
		}

		if(link_address_remote(link,link->raddr,&link->rport)) {

			debug(D_TCP,"made connection to %s:%d",link->raddr,link->rport);

#ifdef CCTOOLS_OPSYS_CYGWIN
			link_nonblocking(link,1);
#endif
			return link;
		}
	} while(link_sleep(link,stoptime,0,1));

	debug(D_TCP,"connection to %s:%d failed (%s)",addr,port,strerror(errno));

	failure:
	save_errno = errno;
	if(link) link_close(link);
	errno = save_errno;
	return 0;
}

static int fill_buffer( struct link *link, time_t stoptime )
{
	int chunk;

	if(link->buffer_length>0) return link->buffer_length;

	while(1) {
		chunk = read(link->fd,link->buffer,BUFFER_SIZE);
		if(chunk>0) {
			link->buffer_start = 0;
			link->buffer_length = chunk;
			return chunk;
		} else if(chunk==0) {
			link->buffer_start = 0;
			link->buffer_length = 0;
			return 0;
		} else {
			if(errno_is_temporary(errno)) {
				if(link_sleep(link,stoptime,1,0)) {
					continue;
				} else {
					return -1;
				}
			} else {
				return -1;
			}
		}
	}
}

/* link_read blocks until all the requested data is available */

int link_read( struct link *link, char *data, size_t count, time_t stoptime )
{
	ssize_t total=0;
	ssize_t chunk=0;

    if(count == 0) return 0;
    
	/* If this is a small read, attempt to fill the buffer */
	if(count<BUFFER_SIZE) {
		chunk = fill_buffer(link,stoptime);
		if(chunk<=0) return chunk;
	}

	/* Then, satisfy the read from the buffer, if any. */

	if(link->buffer_length>0) {
		chunk = MIN(link->buffer_length,count);
		memcpy(data,&link->buffer[link->buffer_start],chunk);
		data += chunk;
		total += chunk;
		count -= chunk;
		link->buffer_start += chunk;
		link->buffer_length -= chunk;
	}

	/* Otherwise, pull it all off the wire. */
		
	while(count>0) {
    	        chunk = read(link->fd,data,count);
		if(chunk<0) {
			if( errno_is_temporary(errno) ) {
				if(link_sleep(link,stoptime,1,0)) {
					continue;
				} else {
					break;
				}
			} else {
				break;
			}
		} else if(chunk==0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			data += chunk;
		}
	}

	if(total>0) {
		return total;
	} else {
		if(chunk==0) {
			return 0;
		} else {
			return -1;
		}
	}		
}

/* link_read_avail returns whatever is available, blocking only if nothing is */

int link_read_avail( struct link *link, char *data, size_t count, time_t stoptime )
{
	ssize_t total=0;
	ssize_t chunk=0;

	/* First, satisfy anything from the buffer. */

	if(link->buffer_length>0) {
		chunk = MIN(link->buffer_length,count);
		memcpy(data,&link->buffer[link->buffer_start],chunk);
		data += chunk;
		total += chunk;
		count -= chunk;
		link->buffer_start += chunk;
		link->buffer_length -= chunk;
	}

	/* Next, read what is available off the wire */
		
	while(count>0) {
    	        chunk = read(link->fd,data,count);
		if(chunk<0) {
			/* ONLY BLOCK IF NOTHING HAS BEEN READ */
			if( errno_is_temporary(errno) && total==0 ) {
				if(link_sleep(link,stoptime,1,0)) {
					continue;
				} else {
					break;
				}
			} else {
				break;
			}
		} else if(chunk==0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			data += chunk;
		}
	}

	if(total>0) {
		return total;
	} else {
		if(chunk==0) {
			return 0;
		} else {
			return -1;
		}
	}		
}

int link_readline( struct link *link, char *line, size_t length, time_t stoptime )
{
	while(1) {
		while(length>0 && link->buffer_length>0) {
			*line = link->buffer[link->buffer_start];
			link->buffer_start++;
			link->buffer_length--;
			if(*line==10) {
				*line = 0;
				return 1;
			} else if(*line==13) {
				continue;
			} else {
				line++;
				length--;
			}
		}
		if(length==0) break;
		if(fill_buffer(link,stoptime)<=0) break;
	}

	return 0;
}

int link_write( struct link *link, const char *data, size_t count, time_t stoptime )
{
	ssize_t total=0;
	ssize_t chunk=0;

	while(count>0) {
	  if (link)
		chunk = write(link->fd,data,count);
		if(chunk<0) {
			if( errno_is_temporary(errno) ) {
				if(link_sleep(link,stoptime,0,1)) {
					continue;
				} else {
					break;
				}
			} else {
				break;
			}
		} else if(chunk==0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			data += chunk;
		}
	}

	if(total>0) {
		return total;
	} else {
		if(chunk==0) {
			return 0;
		} else {
			return -1;
		}
	}		
}

int link_putlstring( struct link *link, const char *data, size_t count, time_t stoptime )
{
	size_t total = 0;
	ssize_t written = 0;

	while (count > 0 && (written = link_write(link, data, count, stoptime)) > 0) {
		count -= written;
		total += written;
		data += written;
	}
	return written < 0 ? written : total;
}

int link_putvfstring( struct link *link, const char *fmt, time_t stoptime, va_list va )
{
	va_list va2;
	size_t size = 65536;
	char buffer[size];
	char *b = buffer;

	va_copy(va2, va);
	int n = vsnprintf(NULL, 0, fmt, va2);
	va_end(va2);

	if (n < 0) return -1;
	if (n > size-1) {
		b = (char *) malloc(n+1);
		if (b == NULL) return -1;
		size = n+1;
	}

	va_copy(va2, va);
	n = vsnprintf(b, size, fmt, va2);
	assert(n >= 0);
	va_end(va2);

	int r = link_putlstring(link, b, (size_t) n, stoptime);

	if (b != buffer) free(b);

	return r;
}

int link_putfstring( struct link *link, const char *fmt, time_t stoptime, ... )
{
	va_list va;

	va_start(va, stoptime);
	int result = link_putvfstring(link, fmt, stoptime, va);
	va_end(va);

	return result;
}

void link_close( struct link *link )
{
	if(link) {
	  	if(link->fd>=0) close(link->fd);
		if(link->rport) debug(D_TCP,"disconnected from %s:%d",link->raddr,link->rport);
		free(link);
	}
}

int link_fd( struct link *link )
{
	return link->fd;
}

#if defined(__GLIBC__) || defined(CCTOOLS_OPSYS_DARWIN)
	#define SOCKLEN_T socklen_t
#else
	#define SOCKLEN_T int
#endif

int link_address_local( struct link *link, char *addr, int *port )
{
	struct sockaddr_in iaddr;
	SOCKLEN_T length;  
	int result;

	length = sizeof(iaddr);
	result = getsockname( link->fd, (struct sockaddr*) &iaddr, &length );
	if(result!=0) return 0;
       
	*port = ntohs(iaddr.sin_port);
	string_from_ip_address((unsigned char *)&iaddr.sin_addr,addr);
	
	return 1;
}

int link_address_remote( struct link *link, char *addr, int *port )
{
	struct sockaddr_in iaddr;
	SOCKLEN_T length;
	int result;

	length = sizeof(iaddr);
	result = getpeername( link->fd, (struct sockaddr*) &iaddr, &length );
	if(result!=0) return 0;

	*port = ntohs(iaddr.sin_port);
	string_from_ip_address((unsigned char *)&iaddr.sin_addr,addr);

	return 1; 
}

INT64_T link_stream_to_buffer( struct link *link, char **buffer, time_t stoptime )
{
	INT64_T buffer_size = 8192;
	INT64_T total=0;
	INT64_T actual;
	char *newbuffer;

	*buffer = malloc(buffer_size);
	if(!*buffer) return -1;

	while(1) {
		actual = link_read(link,&(*buffer)[total],buffer_size-total,stoptime);
		if(actual<=0) break;

		total += actual;

		if( (buffer_size-total)<1 ) {
			buffer_size *= 2;
			newbuffer = realloc(*buffer,buffer_size);
			if(!newbuffer) {
				free(*buffer);
				return -1;
			}
			*buffer = newbuffer;
		}
	}

	(*buffer)[total] = 0;

	return total;
}

INT64_T link_stream_to_fd( struct link *link, int fd, INT64_T length, time_t stoptime )
{
	char buffer[65536];
	INT64_T total=0;
	INT64_T ractual, wactual;

	while(length>0) {
		INT64_T chunk = MIN(sizeof(buffer),length);

		ractual = link_read(link,buffer,chunk,stoptime);
		if(ractual<=0) break;

		wactual = full_write(fd,buffer,ractual);
		if(wactual!=ractual) {
			total = -1;
			break;
		}
			
		total += ractual;
		length -= ractual;
	}

	return total;
}

INT64_T link_stream_to_file( struct link *link, FILE *file, INT64_T length, time_t stoptime )
{
	char buffer[65536];
	INT64_T total=0;
	INT64_T ractual, wactual;

	while(length>0) {
		INT64_T chunk = MIN(sizeof(buffer),length);

		ractual = link_read(link,buffer,chunk,stoptime);
		if(ractual<=0) break;

		wactual = full_fwrite(file,buffer,ractual);
		if(wactual!=ractual) {
			total = -1;
			break;
		}
			
		total += ractual;
		length -= ractual;
	}

	return total;
}

INT64_T link_stream_from_fd( struct link *link, int fd, INT64_T length, time_t stoptime )
{
	char buffer[65536];
	INT64_T total=0;
	INT64_T ractual, wactual;

	while(length>0) {
		INT64_T chunk = MIN(sizeof(buffer),length);

		ractual = full_read(fd,buffer,chunk);
		if(ractual<=0) break;

		wactual = link_write(link,buffer,ractual,stoptime);
		if(wactual!=ractual) {
			total = -1;
			break;
		}
			
		total += ractual;
		length -= ractual;
	}

	return total;
}

INT64_T link_stream_from_file( struct link *link, FILE *file, INT64_T length, time_t stoptime )
{
	char buffer[65536];
	INT64_T total=0;
	INT64_T ractual, wactual;

	while(1) {
		INT64_T chunk = MIN(sizeof(buffer),length);

		ractual = full_fread(file,buffer,chunk);
		if(ractual<=0) break;

		wactual = link_write(link,buffer,ractual,stoptime);
		if(wactual!=ractual) {
			total = -1;
			break;
		}
			
		total += ractual;
		length -= ractual;
	}

	return total;
}

INT64_T link_soak( struct link *link, INT64_T length, time_t stoptime )
{
	char buffer[65536];
	INT64_T total=0;
	INT64_T ractual;

	while(length>0) {
		INT64_T chunk = MIN(sizeof(buffer),length);

		ractual = link_read(link,buffer,chunk,stoptime);
		if(ractual<=0) break;
			
		total += ractual;
		length -= ractual;
	}

	return total;
}

int link_tune( struct link *link, link_tune_t mode )
{
	int onoff;
	int success;

	switch( mode ) {
		case LINK_TUNE_INTERACTIVE:
			onoff=1;
			break;
		case LINK_TUNE_BULK:
			onoff=0;
			break;
		default:
			return 0;
	}

	success = setsockopt( link->fd, IPPROTO_TCP, TCP_NODELAY, (void*)&onoff, sizeof(onoff) ); 
	if(success!=0) return 0;

	return 1;
}

static int link_to_poll( int events )
{
	int r = 0;
	if(events&LINK_READ)  r |= POLLIN|POLLHUP;
	if(events&LINK_WRITE) r |= POLLOUT;
	return r;
}

static int poll_to_link( int events )
{
	int r = 0;
	if(events&POLLIN)     r |= LINK_READ;
	if(events&POLLHUP)    r |= LINK_READ;
	if(events&POLLOUT)    r |= LINK_WRITE;
	return r;
}

int  link_poll( struct link_info *links, int nlinks, int msec )
{
	struct pollfd *fds = malloc(nlinks*sizeof(struct pollfd));
	int i;
	int result;

	memset(fds,0,nlinks*sizeof(struct pollfd));

	for(i=0;i<nlinks;i++) {
		fds[i].fd     = links[i].link->fd;
		fds[i].events = link_to_poll(links[i].events);
	}

	result = poll(fds,nlinks,msec);

	if(result>=0) {
		for(i=0;i<nlinks;i++) {
			links[i].revents = poll_to_link(fds[i].revents);
		}
	}

	free(fds);

	return result;
}

