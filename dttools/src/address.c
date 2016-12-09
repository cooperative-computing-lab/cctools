#include "address.h"
#include <string.h>
#include <arpa/inet.h>
#include <stdio.h>

int address_to_sockaddr( const char *str, int port, struct sockaddr_storage *addr, SOCKLEN_T *length )
{
	memset(addr,0,sizeof(*addr));

	struct sockaddr_in *ipv4 = (struct sockaddr_in *)addr;
	struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)addr;

	if(!str) {
		// When the address is unspecified, we are
		// attempting to bind a listening socket to
		// any avaialble address.  IN6ADDR_ANY accepts
		// both ipv4 and ipv6 binds.
		*length = sizeof(*ipv6);
		ipv6->sin6_family = AF_INET6;
		ipv6->sin6_addr = in6addr_any;
		ipv6->sin6_port = htons(port);
#if defined(CCTOOLS_OPSYS_DARWIN)
		ipv6->sin6_len = sizeof(*ipv6);
#endif
		return AF_INET6;
	} else if(inet_pton(AF_INET,str,&ipv4->sin_addr)==1) {
		*length = sizeof(*ipv4);
		ipv4->sin_family = AF_INET;
		ipv4->sin_port = htons(port);
#if defined(CCTOOLS_OPSYS_DARWIN)
		ipv4->sin_len = sizeof(*ipv4);
#endif
		return AF_INET;
	} else if(inet_pton(AF_INET6,str,&ipv6->sin6_addr)==1) {
		*length = sizeof(*ipv6);
		ipv6->sin6_family = AF_INET6;
		ipv6->sin6_port = htons(port);
#if defined(CCTOOLS_OPSYS_DARWIN)
		ipv6->sin6_len = sizeof(*ipv6);
#endif
		return AF_INET6;
	} else {
		return 0;
	}
}

static int strcount( const char *s, char c )
{
	int count=0;
	while(*s) {
		if(*s++==c) count++;
	}
	return count;
}

/*
The hostport parameter may have an optional port number
separated from the host by a colon.  The meaning of this
was clear in the IPV4 days, because the possible formats
were this:

domain.name
domain.name:1234
100.200.300.400
100.200.300.400:1234

Now that IPV6 is a possibility, parsing is complicated b/c
the address itself can contain colons.  The custom is to
surround the address with brackets when a port is given.
So, we must also catch these formats:

100:200:300::400:500
[100:200:300::400:500]:1234
*/

int address_parse_hostport( const char *hostport, char *host, int *port, int default_port )
{
	*port = default_port;

	int c = strcount(hostport,':');
	if(c==0) {
		strcpy(host,hostport);
		return 1;
	} else if(c==1) {
		if(sscanf(hostport,"%[^:]:%d",host,port)==2) {
			return 1;
		} else {
			return 0;
		}
	} else {
		if(sscanf(hostport,"[%[^]]]:%d",host,port)==2) {
			return 1;
		} else {
			strcpy(host,hostport);
			return 1;
		}
	}
}


