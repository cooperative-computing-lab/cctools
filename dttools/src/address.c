#include "address.h"
#include <string.h>
#include <arpa/inet.h>

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

