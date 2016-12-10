#ifndef ADDRESS_H
#define ADDRESS_H

#include <netinet/in.h>
#include <sys/socket.h>

#ifndef SOCKLEN_T
#if defined(__GLIBC__) || defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_AIX) || defined(__MUSL__)
#define SOCKLEN_T socklen_t
#else
#define SOCKLEN_T int
#endif
#endif

#define IP_ADDRESS_MAX 48

int address_to_sockaddr( const char *addr, int port, struct sockaddr_storage *s, SOCKLEN_T *length );
int address_from_sockaddr( char *str, struct sockaddr *saddr );
int address_parse_hostport( const char *hostport, char *host, int *port, int default_port );

#endif
