/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "domain_name.h"
#include "stringtools.h"
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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int domain_name_lookup_reverse( const char *addr, char *name )
{
	struct hostent *h;
	struct in_addr iaddr;

	debug(D_DNS,"looking up addr %s",addr);

	if(!string_to_ip_address(addr,(unsigned char*)&iaddr)) {
		debug(D_DNS,"%s is not a valid addr");
		return 0;
	}

	h = gethostbyaddr( (char*)&iaddr, sizeof(iaddr), AF_INET );
	if(h) strcpy(name,h->h_name);

	if(h) {
		debug(D_DNS,"%s is %s",addr,name);
		return 1;
	} else {
		debug(D_DNS,"couldn't lookup %s: %s",addr,strerror(errno));
		return 0;
	}
}

int domain_name_lookup( const char *name, char *addr )
{
	struct hostent *h;

	debug(D_DNS,"looking up name %s",name);

	h = gethostbyname( name );
	if(h) string_from_ip_address((const unsigned char*)h->h_addr,addr);

	if(h) {
		debug(D_DNS,"%s is %s",name,addr);
		return 1;
	} else {
		debug(D_DNS,"couldn't look up %s: %s",name,strerror(errno));
		return 0;
	}
}


