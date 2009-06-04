/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef NETWORK_H
#define NETWORK_H

/* The maximum number of chars in a domain name */
#define NETWORK_NAME_MAX 256

/* The maximum number of chars in an address in string form */
#define NETWORK_ADDR_MAX 16

typedef int network_address;
typedef enum { NETWORK_TUNE_INTERACTIVE, NETWORK_TUNE_BULK } network_tune_mode;

int network_serve( int port );
int network_serve_local( const char *path );
int network_accept( int master );
int network_connect( const char *host, int port );
int network_connect_local( const char *path );
void network_close( int fd );

int network_tune( int fd, network_tune_mode mode );
int network_sleep( int fd, int micros );
int network_ok( int fd );

int network_read( int fd, char *data, int length );
int network_write( int fd, const char *data, int length );

int network_address_local( int fd, network_address *host, int *port );
int network_address_remote( int fd, network_address *host, int *port );

void network_address_to_string( network_address host, char *str );
int network_string_to_address( const char *str, network_address *addr );

int network_address_to_name( network_address host, char *str );
int network_name_to_address( const char *name, network_address *addr );
int network_name_canonicalize( const char *name, char *cname, network_address *caddr );

int network_address_get( network_address *addr );
int network_name_get( char *name );
int network_string_get( char *str );

#endif
