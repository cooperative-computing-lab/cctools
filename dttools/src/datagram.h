/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef DATAGRAM_H
#define DATAGRAM_H

/* Maximum number of characters in a datagram address */
#define DATAGRAM_ADDRESS_MAX 17

/* Maximum number of bytes in a datagram payload */
#define DATAGRAM_PAYLOAD_MAX 65536

/* The port value for receiving from any process. */
#define DATAGRAM_PORT_ANY 0

/* The address to send to for broadcasting. */
#define DATAGRAM_ADDRESS_BROADCAST "255.255.255.255"

struct datagram * datagram_create( int port );
void datagram_delete( struct datagram *d );

int datagram_recv( struct datagram *d, char *data, int length, char *addr, int *port, int timeout );
int datagram_send( struct datagram *d, const char *data, int length, const char *addr, int port );
int datagram_fd( struct datagram *d );

#endif
