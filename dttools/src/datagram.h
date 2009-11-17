/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DATAGRAM_H
#define DATAGRAM_H

/** @file datagram.h UDP datagram communications.
This module implements datagram communications using UDP.
A datagram is a small, fixed size message send to a given
host and port, which is not guaranteed to arrive.
<p>
This module is arguably misnamed: A <tt>struct datagram</tt>
does not represent a datagram, but rather an open port that
can be used to send datagrams with @ref datagram_send.
<p>
Example sender:
<pre>
include "datagram.h"
include "debug.h"

const char *message = "hello world!";
const char *address = "192.168.2.1";
int port = 40000;

d = datagram_create(DATAGRAM_PORT_ANY);
if(!d) fatal("couldn't create datagram port");

datagram_send(d,message,strlen(message),address,port);
</pre>

And an example receiver:

<pre>
include "datagram.h"
include "debug.h"

char message[DATAGRAM_PAYLOAD_MAX];
char address[DATAGRAM_ADDRESS_MAX];
int  port = 40000;

d = datagram_create(port);
if(!d) fatal("couldn't create datagram port");

length = datagram_recv(d,message,sizeof(message),&address,&port,10000000);
if(length>0) {
	message[length] = 0;
	printf("got message: %s\n",message);
} else {
	printf("no message received.\n");
}
</pre>


*/

/** Maximum number of characters in a text formatted datagram address. */
#define DATAGRAM_ADDRESS_MAX 48

/** Maximum number of bytes in a datagram payload */
#define DATAGRAM_PAYLOAD_MAX 65536

/** Used to indicate any available port. */
#define DATAGRAM_PORT_ANY 0

/** The address to send to for broadcasting. */
#define DATAGRAM_ADDRESS_BROADCAST "255.255.255.255"

/** Create a new port for sending or receiving datagrams.
@param port The UDP port number to bind to.  On most versions of Unix, an ordinary user can only bind to ports greater than 1024.
@return A new object for sending or receiving datagrams.  On failure, returns null and sets errno appropriately.  A very common error is EADDRINUSE, which indicates another process is already bound to that port.
*/
struct datagram * datagram_create( int port );

/** Destroy a datagram port.
@param d The datagram object to destroy.
*/
void datagram_delete( struct datagram *d );

/** Receive a datagram.
@param d The datagram object.
@param data Where to store the received message.
@param length The length of the buffer, typically DATAGRAM_PAYLOAD_MAX.
@param addr Pointer to a string of at least DATAGRAM_ADDRESS_MAX characters, which will be filled in with the IP address of the sender.
@param port Pointer to an integer which will be filled in with the port number of the sender.
@param timeout Maximum time to wait, in microseconds.
@return On success, returns the number of bytes received.  On failure, returns less than zero and sets errno appropriately.
*/
int datagram_recv( struct datagram *d, char *data, int length, char *addr, int *port, int timeout );

/** Send a datagram.
@param d The datagram object.
@param data The data to send.
@param length The length of the datagram.
@param addr The address of the recipient.
@param port The port of the recipient.
@return On success, returns the number of bytes sent.  On failure, returns less than zero and sets errno appropriately.
*/
int datagram_send( struct datagram *d, const char *data, int length, const char *addr, int port );

/** Obtain the file descriptor of a datagram object.
@param d The datagram object.
@return The file descriptor associated with the underlying socket.
*/
int datagram_fd( struct datagram *d );

#endif
