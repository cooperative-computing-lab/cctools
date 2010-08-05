/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LINK_H
#define LINK_H

/** @file link.h A high level TCP connection library.
A <b>link</b> is a TCP connection to a process on another machine.  This module works at a higher level of abstraction than the socket library, with an easier-to-use API and explicit support for timeouts.
<p>
Timeouts are specified using an absolute stoptime.  For example, if you want
a connection to be attempted for sixty seconds, specify <tt>time(0)+60</tt>.
The operation will be tried and retried until that absolute limit, giving
the caller much greater control over program behavior.
<p>
Note that this library manipulates IP addresses in the form of strings.
To convert a hostname into a string IP address, call @ref domain_name_cache_lookup.  For example, here is how to make a simple HTTP request:
<pre>
struct link *link;
time_t stoptime = time(0)+300;
char addr[LINK_ADDRESS_MAX];
int result;

const char *request = "GET / HTTP/1.0\n\n";

result = domain_name_cache_lookup("www.google.com",addr);
if(!result) fatal("could not lookup name");

link = link_connect(addr,80,stoptime);
if(!link) fatal("could not connect");

result = link_write(link,request,strlen(request),stoptime);
if(result<0) fatal("could not send request");

link_stream_to_file(link,stdout,1000000,stoptime);
link_close(link);
</pre>

*/

#include "int_sizes.h"

#include <time.h>
#include <limits.h>
#include <stdio.h>
#include <sys/types.h>

/** Maximum number of characters in the text representation of a link address.  This must be large enough to accomodate ipv6 in the future. */
#define LINK_ADDRESS_MAX 48

/** Value to usewhen any listen port is acceptable */
#define LINK_PORT_ANY 0

/** Stoptime to give when you wish to wait forever */
#define LINK_FOREVER ((time_t)INT_MAX)

/** Connect to a remote host.
@param addr IP address of server in string form.
@param port Port of server.
@param stoptime Absolute time at which to abort.
@return On success, returns a pointer to a link object.  On failure, returns a null pointer with errno set appropriately.
*/ 
struct link * link_connect( const char *addr, int port, time_t stoptime );

/** Prepare to accept connections.
@ref link_serve will accept connections on any network interface,
which is usually what you want.
@param port The port number to listen on.
@return link A server endpoint that can be passed to @ref link_accept, or null on failure.
*/
struct link * link_serve( int port );

/** Prepare to accept connections on one network interface.
Functions like @ref link_serve, except that the server will
only be visible on the given network interface.
@param addr IP address of the network interface.
@param port The port number to listen on.
@return link A server endpoint that can be passed to @ref link_accept, or null on failure.
*/
struct link * link_serve_address( const char *addr, int port );

/** Accept one connection.
@param master A link returned from @ref link_serve or @ref link_serve_address.
@param stoptime The time at which to abort.
@return link A connection to a client, or null on failure.
*/
struct link * link_accept( struct link *master, time_t stoptime );

/** Read data from a connection.
This call will block until the given number of bytes have been read,
or the connection is dropped.
@param link The link from which to read.
@param data A buffer to hold the data.
@param length The number of bytes to read.
@param stoptime The time at which to abort.
@return The number of bytes actually read, or zero if the connection is closed, or less than zero on error.
*/
int  link_read( struct link *link, char *data, int length, time_t stoptime );

/** Read available data from a connection.
This call will read whatever data is immediately available, and then
return without blocking.
@param link The link from which to read.
@param data A buffer to hold the data.
@param length The number of bytes to read.
@param stoptime The time at which to abort.
@return The number of bytes actually read, or zero if the connection is closed, or less than zero on error.
*/
int  link_read_avail( struct link *link, char *data, int length, time_t stoptime );

/** Write data to a connection.
@param link The link to write.
@param data A pointer to the data.
@param length The number of bytes to write.
@param stoptime The time at which to abort.
@return The number of bytes actually written, or less than zero on error.
*/
int  link_write( struct link *link, const char *data, int length, time_t stoptime );

/** Block until a link is readable or writable.
@param link The link to wait on.
@param usec The maximum number of microseconds to wait.
@param reading Wait for the link to become readable.
@param writing Wait for the link to become writable.
@return One if the link becomes readable or writable before the timeout expires, zero otherwise.
*/
int  link_usleep( struct link *link, int usec, int reading, int writing );

/** Close a connection.
@param link The connection to close.
*/
void link_close( struct link *link );

/** Set the TCP window size to be used for all links.
Takes effect on future calls to @ref link_connect or @ref link_accept.
Default value is set by the system or by the environment variable
TCP_WINDOW_SIZE.
Note that the operating system may place limits on the buffer
sizes actually allocated.  Use @ref link_window_get to retrieve
the buffer actually allocated for a given link.
@param send_window The size of the send window, in bytes.
@param recv_window The size of the recv window, in bytes.
*/

void link_window_set( int send_window, int recv_window );

/** Get the TCP window size actually allocated for this link.
@param link The link to examine.
@param send_window A pointer where to store the send window.
@param recv_window A pointer where to store the receive window.
*/

void link_window_get( struct link *link, int *send_window, int *recv_window );

/** Read a line of text from a link.
Reads a line of text, up to and including a newline, interpreted as either LF
or CR followed by LF.  The line actually returned is null terminated and
does not contain the newline indicator.   An internal buffer is used so that
readline can usually complete with zero or one system calls.
@param link The link to read from.
@param line A pointer to a buffer to fill with data.
@param length The length of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return If greater than zero, a line was read, and the return value indicates the length in bytes.  If equal to zero, end of stream was reached.  If less than zero, an error occurred.
*/
int  link_readline( struct link *link, char *line, int length, time_t stoptime );

/** Get the underlying file descriptor of a link.
@param link The link to examine.
@return The integer file descriptor of the link.
*/
int  link_fd( struct link *link );

int  link_nonblocking( struct link *link, int onoff );

/** Return the local address of the link in text format.
@param link The link to examine.
@param addr Pointer to a string of at least @ref LINK_ADDRESS_MAX bytes, which will be filled with a text representation of the local IP address.
@param port Pointer to an integer, which will be filled with the TCP port number.
@return Positive on success, zero on failure.
*/
int  link_address_local( struct link *link, char *addr, int *port );

/** Return the remote address of the link in text format.
@param link The link to examine.
@param addr Pointer to a string of at least @ref LINK_ADDRESS_MAX bytes, which will be filled with a text representation of the remote IP address.
@param port Pointer to an integer, which will be filled with the TCP port number.
@return Positive on success, zero on failure.
*/
int  link_address_remote( struct link *link, char *addr, int *port );

INT64_T link_stream_to_buffer( struct link *link, char **buffer, time_t stoptime );

INT64_T link_stream_to_fd( struct link *link, int fd, INT64_T length, time_t stoptime );
INT64_T link_stream_to_file( struct link *link, FILE *file, INT64_T length, time_t stoptime );

INT64_T link_stream_from_fd( struct link *link, int fd, INT64_T length, time_t stoptime );
INT64_T link_stream_from_file( struct link *link, FILE *file, INT64_T length, time_t stoptime );

INT64_T link_soak( struct link *link, INT64_T length, time_t stoptime );

/** Options for link performance tuning. */
typedef enum {
	LINK_TUNE_INTERACTIVE,  /**< Data is sent immediately to optimze interactive latency. */
	LINK_TUNE_BULK          /**< Data may be buffered to improve throughput of large transfers. */
} link_tune_t;

/** Tune a link for interactive or bulk performance.  A link may be tuned at any point in its lifecycle.
@ref LINK_TUNE_INTERACTIVE is best used for building latency-sensitive interactive or RPC applications.
@ref LINK_TUNE_BULK is best used to large data transfers.
@param link The link to be tuned.
@param mode The desired tuning mode.
*/
int  link_tune( struct link *link, link_tune_t mode );

/** Indicates a link is ready to read via @ref link_poll.*/
#define LINK_READ 1

/** Indicates a link is ready to write via @ref link_poll.*/
#define LINK_WRITE 2

/** Activity structure passed to @ref link_poll. */
struct link_info {
	struct link *link;  /**< The link to be polled. */
	int events;         /**< The events to wait for (@ref LINK_READ or @ref LINK_WRITE) */
	int revents;	    /**< The events returned (@ref LINK_READ or @ref LINK_WRITE) */
};

/**
Wait for a activity on a an array of links.
@param array Pointer to an array of @ref link_info structures.  Each one should contain a pointer to a valid link and have the events field set to the events (@ref LINK_READ or @ref LINK_WRITE) of interest.  Upon return, each one will have the revents field filled with the events that actually occurred.
@param nlinks The length of the array.
@param msec The number of milliseconds to wait for activity.  Zero indicates do not wait at all, while -1 indicates wait forever.
@return The number of links available to read or write.
*/

int  link_poll( struct link_info *array, int nlinks, int msec );

#endif













