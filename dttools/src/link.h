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

#include <sys/types.h>

#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

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
struct link *link_connect(const char *addr, int port, time_t stoptime);

/** Turn a FILE* into a link.  Useful when trying to poll both remote and local connections using @ref link_poll
@param file File to create the link from.
@return On success, returns a pointer to a link object.  On failure, returns a null pointer with errno set appropriately.
*/
struct link *link_attach_to_file(FILE *file);

/** Turn an fd into a link.  Useful when trying to poll both remote and local connections using @ref link_poll
@param fd File descriptor to create the link from.
@return On success, returns a pointer to a link object.  On failure, returns a null pointer with errno set appropriately.
*/
struct link *link_attach_to_fd(int fd);


/** Prepare to accept connections.
@ref link_serve will accept connections on any network interface,
which is usually what you want.
@param port The port number to listen on.  If less than 1, the first unused port between TCP_LOW_PORT and TCP_HIGH_PORT will be selected.
@return link A server endpoint that can be passed to @ref link_accept, or null on failure.
*/
struct link *link_serve(int port);

/** Prepare to accept connections.
@ref link_serve_range will accept connections on any network interface, which is usually what you want.
@param low The low port in a range to listen on (inclusive).
@param high The high port in a range to listen on (inclusive).
@return link A server endpoint that can be passed to @ref link_accept, or null on failure.
*/
struct link *link_serve_range(int low, int high);

/** Prepare to accept connections on one network interface.
Functions like @ref link_serve, except that the server will only be visible on the given network interface.
@param addr IP address of the network interface.
@param port The port number to listen on.  If less than 1, the first unused port between TCP_LOW_PORT and TCP_HIGH_PORT will be selected.
@return link A server endpoint that can be passed to @ref link_accept, or null on failure.
*/
struct link *link_serve_address(const char *addr, int port);

/** Prepare to accept connections on one network interface.
Functions like @ref link_serve, except that the server will only be visible on the given network interface and allows for a port range.
@param addr IP address of the network interface.
@param low The low port in a range to listen on (inclusive).
@param high The high port in a range to listen on (inclusive).
@return link A server endpoint that can be passed to @ref link_accept, or null on failure.
*/
struct link *link_serve_addrrange(const char *addr, int low, int high);

/** Accept one connection.
@param master A link returned from @ref link_serve or @ref link_serve_address.
@param stoptime The time at which to abort.
@return link A connection to a client, or null on failure.
*/
struct link *link_accept(struct link *master, time_t stoptime);

/** Read data from a connection.
This call will block until the given number of bytes have been read,
or the connection is dropped.
@param link The link from which to read.
@param data A buffer to hold the data.
@param length The number of bytes to read.
@param stoptime The time at which to abort.
@return The number of bytes actually read, or zero if the connection is closed, or less than zero on error.
*/
ssize_t link_read(struct link *link, char *data, size_t length, time_t stoptime);

/** Read available data from a connection.
This call will read whatever data is immediately available, and then
return without blocking.
@param link The link from which to read.
@param data A buffer to hold the data.
@param length The number of bytes to read.
@param stoptime The time at which to abort.
@return The number of bytes actually read, or zero if the connection is closed, or less than zero on error.
*/
ssize_t link_read_avail(struct link *link, char *data, size_t length, time_t stoptime);

/** Write data to a connection.
@param link The link to write.
@param data A pointer to the data.
@param length The number of bytes to write.
@param stoptime The time at which to abort.
@return The number of bytes actually written, or less than zero on error.
*/
ssize_t link_write(struct link *link, const char *data, size_t length, time_t stoptime);

/* Write a string of length len to a connection. All data is written until
 * finished or an error is encountered.
@param link The link to write.
@param str A pointer to the string.
@param len Length of the string.
@param stoptime The time at which to abort.
@return The number of bytes actually written, or less than zero on error.
*/
ssize_t link_putlstring(struct link *link, const char *str, size_t len, time_t stoptime);

/* Write a C string to a connection. All data is written until finished or an
   error is encountered. It is defined as a macro.
@param link The link to write.
@param str A pointer to the string.
@param stoptime The time at which to abort.
@return The number of bytes actually written, or less than zero on error.
*/
#define link_putstring(l,s,t)  (link_putlstring(l,s,strlen(s),t))

/* Write a C literal string to a connection. All data is written until finished
   or an error is encountered. It is defined as a macro.
@param link The link to write.
@param str A pointer to the string.
@param stoptime The time at which to abort.
@return The number of bytes actually written, or less than zero on error.
*/
#define link_putliteral(l,s,t)  (link_putlstring(l,s "",((sizeof(s))-1),t))

/** Write formatted data to a connection. All data is written until finished
  * or an error is encountered.
@param link The link to write.
@param fmt A pointer to the data.
@param stoptime The time at which to abort.
@param ... Format arguments.
@return The number of bytes actually written, or less than zero on error.
*/
ssize_t link_putfstring(struct link *link, const char *fmt, time_t stoptime, ...)
  __attribute__ (( format(printf,2,4) )) ;

/** Write formatted data to a connection. All data is written until finished
  * or an error is encountered.
@param link The link to write.
@param fmt A pointer to the data.
@param stoptime The time at which to abort.
@param va Format arguments.
@return The number of bytes actually written, or less than zero on error.
*/
ssize_t link_putvfstring(struct link *link, const char *fmt, time_t stoptime, va_list va);

/** Block until a link is readable or writable.
@param link The link to wait on.
@param usec The maximum number of microseconds to wait.
@param reading Wait for the link to become readable.
@param writing Wait for the link to become writable.
@return One if the link becomes readable or writable before the timeout expires, zero otherwise.
*/
int link_usleep(struct link *link, int usec, int reading, int writing);

int link_usleep_mask(struct link *link, int usec, sigset_t *mask, int reading, int writing);

/** Block until a link is readable or writable.
@param link The link to wait on.
@param stoptime The time at which to abort.
@param reading Wait for the link to become readable.
@param writing Wait for the link to become writable.
@return One if the link becomes readable or writable before the timeout expires, zero otherwise.
*/
int link_sleep(struct link *link, time_t stoptime, int reading, int writing);

/** Close a connection.
@param link The connection to close.
*/
void link_close(struct link *link);


/** Detach a link from the underlying file descriptor.
 *  Deletes the link structure.
@param link The link to detach.
*/
void link_detach(struct link *link);

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

void link_window_set(int send_window, int recv_window);

/** Get the TCP window size actually allocated for this link.
@param link The link to examine.
@param send_window A pointer where to store the send window.
@param recv_window A pointer where to store the receive window.
*/

void link_window_get(struct link *link, int *send_window, int *recv_window);

/** Read a line of text from a link.
Reads a line of text, up to and including a newline, interpreted as either LF
or CR followed by LF.  The line actually returned is null terminated and
does not contain the newline indicator.   An internal buffer is used so that
readline can usually complete with zero or one system calls.
@param link The link to read from.
@param line A pointer to a buffer to fill with data.
@param length The length of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return True if a line was successfully read.  False if end of stream was reach or an error occured.
*/
int link_readline(struct link *link, char *line, size_t length, time_t stoptime);

/** Get the underlying file descriptor of a link.
@param link The link to examine.
@return The integer file descriptor of the link.
*/
int link_fd(struct link *link);

int link_keepalive(struct link *link, int onoff);

int link_nonblocking(struct link *link, int onoff);


/** Check whether a link has unread contents in its buffer.
@param link The link to examine.
@return 1 if buffer is empty, 0 otherwise.
*/
int link_buffer_empty(struct link *link);

/** Return the local address of the link in text format.
@param link The link to examine.
@param addr Pointer to a string of at least @ref LINK_ADDRESS_MAX bytes, which will be filled with a text representation of the local IP address.
@param port Pointer to an integer, which will be filled with the TCP port number.
@return Positive on success, zero on failure.
*/
int link_address_local(struct link *link, char *addr, int *port);

/** Return the remote address of the link in text format.
@param link The link to examine.
@param addr Pointer to a string of at least @ref LINK_ADDRESS_MAX bytes, which will be filled with a text representation of the remote IP address.
@param port Pointer to an integer, which will be filled with the TCP port number.
@return Positive on success, zero on failure.
*/
int link_address_remote(struct link *link, char *addr, int *port);

ssize_t link_stream_to_buffer(struct link *link, char **buffer, time_t stoptime);

int64_t link_stream_to_fd(struct link *link, int fd, int64_t length, time_t stoptime);
int64_t link_stream_to_file(struct link *link, FILE * file, int64_t length, time_t stoptime);

int64_t link_stream_from_fd(struct link *link, int fd, int64_t length, time_t stoptime);
int64_t link_stream_from_file(struct link *link, FILE * file, int64_t length, time_t stoptime);

int64_t link_soak(struct link *link, int64_t length, time_t stoptime);

/** Options for link performance tuning. */
typedef enum {
	LINK_TUNE_INTERACTIVE,	/**< Data is sent immediately to optimze interactive latency. */
	LINK_TUNE_BULK		/**< Data may be buffered to improve throughput of large transfers. */
} link_tune_t;

/** Tune a link for interactive or bulk performance.  A link may be tuned at any point in its lifecycle.
@ref LINK_TUNE_INTERACTIVE is best used for building latency-sensitive interactive or RPC applications.
@ref LINK_TUNE_BULK is best used to large data transfers.
@param link The link to be tuned.
@param mode The desired tuning mode.
*/
int link_tune(struct link *link, link_tune_t mode);

/** Indicates a link is ready to read via @ref link_poll.*/
#define LINK_READ 1

/** Indicates a link is ready to write via @ref link_poll.*/
#define LINK_WRITE 2

/** Activity structure passed to @ref link_poll. */
struct link_info {
	struct link *link;  /**< The link to be polled. */
	int events;	    /**< The events to wait for (@ref LINK_READ or @ref LINK_WRITE) */
	int revents;	    /**< The events returned (@ref LINK_READ or @ref LINK_WRITE) */
};

/**
Wait for a activity on a an array of links.
@param array Pointer to an array of @ref link_info structures.  Each one should contain a pointer to a valid link and have the events field set to the events (@ref LINK_READ or @ref LINK_WRITE) of interest.  Upon return, each one will have the revents field filled with the events that actually occurred.
@param nlinks The length of the array.
@param msec The number of milliseconds to wait for activity.  Zero indicates do not wait at all, while -1 indicates wait forever.
@return The number of links available to read or write.
*/

int link_poll(struct link_info *array, int nlinks, int msec);

/** Set log file for global link stats.
@param stats Log file.
*/
void link_stats(FILE *log);

#endif
