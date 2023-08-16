/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_STREAM_H
#define CHIRP_STREAM_H

#include <sys/time.h>
#include <stdio.h>

/** @file chirp_stream.h
Streaming I/O interface.
This module implements <i>streaming I/O</i> against a Chirp server.
In this model, the user can read and write small amounts of data
in a continuous stream to or from a remote file.
This interface gives higher data throughput than the @ref chirp_reli.h
interface, but it is <i>unreliable</i>.  If a streaming connection is
lost, the client must close it and start all over again.  If reliability
is more important than performance, use the @ref chirp_reli.h interface instead.
*/

/** Indicates what mode to be used for opening a stream.*/
typedef enum {
	CHIRP_STREAM_READ,	   /**< Open the stream for reading. */
	CHIRP_STREAM_WRITE,  /**< Open the stream for writing. */
} chirp_stream_mode_t;

/** Open a new stream for reading or writing.
Connects to a named server and creates a stream for reading or writing to the given file.
@param hostport The host and optional port number of the Chirp server.
@param path The pathname of the file to access.
@param mode The mode of the stream, either @ref CHIRP_STREAM_READ or @ref CHIRP_STREAM_WRITE.
@param stoptime The absolute time at which to abort.
@return On success, returns a handle to a chirp_stream.  On failure, returns zero and sets errno appropriately.
*/

struct chirp_stream *chirp_stream_open(const char *hostport, const char *path, chirp_stream_mode_t mode, time_t stoptime);

/** Print formatted data to a stream with buffering.
Writes formatted data to a stream, just like a standard Unix printf.
@param stream A stream created by @ref chirp_stream_open.
@param stoptime The absolute time at which to abort.
@param fmt A printf-style format string, followed by the data to transmit.
@return On success, returns the number of characters written to the stream.  On failure, returns less than zero and sets errno appropriately.
*/

int chirp_stream_printf(struct chirp_stream *stream, time_t stoptime, const char *fmt, ...);

/** Read a single line from a stream with buffering.
Reads a single line terminated by a linefeed (ASCII byte 10).  Carriage returns (ASCII byte 13) are ignored and removed from the input.
@param stream A stream created by @ref chirp_stream_open.
@param line A pointer to a buffer where the line can be placed.
@param length The size of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return On success, the number of bytes actually read.  On end-of-stream, returns zero.  On failure, returns less than zero and sets errno appropriately.
*/
int chirp_stream_readline(struct chirp_stream *stream, char *line, int length, time_t stoptime);

/** Write data to a stream.
@param stream A stream created by @ref chirp_stream_open.
@param data A pointer to a buffer of data to write.
@param length The size of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return On success, the number of bytes actually written.  On failure, returns less than zero and sets errno appropriately.
*/

int chirp_stream_write(struct chirp_stream *stream, const void *data, int length, time_t stoptime);

/** Read data from a stream.
@param stream A stream created by @ref chirp_stream_open.
@param data A pointer to a buffer where data can be placed.
@param length The size of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return On success, the number of bytes actually read.  On end-of-stream, returns zero. On failure, returns less than zero and sets errno appropriately.
*/

int chirp_stream_read(struct chirp_stream *stream, void *data, int length, time_t stoptime);

/** Flush buffered data to the stream.
@param stream A stream created by @ref chirp_stream_open.
@param stoptime The absolute time at which to abort.
@return On success, returns the number of characters written to the stream.  On failure, returns less than zero and sets errno appropriately.
*/

int chirp_stream_flush(struct chirp_stream *stream, time_t stoptime);

/** Closes a stream.
This routine closes and deallocates all state associated with a stream.
Note that a stream may buffer data internally, so the called does not know if all data has been written successfully unless this function returns success.
@param stream A stream created by @ref chirp_stream_open.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero and sets errno appropriately.
*/

int chirp_stream_close(struct chirp_stream *stream, time_t stoptime);

#endif

/* vim: set noexpandtab tabstop=8: */
