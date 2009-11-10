/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef FULL_IO_H
#define FULL_IO_H

#include <sys/types.h>
#include <stdio.h>
#include "int_sizes.h"

/** @file full_io.h
Perform complete I/O operations, retrying through failures and signals.
A subtle property of Unix is that the kernel may choose to leave an I/O
operation incomplete, returning a smaller number of bytes than you requested,
or simply giving up with the error EINTR, if it is simply more "convenient".
The full_io routines look lik normal Unix read and write operations, but
silently retry through temporary failures.  Of course, these routines may still
fail for more permanent reasons such as end of file or disk full.
*/

/** Read the next bytes from a file descriptor.
@param fd File descriptor.
@param buf Pointer to buffer.
@param count The number of bytes to read.
@return The number of bytes actually read, or less than zero indicating error.
*/
ssize_t full_read( int fd, void *buf, size_t count );

/** Write the next bytes to a file descriptor.
@param fd File descriptor.
@param buf Pointer to buffer.
@param count The number of bytes to write.
@return The number of bytes actually written, or less than zero indicating error.
*/
ssize_t full_write( int fd, const void *buf, size_t count );

/** Read arbitrary bytes from a file descriptor.
@param fd File descriptor.
@param buf Pointer to buffer.
@param count The number of bytes to read.
@param offset The offset in the file to begin from.
@return The number of bytes actually read, or less than zero indicating error.
*/
ssize_t full_pread( int fd, void *buf, size_t count, off_t offset );

/** Write arbitrary bytes to a file descriptor.
@param fd File descriptor.
@param buf Pointer to buffer.
@param count The number of bytes to write.
@param offset The offset in the file to begin from.
@return The number of bytes actually written, or less than zero indicating error.
*/
ssize_t full_pwrite( int fd, const void *buf, size_t count, off_t offset );

/** Read arbitrary bytes from a file descriptor. (64 bit)
@param fd File descriptor.
@param buf Pointer to buffer.
@param count The number of bytes to read.
@param offset The offset in the file to begin from.
@return The number of bytes actually read, or less than zero indicating error.
*/
INT64_T full_pread64( int fd, void *buf, INT64_T count, INT64_T offset );

/** Write arbitrary bytes to a file descriptor.  (64 bit)
@param fd File descriptor.
@param buf Pointer to buffer.
@param count The number of bytes to write.
@param offset The offset in the file to begin from.
@return The number of bytes actually written, or less than zero indicating error.
*/
INT64_T full_pwrite64( int fd, const void *buf, INT64_T count, INT64_T offset );

/** Read the next bytes from a file stream.
@param file Standard file stream.
@param buf Pointer to buffer.
@param count The number of bytes to read.
@return The number of bytes actually read, or less than zero indicating error.
*/
ssize_t full_fread( FILE *file, void *buf, size_t count );

/** Write the next bytes to a file stream.
@param file Standard file stream.
@param buf Pointer to buffer.
@param count The number of bytes to write.
@return The number of bytes actually written, or less than zero indicating error.
*/
ssize_t full_fwrite( FILE *file, const void *buf, size_t count );

#endif
