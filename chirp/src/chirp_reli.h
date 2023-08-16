/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file chirp_reli.h The primary user API for accessing Chirp servers.

@ref chirp_reli.h is designed to look similar to the Unix I/O interface.
It is called "reli" because it is "reliable".
Each function call here has the capabaility to detect and retry a large number
of network and server errors with an exponential backoff, until a user-defined time limit is reached.
The caller need not worry about connecting to or disconnecting from servers.

All functions in this file have several common calling conventions.
@param host A hostname may be a domain name or an IP address, followed by an optional colon and port number.
If not given, the port number is assumed to by the default Chirp port of 9094.
@param path A pathname identifies a file from the root of the given file server, and must start with a slash.
The Chirp protocol allows pathnames to contain any printable ASCII character except a newline.
@param stoptime All functions accept a final argument <tt>time_t stoptime</tt> which
indicates the absolute time at which to abort.  For example, to
try an operation for 60 seconds, pass <tt>time(0)+60</tt> as <tt>stoptime</tt>.
Any transient network failures will be silently retried until this timeout is reached.
@return On success, all functions return an integer <b>greater than or equal to zero</b>,
and <tt>errno</tt> may have any arbitrary value.
On failure, all return an integer <b>less than zero</b>, and set errno
to the reason for the failure.  (@ref chirp_reli_open is the only exception to this rule.)
The caller may invoke <tt>strerror(errno)</tt> to generate a human-readable string representing the error.
*/

#ifndef CHIRP_RELI_H
#define CHIRP_RELI_H

#include "chirp_types.h"

#include <sys/types.h>
#include <stdio.h>
#include <dirent.h>

/** Creates or opens a file in preparation for I/O.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param flags Any of the following Unix open flags ORed together:
- O_RDONLY - Open for reading only.
- O_WRONLY - Open for writing only.
- O_RDWR - Open for both read and write.
- O_APPEND - Open for appending to the file.
- O_CREAT - Create the file if it does not exist.
- O_TRUNC - Truncate the file to zero bytes.
- O_SYNC - Force synchronous writes to disk.
@param mode The Unix mode bits to be given to the file.  Chirp only honors the owner component of the mode bits.  Typical choices
are <tt>0700</tt> for an executable, and <tt>0600</tt> for a data file.  (Note the leading zero to indicate octal data.)
@param stoptime The absolute time at which to abort.
@return On success, returns a pointer to a chirp_file.  On failure, returns zero and sets errno appropriately.
@see chirp_reli_pread, chirp_reli_pwrite, chirp_reli_sread, chirp_reli_swrite, chirp_reli_fstat, chirp_reli_fstatfs chirp_reli_fchmod, chirp_reli_fchown, chirp_reli_ftruncate, chirp_reli_flush, chirp_reli_close
*/

struct chirp_file *chirp_reli_open(const char *host, const char *path, INT64_T flags, INT64_T mode, time_t stoptime);

/** Closes an open file.  Note that a close may need to write buffered data to disk before completing, so <b>chirp_reli_close can fail</b>.
If chirp_reli_close indicates failures, the <tt>struct chirp_file</tt> is deallocated and can no longer be used, but the caller
must assume some previously written data was lost.
@param file A chirp_file handle returned by chirp_reli_open.
@param stoptime The absolute time at which to abort.
@see chirp_file_open
*/

INT64_T chirp_reli_close(struct chirp_file *file, time_t stoptime);

/** Read data from a file.  Small reads may be buffered into large reads for efficiency.
@param file A chirp_file handle returned by chirp_reli_open.
@param buffer Pointer to destination buffer.
@param length Number of bytes to read.
@param offset Beginning offset in file.
@param stoptime The absolute time at which to abort.
@return On success, returns the number of bytes actually read, which may be less than that requested.  On end of file, returns zero.  On failure, <0 and sets errno.
@see chirp_reli_open, chirp_reli_pread_unbuffered, chirp_reli_sread
*/

INT64_T chirp_reli_pread(struct chirp_file *file, void *buffer, INT64_T length, INT64_T offset, time_t stoptime);

/** Write data to a file.  Small writes may be buffered together into large writes for efficiency.
@param file A chirp_file handle returned by chirp_reli_open.
@param buffer Pointer to source buffer.
@param length Number of bytes to write.
@param offset Beginning offset in file.
@param stoptime The absolute time at which to abort.
@return On success, returns the number of bytes actually written, which may be less than requested.  On failure, <0 and sets errno.
@see chirp_reli_open, chirp_reli_swrite
*/

INT64_T chirp_reli_pwrite(struct chirp_file *file, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime);

/** Read data from a file without buffering.
@param file A chirp_file handle returned by chirp_reli_open.
@param buffer Pointer to destination buffer.
@param length Number of bytes to read.
@param offset Beginning offset in file.
@param stoptime The absolute time at which to abort.
@return On success, returns the number of bytes actually read.  If the end of file has been reached, returns zero.  On failure, <0 and sets errno.
@see chirp_reli_open, chirp_reli_pread, chirp_reli_sread
*/

INT64_T chirp_reli_pread_unbuffered(struct chirp_file *file, void *buffer, INT64_T length, INT64_T offset, time_t stoptime);

/** Write data to a file without buffering.
@param file A chirp_file handle returned by chirp_reli_open.
@param buffer Pointer to source buffer.
@param length Number of bytes to write.
@param offset Beginning offset in file.
@param stoptime The absolute time at which to abort.
@return On success, returns the number of bytes actually written. On failure, <0 and sets errno.
@see chirp_reli_open, chirp_reli_pwrite, chirp_reli_swrite
*/

INT64_T chirp_reli_pwrite_unbuffered(struct chirp_file *file, const void *buffer, INT64_T length, INT64_T offset, time_t stoptime);

/** Strided read from a file.  Reads <tt>stride_length</tt> bytes every <tt>stride_skip</tt> bytes, starting from <tt>offset</tt>
up to a maximum of <tt>length</tt> bytes read.
@param file A chirp_file handle returned by chirp_reli_open.
@param buffer Pointer to destiation buffer.
@param length Maximum number of bytes to read.
@param stride_length Bytes to read in each stride.
@param stride_skip Bytes to skip between each stride.
@param offset Beginning offset in file.
@param stoptime The absolute time at which to abort.
@return On success, returns the number of bytes actually read, which may be less than requested.  On failure, <0 and sets errno.
@see chirp_reli_open, chirp_reli_pread, chirp_reli_pwrite, chirp_reli_sread, chirp_reli_swrite
*/

INT64_T chirp_reli_sread(struct chirp_file *file, void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);

/** Strided write to a file.  Writes <tt>stride_length</tt> bytes every <tt>stride_skip</tt> bytes, starting from <tt>offset</tt>
up to a maximum of <tt>length</tt> bytes written.
@param file A chirp_file handle returned by chirp_reli_open.
@param buffer Pointer to destiation buffer.
@param length Maximum number of bytes to write.
@param stride_length Bytes to write in each stride.
@param stride_skip Bytes to skip between each stride.
@param offset Beginning offset in file.
@param stoptime The absolute time at which to abort.
@return On success, returns the number of bytes actually written, which may be less than requested.  On failure, <0 and sets errno.
@see chirp_reli_open, chirp_reli_pread, chirp_reli_pwrite, chirp_reli_sread, chirp_reli_swrite
*/

INT64_T chirp_reli_swrite(struct chirp_file *file, const void *buffer, INT64_T length, INT64_T stride_length, INT64_T stride_skip, INT64_T offset, time_t stoptime);

/** Get file status.
@param file A chirp_file handle returned by chirp_reli_open.
@param info A pointer to a @ref chirp_stat structure to fill.
@param stoptime The absolute time at which to abort.
@return >=0 on success, <0 on failure.
@see chirp_reli_open, chirp_stat
*/

INT64_T chirp_reli_fstat(struct chirp_file *file, struct chirp_stat *info, time_t stoptime);

/** Get file system status.
@param file A chirp_file handle returned by chirp_reli_open.
@param info A pointer to a @ref chirp_statfs structure to fill.
@param stoptime The absolute time at which to abort.
@return >=0 on success, <0 on failure.
@see chirp_reli_open, chirp_reli_statfs
*/

INT64_T chirp_reli_fstatfs(struct chirp_file *file, struct chirp_statfs *info, time_t stoptime);

/** Change the ownership of a file.
@deprecated Note that the current Chirp file server does not make use of the Unix owner field, see @ref chirp_reli_setacl instead.
@param file A chirp_file handle returned by chirp_reli_open.
@param uid The new user ID.
@param gid The new group ID.
@param stoptime The absolute time at which to abort.
@see chirp_reli_chown, chirp_reli_setacl, chirp_reli_getacl
*/

INT64_T chirp_reli_fchown(struct chirp_file *file, INT64_T uid, INT64_T gid, time_t stoptime);

/** Change the mode bits of a file.
The mode bits may set group/other permissions. If the file is a not a directory, the mode bits may also set user execute.
See @ref chirp_reli_setacl for alternate strong authorization control.
@param file A chirp_file handle returned by chirp_reli_open.
@param mode The new mode bits, typically <tt>0700</tt> for an executable or <tt>0600</tt> for a data file.
@param stoptime The absolute time at which to abort.
@see chirp_reli_chmod, chirp_reli_setacl, chirp_reli_getacl
*/

INT64_T chirp_reli_fchmod(struct chirp_file *file, INT64_T mode, time_t stoptime);

/** Truncate an open file.
@param file A chirp_file handle returned by chirp_reli_open.
@param length The new length of the file.
@param stoptime The absolute time at which to abort.
@see chirp_reli_open, chirp_reli_truncate
*/

INT64_T chirp_reli_ftruncate(struct chirp_file *file, INT64_T length, time_t stoptime);

/** Flush any pending changes to a file.
To improve performance, Chirp buffers small writes to files.
These writes might not be forced to disk until a later write or a call to @ref chirp_reli_close.
To force any buffered writes to disk, call this function.
@param file A chirp_file handle returned by chirp_reli_open.
@param stoptime The absolute time at which to abort.
@see chirp_reli_close
*/

INT64_T chirp_reli_flush(struct chirp_file *file, time_t stoptime);

INT64_T chirp_reli_fsync(struct chirp_file *file, time_t stoptime);

/** Get an entire file efficiently.
Reads an entire remote file, and write the contents to a standard FILE stream.
To get an entire directory tree, see @ref chirp_recursive_get instead.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param stream A standard FILE stream obtained from fopen(). Such a stream may be obtained from fopen(), or could be the standard globals
<tt>stdin</tt>, <tt>stdout</tt>, or <tt>stderr</tt>.
@param stoptime The absolute time at which to abort.
@return The size in bytes of the file, or less than zero on error.
@see chirp_reli_open
*/

INT64_T chirp_reli_getfile(const char *host, const char *path, FILE * stream, time_t stoptime);

/** Get an entire file efficiently to memory.
Reads an entire remote file into newly allocated memory.
To get an entire directory tree, see @ref chirp_recursive_get instead.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param buffer A pointer to an uninitialized pointer.
On success, this pointer will point to newly allocated memory containing the file.  The caller must then release the member by calling free().
@param stoptime The absolute time at which to abort.
@return The size of the file in bytes, or less than zero on error.
*/

INT64_T chirp_reli_getfile_buffer(const char *host, const char *path, char **buffer, time_t stoptime);

/** Put an entire file efficiently.
Reads data out of a standard I/O stream and writes it to a remote file.
To put an entire directory tree, see @ref chirp_recursive_put instead.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param stream A standard FILE stream obtained from fopen(). Such a stream may be obtained from fopen(), or could be the standard globals
<tt>stdin</tt>, <tt>stdout</tt>, or <tt>stderr</tt>.
@param mode The Unix mode bits to give to the remote file, typically <tt>0700</tt> for an executable or <tt>0600</tt> for a data file.
@param length The length in bytes of the file to write.
@param stoptime The absolute time at which to abort.
@return The size of the file in bytes, or less than zero on error.
*/

INT64_T chirp_reli_putfile(const char *host, const char *path, FILE * stream, INT64_T mode, INT64_T length, time_t stoptime);

/** Put an entire file efficiently from memory.
Reads data out of memory and writes it to a remote file.
To put an entire directory tree, see @ref chirp_recursive_put instead.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param buffer A pointer to the file data to write.
@param mode The Unix mode bits to give to the remote file, typically <tt>0700</tt> for an executable or <tt>0600</tt> for a data file.
@param length The length in bytes of the file to write.
@param stoptime The absolute time at which to abort.
@return The size of the file in bytes, or less than zero on error.
*/

INT64_T chirp_reli_putfile_buffer(const char *host, const char *path, const void *buffer, INT64_T mode, size_t length, time_t stoptime);

/** Open a file search stream
Performs a search operation on the Chirp server and stores its results to be read via readsearch
@param host The name and port of the Chirp server to access.
@param paths The pathnames of the directories to search.
@param pattern The pattern to search for
@param flags Bit flags controlling various aspects of the search
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

CHIRP_SEARCH *chirp_reli_opensearch( const char *host, const char *paths, const char *pattern, int flags, time_t stoptime );

/** Get a detailed directory listing.
Gets a detailed directory listing from a Chirp server, and then calls the callback once for each element in the directory.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to access.
@param callback The function to be called for each element in the listing.
@param arg An optional convenience pointer that will be passed to the callback function.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_getlongdir(const char *host, const char *path, chirp_longdir_t callback, void *arg, time_t stoptime);

/** Get a simple directory listing.
Gets a simple directory listing from a Chirp server, and then calls the callback once for each element in the directory.  This is a low-level function, you may find @ref chirp_reli_opendir easier to use.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to access.
@param callback The function to be called for each element in the listing.
@param arg An optional convenience pointer that will be passed to the callback function.
@param stoptime The absolute time at which to abort.
@see chirp_reli_opendir
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_getdir(const char *host, const char *path, chirp_dir_t callback, void *arg, time_t stoptime);

/**
Open a directory for listing.  This function returns a pointer to an opened directory.
You may then call @ref chirp_reli_readdir to read directory elements one by one.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to access.
@param stoptime The absolute time at which to abort.
@return On success, returns a pointer to an opaque chirp_dir object.  On failure, returns null.
@see chirp_reli_opendir chirp_reli_readdir, chirp_reli_closedir
*/

struct chirp_dir *chirp_reli_opendir(const char *host, const char *path, time_t stoptime);

/**
Read one item from a directory.  Accepts a pointer to a directory opened by @ref chirp_reli_opendir
and returns the next @ref chirp_dirent object, which describes the name and properties of the next
item in the list.  Returns null when the list is complete.
Note that this function has no timeout because it operates solely on memory structures.
@param dir A pointer to a directory returned from @ref chirp_reli_opendir.
@return On success, returns a pointer to a @ref chirp_dirent object.  Returns null when the list is complete.
@see chirp_reli_opendir chirp_reli_readdir, chirp_reli_closedir
*/

struct chirp_dirent *chirp_reli_readdir(struct chirp_dir *dir);

/**
Close a directory.  This function releases the chirp_dir object returned by @ref chirp_reli_opendir.
It should be called after @ref chirp_reli_readdir returns null to indicate the end of the directory.
Note that this function has no timeoutbecause it operates solely on memory structures.
@param dir A pointer to a directory returned from @ref chirp_reli_opendir.
@see chirp_reli_opendir chirp_reli_readdir, chirp_reli_closedir
*/

void chirp_reli_closedir(struct chirp_dir *dir);

/* FIXME document */
INT64_T chirp_reli_ticket_create(const char *host, char name[CHIRP_PATH_MAX], unsigned bits, time_t stoptime);
INT64_T chirp_reli_ticket_register(const char *host, const char *name, const char *subject, time_t duration, time_t stoptime);
INT64_T chirp_reli_ticket_delete(const char *host, const char *name, time_t stoptime);
INT64_T chirp_reli_ticket_list(const char *host, const char *subject, char ***list, time_t stoptime);
INT64_T chirp_reli_ticket_get(const char *host, const char *name, char **subject, char **ticket, time_t * duration, char ***rights, time_t stoptime);
INT64_T chirp_reli_ticket_modify(const char *host, const char *name, const char *path, const char *aclmask, time_t stoptime);

/** Get an access control list.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to access.
@param callback A function to call for each entry of the ACL.
@param arg An additional argument to pass to the callback.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_getacl(const char *host, const char *path, chirp_dir_t callback, void *arg, time_t stoptime);

/** Modify an access control list.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to access.
@param subject The name of the subject to modify, such as <tt>"hostname:somewhere.nd.edu"</tt>.
@param rights A string giving the new rights for the subject, such as <tt>"rwlda"</tt> or <tt>"."</tt> to indicate no rights.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_setacl(const char *host, const char *path, const char *subject, const char *rights, time_t stoptime);

/** Reset an access control list.  This call will remove all entries from the access control list and grant to the calling user only those rights stated here.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to access.
@param rights A string giving the new rights for the subject, such as <tt>"rwlda"</tt>.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_resetacl(const char *host, const char *path, const char *rights, time_t stoptime);

/** Identify the true location of a path.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to locate.
@param callback A function to call for each location of the file.
@param arg An additional argument to pass to the callback.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/
INT64_T chirp_reli_locate(const char *host, const char *path, chirp_loc_t callback, void *arg, time_t stoptime);

/** Return the caller's identity.
@param host The name and port of the Chirp server to access.
@param subject The buffer to fill with the caller's identity.
@param length The length of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_whoami(const char *host, char *subject, INT64_T length, time_t stoptime);

/** Return the server's identity against another server.
This causes the server to call <b>another</b> Chirp server and invoke @ref chirp_reli_whoami.
@param host The name and port of the Chirp server to access.
@param rhost The name and port of the other server to connect to.
@param subject The buffer to fill with the server's identity.
@param length The length of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_whoareyou(const char *host, const char *rhost, char *subject, INT64_T length, time_t stoptime);

/** Delete a file.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to delete.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_unlink(const char *host, const char *path, time_t stoptime);

/** Rename a file or directory.
@param host The name and port of the Chirp server to access.
@param path The current pathname of the file.
@param newpath The new pathname of the file.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_rename(const char *host, const char *path, const char *newpath, time_t stoptime);

/** Create a hard link.
@param host The name and port of the Chirp server to access.
@param path The pathname of an existing file.
@param newpath The name of the link to create.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_link(const char *host, const char *path, const char *newpath, time_t stoptime);

/** Create a symbolic link.
@param host The name and port of the Chirp server to access.
@param path The existing path to link to.
@param newpath The name of the new link to create.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_symlink(const char *host, const char *path, const char *newpath, time_t stoptime);

/** Examine a symbolic link.
@param host The name and port of the Chirp server to access.
@param path The pathname of the link to read.
@param buf The buffer in which to place the link contents.
@param length The length of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_readlink(const char *host, const char *path, char *buf, INT64_T length, time_t stoptime);


/** Create a new directory.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to create.
@param mode The unix mode bits of the new directory, typically <tt>0700</tt>.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_mkdir(const char *host, const char *path, INT64_T mode, time_t stoptime);

/** Create a new directory recursively.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to create.
@param mode The unix mode bits of the new directory, typically <tt>0700</tt>.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_mkdir_recursive(const char *host, const char *path, INT64_T mode, time_t stoptime);

/** Delete a directory if it is empty.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to delete.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
@see chirp_reli_rmall.
*/

INT64_T chirp_reli_rmdir(const char *host, const char *path, time_t stoptime);

/** Delete a directory recursively.
Deletes a directory recursively, even if it is not empty.
The recursion is performed on the file server, so this call is efficient to perform over the network.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to delete.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
@
*/

INT64_T chirp_reli_rmall(const char *host, const char *path, time_t stoptime);

/** Get file status.
If called on a symbolic link, @ref chirp_reli_stat will follow that link and obtain the status of the underlying file.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param info A pointer to a @ref chirp_stat structure to fill.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_stat(const char *host, const char *path, struct chirp_stat *info, time_t stoptime);

/** Get file or link status.
If called on a symbolic link, @ref chirp_reli_lstat will return the status of the link itself.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param info A pointer to a @ref chirp_stat structure to fill.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
@see chirp_reli_lstat, chirp_reli_fstat, chirp_reli_statfs
*/

INT64_T chirp_reli_lstat(const char *host, const char *path, struct chirp_stat *info, time_t stoptime);

/** Get filesystem status.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param info A pointer to a @ref chirp_statfs structure to fill.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_statfs(const char *host, const char *path, struct chirp_statfs *info, time_t stoptime);

/** Check access permissions.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param flags Access permission to check:
- R_OK - Check for read permission.
- W_OK - Check for write permission.
- X_OK - Check for execute permission.
@param stoptime The absolute time at which to abort.
@return If access will be granted, returns greater than or equal to zero.  On failure, returns less than zero and sets errno.
*/

INT64_T chirp_reli_access(const char *host, const char *path, INT64_T flags, time_t stoptime);

/** Change mode bits.
The mode bits may set group/other permissions. If the file is a not a directory, the mode bits may also set user execute.
See @ref chirp_reli_setacl for alternate strong authorization control.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param mode The new mode bits, typically <tt>0700</tt> for an executable or <tt>0600</tt> for a data file.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
@see chirp_reli_fchmod, chirp_reli_setacl
*/

INT64_T chirp_reli_chmod(const char *host, const char *path, INT64_T mode, time_t stoptime);

/** Change the ownership of a file.
@deprecated Note that the current Chirp file server does not make use of the Unix owner field, see @ref chirp_reli_setacl instead.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param uid The new user ID.
@param gid The new group ID.
@param stoptime The absolute time at which to abort.
@see chirp_reli_fchown, chirp_reli_setacl, chirp_reli_getacl
*/

INT64_T chirp_reli_chown(const char *host, const char *path, INT64_T uid, INT64_T gid, time_t stoptime);

/** Change the ownership of a file or link.
@deprecated Note that the current Chirp file server does not make use of the Unix owner field, see @ref chirp_reli_setacl instead.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param uid The new user ID.
@param gid The new group ID.
@param stoptime The absolute time at which to abort.
@see chirp_reli_fchown, chirp_reli_setacl, chirp_reli_getacl
*/

INT64_T chirp_reli_lchown(const char *host, const char *path, INT64_T uid, INT64_T gid, time_t stoptime);

/** Truncate a file.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param length The new length of the file.
@param stoptime The absolute time at which to abort.
@see chirp_reli_ftruncate
*/

INT64_T chirp_reli_truncate(const char *host, const char *path, INT64_T length, time_t stoptime);

/** Change the modification times of a file.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param actime The new access time.
@param modtime The new modification time.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_utime(const char *host, const char *path, time_t actime, time_t modtime, time_t stoptime);

/** Checksum a remote file.
 *
 * A hash checksum is performed remotely by the file server, so it is much more
 * efficient than pulling data and computing the hash locally.  Note that the
 * data is returned in <b>binary</b> digest form.
 *
 * @param host The name and port of the Chirp server to access.
 * @param path The pathname of the file to access.
 * @param algorithm The type of hash function to use (e.g. "md5" or "sha1").
 * @param digest The buffer to place the binary checksum digest.
 * @param stoptime The absolute time at which to abort.
 * @return On success, returns actual size of the digest.  On failure, returns less than zero and sets errno.
*/

INT64_T chirp_reli_hash(const char *host, const char *path, const char *algorithm, unsigned char digest[CHIRP_DIGEST_MAX], time_t stoptime);

/** Checksum a remote file. (DEPRECATED: use @ref chirp_reli_hash)
This MD5 checksum is performed remotely by the file server, so it is much more
efficient than computing one by invoking a local command.  Note that the data
is returned in <b>binary</b> digest form.  Use @ref md5_to_string to convert the
digest into a human readable form.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param digest The buffer to place the binary checksum digest.
@param stoptime The absolute time at which to abort.
@return On success, returns length of digest.  On failure, returns less than zero and sets errno.
@see md5_to_string
*/

INT64_T chirp_reli_md5(const char *host, const char *path, unsigned char digest[CHIRP_DIGEST_MAX], time_t stoptime);

/** Set replication factor.
Sets the number of replicas desired for the indicated file or for all newly created files.
This only has an effect if the server backend supports replication, currently only with HDFS.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to modify.  If the pathname is <tt>"@@@"</tt>, then this call sets the replication factor for all files created by this process.
@param nreps The number of replicas desired.  Zero indicates the server should use the default replication value.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_setrep(const char *host, const char *path, int nreps, time_t stoptime );

INT64_T chirp_reli_getxattr(const char *host, const char *path, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_reli_fgetxattr(struct chirp_file *file, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_reli_lgetxattr(const char *host, const char *path, const char *name, void *data, size_t size, time_t stoptime);
INT64_T chirp_reli_listxattr(const char *host, const char *path, char *list, size_t size, time_t stoptime);
INT64_T chirp_reli_flistxattr(struct chirp_file *file, char *list, size_t size, time_t stoptime);
INT64_T chirp_reli_llistxattr(const char *host, const char *path, char *list, size_t size, time_t stoptime);
INT64_T chirp_reli_setxattr(const char *host, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_reli_fsetxattr(struct chirp_file *file, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_reli_lsetxattr(const char *host, const char *path, const char *name, const void *data, size_t size, int flags, time_t stoptime);
INT64_T chirp_reli_removexattr(const char *host, const char *path, const char *name, time_t stoptime);
INT64_T chirp_reli_fremovexattr(struct chirp_file *file, const char *name, time_t stoptime);
INT64_T chirp_reli_lremovexattr(const char *host, const char *path, const char *name, time_t stoptime);

INT64_T chirp_reli_job_create (const char *host, const char *json, chirp_jobid_t *id, time_t stoptime);
INT64_T chirp_reli_job_commit (const char *host, const char *json, time_t stoptime);
INT64_T chirp_reli_job_kill (const char *host, const char *json, time_t stoptime);
INT64_T chirp_reli_job_status (const char *host, const char *json, char **status, time_t stoptime);
INT64_T chirp_reli_job_wait (const char *host, chirp_jobid_t id, INT64_T timeout, char **status, time_t stoptime);
INT64_T chirp_reli_job_reap (const char *host, const char *json, time_t stoptime);

/** Set the debug options on the remote server.
@param host The name and port of the Chirp server to access.
@param flag A debug flag to set on the server. Pass NULL to not change. Pass "clear" to clear.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_remote_debug(const char *host, const char *flag, time_t stoptime);

/** Return the local path of a file.
This function allows the caller to find out the local path where a file is stored,
which is useful if you intend to execute a program on the host by some other means to access the file.
Note that the local path will only be accessible if the directory ACL
has been readable to the user <tt>system:localuser</tt> using @ref chirp_reli_setacl.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param localpath A buffer into which the local path will be stored.
@param length The length of the buffer in bytes.
@param stoptime The absolute time at which to abort.
@return On success, greater than or equal to zero.  On failure, returns less than zero and sets errno.
*/

INT64_T chirp_reli_localpath(const char *host, const char *path, char *localpath, int length, time_t stoptime);

/** Measure remote space consumption.
This routine causes the server to internally measure the space consumed by each user of the system.
This could be a very long running function call.  It then allocates a list of @ref chirp_audit structures
describing the current space usage.  The caller is responsible for free()ing the list when done.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param list A pointer to an uninitialized <tt>struct chirp_audit *list</tt>.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_audit(const char *host, const char *path, struct chirp_audit **list, time_t stoptime);

/** Third party transfer.
Directs the server to transfer a file or directory to another (third-party) server.
If a directory is mentioned, the transfer will be performed recursively, and will
preserve the access controls present in the source directory.
@param host The name and port of the source Chirp server.
@param path The pathname of the source file or directory to transfer.
@param thirdhost The name and port of the target Chirp server.
@param thirdpath The pathname of the target file or directory.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
*/

INT64_T chirp_reli_thirdput(const char *host, const char *path, const char *thirdhost, const char *thirdpath, time_t stoptime);

/** Create a space allocation.
Creates a new directory with a firm guarantee that the user will be able to store a specific amount of data there.
@param host The name and port of the Chirp server to access.
@param path The pathname of the directory to create.
@param size The size in bytes of the allocation.
@param mode The unix mode bits of the new directory, typically <tt>0700</tt>.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
@see chirp_reli_lsalloc
*/

INT64_T chirp_reli_mkalloc(const char *host, const char *path, INT64_T size, INT64_T mode, time_t stoptime);

/** List a space allocation.
@param host The name and port of the Chirp server to access.
@param path The pathname of the file to access.
@param allocpath A buffer that will be filled with the root path of the containing allocation.
@param total A pointer to an INT64_T that will be filled with the total size of the allocation.
@param inuse A pointer to an INT64_T that will be filled with the bytes actually used in the allocation.
@param stoptime The absolute time at which to abort.
@return On success, returns greater than or equal to zero.  On failure, returns less than zero  and sets errno.
@see chirp_reli_mkalloc
*/

INT64_T chirp_reli_lsalloc(const char *host, const char *path, char *allocpath, INT64_T * total, INT64_T * inuse, time_t stoptime);

/** Perform multiple I/O operations in bulk.
This operation will perform multiple I/O operations by pipelining the requests
and the results. It is the most efficient way to perform multiple reads
and writes simultaneously, whether against one or many files.
@param list An array of @ref chirp_bulkio structures, each describing one I/O operation.
@param count The number of entries in the list.
@param stoptime The absolute time at which to abort.
@return If all operations in the array succeed, this function will return greater than or equal to zero.  If one or more operations fail, this function will return less than zero.  The result of each individual operation may be determined by examining the result and errnum fields set in each @ref chirp_bulkio structure.
*/

INT64_T chirp_reli_bulkio(struct chirp_bulkio *list, int count, time_t stoptime);

/** Return the current buffer block size.
This module performs input and output buffering to improve the performance of small I/O operations.
Operations larger than the buffer size are sent directly over the network, while those smaller are
aggregated together.  This function returns the current buffer size.
@return The current file buffer size.
*/

INT64_T chirp_reli_blocksize_get();

/** Set the buffer block size.
This module performs input and output buffering to improve the performance of small I/O operations.
Operations larger than the buffer size are sent directly over the network, while those smaller are
aggregated together.  This function sets the current buffer size.
@param bs The new buffer block size.
*/

void chirp_reli_blocksize_set(INT64_T bs);

/** Prepare to fork in a parallel program.
The Chirp library is not thread-safe, but it can be used in a program
that exploits parallelism by calling fork().  Before calling fork, this
function must be invoked to clean up shared state such as TCP connections.
After forking, each process will maintain its own connection to each Chirp server.
*/

void chirp_reli_cleanup_before_fork();

/** Closes the link to the host, if active.
@param host A hostname may be a domain name or an IP address, followed by an optional colon and port number.
*/
void chirp_reli_disconnect( const char *host );

#endif

/* vim: set noexpandtab tabstop=8: */
