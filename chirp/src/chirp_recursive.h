/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_RECURSIVE_H
#define CHIRP_RECURSIVE_H

#include "int_sizes.h"
#include <time.h>

/** @file chirp_recursive.h
A high level interface to put and get large directories trees to and from Chirp servers.
*/

/** Recursively put a file or directory to a Chirp server.
Relies on @ref chirp_reli_putfile and similar calls
to handle a number of failure cases.
@param hostport The host and port of the Chirp server.
@param sourcepath The path to the local file or directory to send.
@param targetpath The name to give the file or directory on the server.
@param stoptime The absolute time at which to abort.
@return On success, returns the sum of file bytes transferred.  On failure, returns less than zero and sets errno appropriately.
*/

INT64_T chirp_recursive_put(const char *hostport, const char *sourcepath, const char *targetpath, time_t stoptime);

/** Recursively get a file or directory from a Chirp server.
Relies on @ref chirp_reli_getfile and similar calls
to handle a number of failure cases.
@param hostport The host and port of the Chirp server.
@param sourcepath The path to the remote file or directory to get.
@param targetpath The name to give the local file or directory.
@param stoptime The absolute time at which to abort.
@return On success, returns the sum of file bytes transferred.  On failure, returns less than zero and sets errno appropriately.
*/

INT64_T chirp_recursive_get(const char *hostport, const char *sourcepath, const char *targetpath, time_t stoptime);

#endif

/* vim: set noexpandtab tabstop=8: */
