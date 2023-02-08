/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_H
#define CCTOOLS_H

#include <stdio.h>
#include <stdint.h>

/** @file cctools.h Common CCTools functions for version and common
	output management.
*/


/** Print the version of the software to the file stream.
	@param stream The file stream to print to.
	@param cmd    The name of the program running (argv[0]).
  */
void cctools_version_print (FILE *stream, const char *cmd);

/** Create a new buffer.
	@param type   The debug type.
	@param cmd    The name of the program running (argv[0]).
  */
void cctools_version_debug (uint64_t type, const char *cmd);

int cctools_version_cmp (const char *v1, const char *v2);

#endif /* CCTOOLS_H */
