/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CCTOOLS_H
#define CCTOOLS_H

#include <stdio.h>

/** @file cctools.h Common CCTools functions for version and common
    output management.
*/


/** Print the version of the software to the file stream.
    @param stream The file stream to print to.
    @param cmd    The name of the program running (argv[0]).
  */
void print_version (FILE *stream, const char *cmd);

/** Create a new buffer.
    @param type   The debug type.
    @param cmd    The name of the program running (argv[0]).
  */
void debug_version (int type, const char *cmd);


#endif /* CCTOOLS_H */
