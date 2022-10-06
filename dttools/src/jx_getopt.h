/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file jx_getopt.h Pull command line args from a JSON document.
 *
 * This is a wrapper around getopt_long that supports reading arguments
 * from a JX object during command line parsing. The object's keys are
 * interpreted as long option names, and the values are stored in optarg.
 * Currently only string, integer, float, and boolean values are written
 * to optarg. Other value types result in optarg being set to NULL. To
 * access the raw JX struture provided as an option argument, callers
 * can inspect jx_optarg. JX-aware programs can check jx_optarg when
 * reading JX values from the command line to simplify parsing and avoid
 * quoting issues.
 */

#ifndef JX_GETOPT_H
#define JX_GETOPT_H

#include <unistd.h>
#include <getopt.h>

#include "jx.h"

extern struct jx *jx_optarg;

/** Process command lines from a JX document.
 * The pushed args will be processesed next, before any remaining command line
 * args or previously pushed JX args.
 * @param j The args to process.
 */
void jx_getopt_push(struct jx *j);

/** Parse the next argument.
 * If there are no JX args to be processed, this is the same as calling
 * getopt_long() directly.
 * @returns 0 on error. Do not use this as a valid option val.
 */
int jx_getopt(int argc, char *const argv[], const char *optstring, const struct option *longopts, int *longindex);

#endif
