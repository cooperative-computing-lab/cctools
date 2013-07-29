/*
Copyright (C) 2013- The University of Notre Dame This software is
distributed under the GNU General Public License.  See the file
COPYING for details.
*/

#ifndef __RMONITOR_H
#define __RMONITOR_H

#include "rmsummary.h"

#define RESOURCE_MONITOR_ENV_VAR "CCTOOLS_RESOURCE_MONITOR"


/** Wraps a command line with the resource monitor.
The command line is rewritten to be run inside the monitor with
the corresponding log file options.
@param cmdline A command line.
@param template The filename template for all the log files.
@param limits The name of the resource limits file. NULL if no limits are going to be specified.
@param summary Generate <template>.summary if not 0.
@param time_series Generate <template>.series if not 0.
@param opened_files Generate <template>.files if not 0.
@return A new command line that runs the original command line wrapped with the resource monitor.
*/

char *resource_monitor_rewrite_command(char *cmdline, char *template_filename, char *limits_filename,
				       const char *extra_monitor_options,
				       int summary, int time_series, int opened_files);

/** Looks for a resource monitor executable, and makes a copy in
current working directory.
The resource monitor executable is searched, in order, in the following locations: the path given as an argument, the value of the environment variable RESOURCE_MONITOR_ENV_VAR, the current working directory, the cctools installation directory. The copy is deleted when the current process exits.
@param path_from_cmdline The first path to look for the resource monitor executable.
@return The name of the monitor executable in the current working directory.
*/

char *resource_monitor_copy_to_wd(char *path_from_cmdline);

/**  Reads a single resources file from filename **/
struct rmsummary *resident_monitor_parse_resources_file(char *filename);


#endif
