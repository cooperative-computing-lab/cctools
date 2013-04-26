/*
Copyright (C) 2013- The University of Notre Dame This software is
distributed under the GNU General Public License.  See the file
COPYING for details.
*/

#define RESOURCE_MONITOR_ENV_VAR "CCTOOLS_RESOURCE_MONITOR"

#define RMONITOR_DEFAULT_NAME  "\001"     /* Tells resource_monitor_rewrite_command to use the default
											 name of the log file. It does not look pretty, but
											 allows us to have a single argument per log file. It does
											 not work in the event that the user want to name a file
											 with the octal character "\001". */

#define RMONITOR_DONT_GENERATE  NULL     /* Tells resource_monitor_rewrite_command not to add the
											corresponding log file. */

/** Wraps a command line with the resource monitor.
The command line is rewritten to be run inside the monitor with
the corresponding log file options.
@param cmdline A command line.
@param template The filename template for all the log files, used when RMONITOR_DEFAULT_NAME is specified for the parameters below.
@param summary The name of the summary file to be generated. If RMONITOR_DEFAULT_NAME, use the default name from resource_monitor. If RMONITOR_DONT_GENERATE, then it tells the monitor no to generate the summary file.
@param time_series The name of the time-series log to be generated. Use RMONITOR_DEFAULT_NAME and RMONITOR_DONT_GENERATE as with summary.
@param opened_files The name of the list of opened files log to be generated. Use RMONITOR_DEFAULT_NAME and RMONITOR_DONT_GENERATE as with summary.
@return A new command line that runs the original command line wrapped with the resource monitor.
*/

char *resource_monitor_rewrite_command(char *cmdline, char *template, char *summary, char *time_series, char *opened_files);

/** Looks for a resource monitor executable, and makes a copy in
current working directory.
The resource monitor executable is searched, in order, in the following locations: the path given as an argument, the value of the environment variable RESOURCE_MONITOR_ENV_VAR, the current working directory, the cctools installation directory. The copy is deleted when the current process exits.
@param path_from_cmdline The first path to look for the resource monitor executable.
@return The name of the monitor executable in the current working directory.
*/

char *resource_monitor_copy_to_wd(char *path_from_cmdline);

