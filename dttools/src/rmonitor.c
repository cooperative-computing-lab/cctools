/*
Copyright (C) 2013- The University of Notre Dame This software is
distributed under the GNU General Public License.  See the file
COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <stddef.h>

#include <sys/stat.h>

#include "debug.h"
#include "copy_stream.h"
#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "rmonitor.h"

static char *resource_monitor_check_path(const char *path, const char *executable_opt) {
	struct stat buf;

	if(path)
	{
		char *monitor_path;
		if(executable_opt) {
			monitor_path = string_format("%s/%s", path, executable_opt);
		}
		else {
			monitor_path = xxstrdup(path);
		}

		if(stat(monitor_path, &buf) == 0)
			if(S_ISREG(buf.st_mode) && access(monitor_path, R_OK|X_OK) == 0)
				return monitor_path;

		/* If we get here, we did not find a valid monitor at path */
		free(monitor_path);
	}

	return NULL;
}


char *resource_monitor_locate(const char *path_from_cmdline)
{
	char *test_path;
	char *monitor_path;

	debug(D_RMON,"locating resource monitor executable...\n");

	debug(D_RMON,"trying executable from path provided at command line.\n");
	monitor_path = resource_monitor_check_path(path_from_cmdline, NULL);
	if(monitor_path)
		return monitor_path;

	debug(D_RMON,"trying executable from $%s.\n", RESOURCE_MONITOR_ENV_VAR);
	test_path = getenv(RESOURCE_MONITOR_ENV_VAR);
	monitor_path = resource_monitor_check_path(test_path, NULL);
	if(monitor_path)
		return monitor_path;

	debug(D_RMON,"trying executable at local directory.\n");
	//LD_CONFIG version.
	monitor_path = resource_monitor_check_path("./", "resource_monitor");
	if(monitor_path)
		return monitor_path;

	debug(D_RMON,"trying executable at PATH.\n");
	//LD_CONFIG version.
	monitor_path = path_which("resource_monitor");
	if(monitor_path)
		return monitor_path;

	//static "vanilla" version
	monitor_path = path_which("resource_monitorv");
	if(monitor_path)
		return monitor_path;

	debug(D_RMON,"trying executable at installed path location.\n");
	//LD_CONFIG version.
	monitor_path = resource_monitor_check_path(INSTALL_PATH, "bin/resource_monitor");
	if(monitor_path)
		return monitor_path;

	//static "vanilla" version
	monitor_path = resource_monitor_check_path(INSTALL_PATH, "bin/resource_monitorv");
	if(monitor_path)
		return monitor_path;

	return NULL;
}


//Using default sampling interval. We may want to add an option
//to change it.
char *resource_monitor_rewrite_command(const char *cmdline, const char *monitor_path, const char *template_filename, const char *limits_filename,
					   const char *extra_monitor_options,
					   int time_series, int inotify_stats)
{


	buffer_t cmd_builder;
	buffer_init(&cmd_builder);

	if(!monitor_path)
		fatal("Monitor path should be specified.");

	buffer_printf(&cmd_builder, "./%s", monitor_path);
	buffer_printf(&cmd_builder, " --with-output-files=%s", template_filename);

	if(time_series)
		buffer_printf(&cmd_builder, " --with-time-series");

	if(inotify_stats)
		buffer_printf(&cmd_builder, " --with-inotify");

	if(limits_filename)
		buffer_printf(&cmd_builder, " --limits-file=%s", limits_filename);

	if(extra_monitor_options)
		buffer_printf(&cmd_builder, " %s", extra_monitor_options);

	char *cmdline_escaped = string_escape_shell(cmdline);
	buffer_printf(&cmd_builder, " --sh %s", cmdline_escaped);
	free(cmdline_escaped);

	char *result = xxstrdup(buffer_tostring(&cmd_builder));
	buffer_free(&cmd_builder);

	return result;
}

/* vim: set noexpandtab tabstop=4: */
