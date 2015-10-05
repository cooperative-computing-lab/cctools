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

static char *monitor_exe  = NULL;

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

//atexit handler
void resource_monitor_delete_exe(void)
{
	debug(D_RMON, "unlinking %s\n", monitor_exe);
	unlink(monitor_exe);
}

char *resource_monitor_copy_to_wd(const char *path_from_cmdline)
{
	char *mon_unique;
	char *monitor_org;
	monitor_org = resource_monitor_locate(path_from_cmdline);

	if(!monitor_org)
		fatal("Monitor program could not be found.\n");

	mon_unique = string_format("monitor-%d", getpid());

	debug(D_RMON,"copying monitor %s to %s.\n", monitor_org, mon_unique);

	if(copy_file_to_file(monitor_org, mon_unique) == 0)
		fatal("Could not copy monitor %s to %s in local directory: %s\n",
				monitor_org, mon_unique, strerror(errno));

	atexit(resource_monitor_delete_exe);
	chmod(mon_unique, 0777);

	monitor_exe = mon_unique;

	return mon_unique;
}

//Using default sampling interval. We may want to add an option
//to change it.
char *resource_monitor_rewrite_command(const char *cmdline, const char *monitor_path, const char *template_filename, const char *limits_filename,
					   const char *extra_monitor_options,
					   int time_series, int inotify_stats)
{
	char cmd_builder[PATH_MAX];
	int  index;

	if(!monitor_path && !monitor_exe)
		monitor_exe = resource_monitor_copy_to_wd(NULL);


	if(!monitor_path)
		monitor_path = monitor_exe;

	index = sprintf(cmd_builder, "./%s --with-output-files=%s ", monitor_path, template_filename);

	if(time_series)
		index += sprintf(cmd_builder + index, "--with-time-series ");

	if(inotify_stats)
		index += sprintf(cmd_builder + index, "--with-inotify ");

	if(limits_filename)
		index += sprintf(cmd_builder + index, "--limits-file=%s ", limits_filename);

	if(extra_monitor_options)
		index += sprintf(cmd_builder + index, "%s ", extra_monitor_options);

	sprintf(cmd_builder + index, "-- %s", cmdline);

	return xxstrdup(cmd_builder);
}

/* vim: set noexpandtab tabstop=4: */
