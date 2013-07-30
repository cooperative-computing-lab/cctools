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
#include "stringtools.h"
#include "xxmalloc.h"

#include "rmonitor.h"

static char *monitor_exe  = NULL;

//BUG: Too much code repetition!
char *resource_monitor_locate(char *path_from_cmdline)
{
	char *monitor_path;
	struct stat buf;	

	debug(D_RMON,"locating resource monitor executable...\n");

	monitor_path = path_from_cmdline;
	if(monitor_path)
	{
		debug(D_RMON,"trying executable from path provided at command line.\n");
		if(stat(monitor_path, &buf) == 0)
			if(S_ISREG(buf.st_mode) && access(monitor_path, R_OK|X_OK) == 0)
				return xxstrdup(monitor_path);	
	}

	monitor_path = getenv(RESOURCE_MONITOR_ENV_VAR);
	if(monitor_path)
	{
		debug(D_RMON,"trying executable from $%s.\n", RESOURCE_MONITOR_ENV_VAR);
		if(stat(monitor_path, &buf) == 0)
			if(S_ISREG(buf.st_mode) && access(monitor_path, R_OK|X_OK) == 0)
				return xxstrdup(monitor_path);	
	}

	debug(D_RMON,"trying executable at local directory.\n");
	//LD_CONFIG version.
	monitor_path = string_format("./resource_monitor");
	if(stat(monitor_path, &buf) == 0)
		if(S_ISREG(buf.st_mode) && access(monitor_path, R_OK|X_OK) == 0)
			return xxstrdup(monitor_path);	

	//static "vanilla" version
	free(monitor_path);
	monitor_path = string_format("./resource_monitorv");
	if(stat(monitor_path, &buf) == 0)
		if(S_ISREG(buf.st_mode) && access(monitor_path, R_OK|X_OK) == 0)
			return xxstrdup(monitor_path);	

	debug(D_RMON,"trying executable at installed path location.\n");
	//LD_CONFIG version.
	free(monitor_path);
	monitor_path = string_format("%s/bin/resource_monitor", INSTALL_PATH);
	if(stat(monitor_path, &buf) == 0)
		if(S_ISREG(buf.st_mode) && access(monitor_path, R_OK|X_OK) == 0)
			return xxstrdup(monitor_path);	
	
	free(monitor_path);
	monitor_path = string_format("%s/bin/resource_monitorv", INSTALL_PATH);
	if(stat(monitor_path, &buf) == 0)
		if(S_ISREG(buf.st_mode) && access(monitor_path, R_OK|X_OK) == 0)
			return xxstrdup(monitor_path);	

	return NULL;
}

//atexit handler
void resource_monitor_delete_exe(void)
{
	debug(D_RMON, "unlinking %s\n", monitor_exe);
	unlink(monitor_exe);
}

char *resource_monitor_copy_to_wd(char *path_from_cmdline)
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
char *resource_monitor_rewrite_command(char *cmdline, char *template_filename, char *limits_filename,
				       const char *extra_monitor_options,
				       int summary, int time_series, int opened_files)
{
	char cmd_builder[PATH_MAX];
	int  index;
	
	if(!monitor_exe)
		monitor_exe = resource_monitor_copy_to_wd(NULL);

	index = sprintf(cmd_builder, "./%s --with-disk-footprint ", monitor_exe);

	index += sprintf(cmd_builder + index, "--with-output-files=%s ", template_filename);

	if(!summary)
		index += sprintf(cmd_builder + index, "--without-summary-file ");

	if(!time_series)
		index += sprintf(cmd_builder + index, "--without-time-series ");

	if(!opened_files)
		index += sprintf(cmd_builder + index, "--without-opened-files ");

	if(limits_filename)
		index += sprintf(cmd_builder + index, "--limits-file=%s ", limits_filename);

	if(extra_monitor_options)
		index += sprintf(cmd_builder + index, "%s ", extra_monitor_options);

	sprintf(cmd_builder + index, "-- %s", cmdline);

	return xxstrdup(cmd_builder);
}






