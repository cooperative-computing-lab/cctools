/*
Copyright (C) 2022 The University of Notre Dame This software is
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

	if(path_from_cmdline) {
		debug(D_RMON,"trying executable from path provided at command line.\n");
		monitor_path = resource_monitor_check_path(path_from_cmdline, NULL);
		if(monitor_path) {
			return monitor_path;
		}

		// if path given explicitely, then return as not found.
		return NULL;
	}

	test_path = getenv(RESOURCE_MONITOR_ENV_VAR);
	if(test_path) {
		debug(D_RMON,"trying executable from $%s.\n", RESOURCE_MONITOR_ENV_VAR);
		monitor_path = resource_monitor_check_path(test_path, NULL);
		if(monitor_path) {
			return monitor_path;
		}

		// if env var given explicitely, then return as not found.
		return NULL;
	}

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
char *resource_monitor_write_command(const char *monitor_path, const char *template_filename, const struct rmsummary *limits, const char *extra_monitor_options, int debug_output, int time_series, int inotify_stats, const char *measure_dir)
{
	buffer_t cmd_builder;
	buffer_init(&cmd_builder);

	if(!monitor_path)
		fatal("Monitor path should be specified.");

	//useful when debugging (uncomment):
	//buffer_printf(&cmd_builder, "valgrind  --leak-check=full -- ");

	buffer_printf(&cmd_builder, "%s --no-pprint", monitor_path);

	buffer_printf(&cmd_builder, " --with-output-files=%s", template_filename);

	if(debug_output)
		buffer_printf(&cmd_builder, " -dall -o %s.debug", template_filename);

	if(time_series)
		buffer_printf(&cmd_builder, " --with-time-series");

	if(inotify_stats)
		buffer_printf(&cmd_builder, " --with-inotify");

	if(measure_dir)
		buffer_printf(&cmd_builder, " --measure-dir %s", measure_dir);

	if(limits) {
		size_t i = 0;
		const char **resources = rmsummary_list_resources();
		for(i = 0; i < rmsummary_num_resources(); i++) {
			const char *r = resources[i];
			double v = rmsummary_get(limits, r);
			if(v > -1) {
				buffer_printf(&cmd_builder, " -L '%s: %s'", r, rmsummary_resource_to_str(r, v, 0));
			}
		}
	}

	if(extra_monitor_options)
		buffer_printf(&cmd_builder, " %s", extra_monitor_options);

	buffer_printf(&cmd_builder, " --sh []");

	char *result;
	buffer_dupl(&cmd_builder, &result, 0);
	buffer_free(&cmd_builder);

	return result;
}

/* vim: set noexpandtab tabstop=8: */
