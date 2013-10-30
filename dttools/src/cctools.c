/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "cctools.h"
#include "debug.h"

#include <assert.h>
#include <stdio.h>

void cctools_version_print (FILE *stream, const char *cmd)
{
	fprintf(stream, "%s version %s (released %s)\n", cmd, CCTOOLS_VERSION, CCTOOLS_RELEASE_DATE);
	fprintf(stream, "\tBuilt by %s@%s on %s at %s\n", BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
	fprintf(stream, "\tSystem: %s\n", CCTOOLS_SYSTEM_INFORMATION);
	fprintf(stream, "\tConfiguration: %s\n", CCTOOLS_CONFIGURE_ARGUMENTS);
}

void cctools_version_debug (int type, const char *cmd)
{
	debug(type, "%s version %s (released %s)", cmd, CCTOOLS_VERSION, CCTOOLS_RELEASE_DATE);
	debug(type, "Built by %s@%s on %s at %s", BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
	debug(type, "System: %s", CCTOOLS_SYSTEM_INFORMATION);
	debug(type, "Configuration: %s", CCTOOLS_CONFIGURE_ARGUMENTS);
}

/* vim: set noexpandtab tabstop=4: */
