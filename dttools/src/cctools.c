/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "cctools.h"
#include "debug.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

void cctools_version_print (FILE *stream, const char *cmd)
{
	fprintf(stream, "%s version %s (released %s)\n", cmd, CCTOOLS_VERSION, CCTOOLS_RELEASE_DATE);
	fprintf(stream, "\tBuilt by %s@%s on %s\n", BUILD_USER, BUILD_HOST, BUILD_DATE);
	fprintf(stream, "\tSystem: %s\n", CCTOOLS_SYSTEM_INFORMATION);
	fprintf(stream, "\tConfiguration: %s\n", CCTOOLS_CONFIGURE_ARGUMENTS);
}

void cctools_version_debug (uint64_t type, const char *cmd)
{
	debug(type, "%s version %s (released %s)", cmd, CCTOOLS_VERSION, CCTOOLS_RELEASE_DATE);
	debug(type, "Built by %s@%s on %s", BUILD_USER, BUILD_HOST, BUILD_DATE);
	debug(type, "System: %s", CCTOOLS_SYSTEM_INFORMATION);
	debug(type, "Configuration: %s", CCTOOLS_CONFIGURE_ARGUMENTS);
}

int cctools_version_cmp (const char *v1, const char *v2)
{
	int major1 = 0, minor1 = 0, micro1 = 0;
	int major2 = 0, minor2 = 0, micro2 = 0;

	sscanf(v1, "%d.%d.%d", &major1, &minor1, &micro1);
	sscanf(v2, "%d.%d.%d", &major2, &minor2, &micro2);

	int rc = memcmp(&major1, &major2, sizeof(int));
	if (!rc)
		rc = memcmp(&minor1, &minor2, sizeof(int));
	if (!rc)
		rc = memcmp(&micro1, &micro2, sizeof(int));
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
