/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "cctools.h"
#include "debug.h"
#include "getopt.h"
#include "parrot_client.h"
#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "pfs_resolve.h"
#include "pfs_mountfile.h"

#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static void show_help()
{
	const char *optfmt = "%2s %-20s %s%s\n";
	printf("usage: parrot_namespace [options] <command>\n");
	printf("\n");
	printf("Where options are:\n");
	printf(optfmt, "-M", "--mount /foo=/bar", "Mount (redirect) /foo to /bar", " (PARROT_MOUNT_STRING)");
	printf(optfmt, "-m", "--ftab-file <file>", "Use <file> as a mountlist", " (PARROT_MOUNT_FILE)");
	printf(optfmt, "", "--parrot-path <path>", "Path to parrot_run", " (PARROT_PATH)");
	printf(optfmt, "-d", "--debug <flags>", "Enable debugging for this subsystem", " (PARROT_DEBUG_FLAGS)");
	printf(optfmt, "-o", "--debug-file <file>", "Send debugging to this file", " (PARROT_DEBUG_FILE)");
	printf(optfmt, "-v", "--version", "Show version number", "");
	printf(optfmt, "-h", "--help", "Help: Show these options", "");
}

typedef enum {
	LONG_OPT_PARROT_PATH = UCHAR_MAX+1,

} long_options_t;

int main( int argc, char *argv[] )
{
	char parrot_path[PATH_MAX];
	int parrot_in_parrot = 0;
	strcpy(parrot_path, "parrot_run");

	char *s;
	s = getenv("PARROT_DEBUG_FLAGS");
	if(s) {
		char *x = xxstrdup(s);
		int nargs;
		char **args;
		if(string_split(x,&nargs,&args)) {
			for(int i=0;i<nargs;i++) {
				debug_flags_set(args[i]);
			}
		}
		free(x);
	}

	char buf[4096];
	if (parrot_version(buf, sizeof(buf)) >= 0) {
		debug(D_DEBUG, "running under parrot %s\n", buf);
		parrot_in_parrot = 1;
		if (parrot_fork_namespace() < 0) {
			fatal("cannot dissociate from parent namespace");
		}
	}

	s = getenv("PARROT_MOUNT_FILE");
	if(s && parrot_in_parrot) pfs_mountfile_parse_file(s);

	s = getenv("PARROT_MOUNT_STRING");
	if(s && parrot_in_parrot) pfs_mountfile_parse_string(s);

	s = getenv("PARROT_PATH");
	if (s) snprintf(parrot_path, PATH_MAX, "%s", s);

static const struct option long_options[] = {
	{"help",  no_argument, 0, 'h'},
	{"version", no_argument, 0, 'v'},
	{"debug", required_argument, 0, 'd'},
	{"debug-file", required_argument, 0, 'o'},
	{"mount", required_argument, 0, 'M'},
	{"tab-file", required_argument, 0, 'm'},
	{"parrot-path", required_argument, 0, LONG_OPT_PARROT_PATH},
	{0,0,0,0}
};

	int c;
	while((c=getopt_long(argc,argv,"d:vhM:m:o:", long_options, NULL)) > -1) {
		switch(c) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'h':
			show_help();
			return 0;
			break;
		case 'v':
			cctools_version_print(stdout,"parrot_mount");
			return 0;
		case 'm':
			if (parrot_in_parrot) pfs_mountfile_parse_file(optarg);
			break;
		case 'M':
			if (parrot_in_parrot) pfs_mountfile_parse_string(optarg);
			break;
		case LONG_OPT_PARROT_PATH:
			snprintf(parrot_path, PATH_MAX, "%s", optarg);
			break;
		default:
			show_help();
			return 1;
			break;
		}
	}

	if(optind >= argc) {
		show_help();
		return 1;
	}

	if (parrot_in_parrot) {
		if (execvp(argv[optind], &argv[optind]) < 0) {
			fatal("failed to exec %s: %s\n", argv[optind], strerror(errno));
		}
	} else {
		if (execvp(parrot_path, argv) < 0) {
			fatal("failed to exec %s: %s\n", parrot_path, strerror(errno));
		}
	}
}

/* vim: set noexpandtab tabstop=4: */
