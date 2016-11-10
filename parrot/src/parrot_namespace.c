/*
Copyright (C) 2016- The University of Notre Dame
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
	printf(optfmt, "-l", "--ld-path=<path>", "Path to ld.so to use", " (PARROT_LDSO_PATH)");
	printf(optfmt, "", "--parrot-path <path>", "Path to parrot_run", " (PARROT_PATH)");
	printf(optfmt, "-v", "--version", "Show version number", "");
	printf(optfmt, "-h", "--help", "Help: Show these options", "");
}

typedef enum {
	LONG_OPT_PARROT_PATH = UCHAR_MAX+1,

} long_options_t;

static const struct option long_options[] = {
	{"help",  no_argument, 0, 'h'},
	{"version", no_argument, 0, 'v'},
	{"mount", required_argument, 0, 'M'},
	{"tab-file", required_argument, 0, 'm'},
	{"ld-path", required_argument, 0, 'l'},
	{"parrot-path", required_argument, 0, LONG_OPT_PARROT_PATH},
	{0,0,0,0}
};

int main( int argc, char *argv[] )
{
	char parrot_path[PATH_MAX];
	char ldso[PATH_MAX];
	strcpy(parrot_path, "parrot_run");
	strcpy(ldso, "");

	char *s = getenv("PARROT_PATH");
	if (s) snprintf(parrot_path, PATH_MAX, "%s", s);

	s = getenv("PARROT_LDSO_PATH");
	if (s) snprintf(ldso, PATH_MAX, "%s", s);

	int c;
	const char *optstring = "vhM:m:l:";
	while((c=getopt_long(argc, argv, optstring, long_options, NULL)) > -1) {
		switch(c) {
		case 'h':
			show_help();
			return 0;
		case 'v':
			cctools_version_print(stdout,"parrot_mount");
			return 0;
		case 'l':
			snprintf(ldso, PATH_MAX, "%s", optarg);
			break;
		case LONG_OPT_PARROT_PATH:
			snprintf(parrot_path, PATH_MAX, "%s", optarg);
			break;
		default:
			break;
		}
	}

	char buf[4096];
	if (parrot_version(buf, sizeof(buf)) >= 0) {
		debug(D_DEBUG, "running under parrot %s\n", buf);
		if (parrot_fork_namespace(ldso) < 0) {
			fatal("cannot dissociate from parent namespace");
		}
	} else {
		if (execvp(parrot_path, argv) < 0) {
			fatal("failed to exec %s: %s\n", parrot_path, strerror(errno));
		}
	}

	s = getenv("PARROT_MOUNT_FILE");
	if (s) pfs_mountfile_parse_file(s);

	s = getenv("PARROT_MOUNT_STRING");
	if (s) pfs_mountfile_parse_string(s);

	optind = 1;
	while((c=getopt_long(argc, argv, optstring, long_options, NULL)) > -1) {
		switch(c) {
		case 'm':
			pfs_mountfile_parse_file(optarg);
			break;
		case 'M':
			pfs_mountfile_parse_string(optarg);
			break;
		case 'h':
		case 'v':
		case 'l':
		case LONG_OPT_PARROT_PATH:
			break;
		default:
			show_help();
			return 1;
		}
	}

	if(optind >= argc) {
		show_help();
		return 1;
	}

	if (execvp(argv[optind], &argv[optind]) < 0) {
		fatal("failed to exec %s: %s\n", argv[optind], strerror(errno));
	}
}

/* vim: set noexpandtab tabstop=4: */
