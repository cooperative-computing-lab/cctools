/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "cctools.h"
#include "debug.h"
#include <getopt.h>
#include "parrot_client.h"
#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "pfs_resolve.h"
#include "pfs_mountfile.h"
#include "list.h"

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
	{"parrot-path", required_argument, 0, LONG_OPT_PARROT_PATH},
	{0,0,0,0}
};

int main( int argc, char *argv[] )
{
	struct list *mountfiles = list_create();
	struct list *mountstrings = list_create();

	char *parrot_path = "parrot_run";

	if (getenv("PARROT_PATH")) parrot_path = getenv("PARROT_PATH");

	if (getenv("PARROT_MOUNT_FILE")) list_push_head(mountfiles, getenv("PARROT_MOUNT_FILE"));
	if (getenv("PARROT_MOUNT_STRING")) list_push_head(mountstrings, getenv("PARROT_MOUNT_STRING"));

	int c;
	while((c=getopt_long(argc, argv, "vhM:m:l:", long_options, NULL)) > -1) {
		switch(c) {
		case 'm':
			list_push_head(mountstrings, xxstrdup(optarg));
			break;
		case 'M':
			list_push_head(mountfiles, xxstrdup(optarg));
			break;
		case 'h':
			show_help();
			return 0;
		case 'v':
			cctools_version_print(stdout,"parrot_mount");
			return 0;
		case LONG_OPT_PARROT_PATH:
			parrot_path = xxstrdup(optarg);
			break;
		default:
			show_help();
			return 1;
		}
	}

	char buf[4096];
	if (parrot_version(buf, sizeof(buf)) >= 0) {
		debug(D_DEBUG, "running under parrot %s\n", buf);
		if (parrot_fork_namespace() < 0) {
			fatal("cannot dissociate from parent namespace");
		}
	} else {
		if (execvp(parrot_path, argv) < 0) {
			fatal("failed to exec %s: %s\n", parrot_path, strerror(errno));
		}
	}

	char *s;
	list_first_item(mountfiles);
	while ((s = list_next_item(mountfiles))) pfs_mountfile_parse_file(s);

	list_first_item(mountstrings);
	while ((s = list_next_item(mountstrings))) pfs_mountfile_parse_string(s);

	if (execvp(argv[optind], &argv[optind]) < 0) {
		fatal("failed to exec %s: %s\n", argv[optind], strerror(errno));
	}
}

/* vim: set noexpandtab tabstop=4: */
