/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "mapreduce.h"

#include "debug.h"
#include "stringtools.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

struct mr_reduce_config {
	const char	*scratch_dir;
	int             rid;
	int             nmappers;
	int             nreducers;
};

static int reduce( const char *inputfile, const char *outputfile ) {
	int ifd = -1, ofd = -1, result = EXIT_FAILURE;
	pid_t pid;

	ifd = open(inputfile, O_RDONLY);
	if (ifd < 0) {
		debug(D_NOTICE, "cannot open %s: %s", inputfile, strerror(errno));
		goto r_return;
	}

	ofd = open(outputfile, O_WRONLY|O_CREAT|O_TRUNC, 0666);
	if (ofd < 0) {
		debug(D_NOTICE, "cannot open %s: %s", outputfile, strerror(errno));
		goto r_return;
	}
	
	switch (pid = fork()) {
		case -1:
			goto r_return;
			break;
		case 0:
			dup2(ifd, STDIN_FILENO);
			dup2(ofd, STDOUT_FILENO);
			if (execlp(MR_REDUCER, MR_REDUCER, NULL) < 0) {
				debug(D_NOTICE, "cannot execute %s: %s", MR_REDUCER, strerror(errno));
				goto r_return;
			}
			exit(EXIT_FAILURE);
			break;
		default:
			if (wait(NULL) >= 0) {
				result = EXIT_SUCCESS;
			}
			break;
	}

r_return:
	if (ifd >= 0) close(ifd);
	if (ofd >= 0) close(ofd);
	return result;
}

static int mr_reduce( struct mr_reduce_config *cfg ) {
	char inputfile[MR_MAX_STRLEN];
	char outputfile[MR_MAX_STRLEN];

	snprintf(inputfile, MR_MAX_STRLEN, "%s/reduce.input.%d", cfg->scratch_dir, cfg->rid);
	snprintf(outputfile, MR_MAX_STRLEN, "%s/reduce.output.%d", cfg->scratch_dir, cfg->rid);

	return reduce(inputfile, outputfile);
}

static void show_help( const char *cmd ) {
	printf("Use: %s [options] <scratch_dir> <rid> <nmappers> <nreducers>\n", cmd);
	printf("where options are:\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

static void show_version( const char *cmd ) {       
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

int main( int argc, char *argv[] ) {
	const char *progname = "mr_reduce";
	struct mr_reduce_config cfg;
	char c;
	
	debug_config(progname);

	while ((c = getopt(argc, argv, "d:o:hv")) != (char)-1) {
		switch (c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'h':
				show_help(progname);
				exit(EXIT_SUCCESS);
				break;
			case 'v':
				show_version(progname);
				exit(EXIT_SUCCESS);
				break;
		}
	}
	
	if ((argc - optind) != 4) {
		show_help(progname);
		exit(EXIT_FAILURE);
	}

	cfg.scratch_dir	= argv[optind];
	cfg.rid		= atoi(argv[optind + 1]);
	cfg.nmappers	= atoi(argv[optind + 2]);
	cfg.nreducers	= atoi(argv[optind + 3]);

	return mr_reduce(&cfg);
}

// vim: sw=8 sts=8 ts=8 ft=cpp
