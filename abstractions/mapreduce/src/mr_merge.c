/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "mapreduce.h"

#include "debug.h"
#include "list.h"
#include "mergesort.h"
#include "stringtools.h"
#include "xmalloc.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

struct mr_merge_config {
	const char     *scratch_dir;
	int		wid;	
	int		nmappers;
	int		nreducers;
};

static int merge_streams( FILE *lfp, FILE *rfp, FILE *ofp ) {
	char lbuffer[MR_MAX_STRLEN];
	char rbuffer[MR_MAX_STRLEN];
	int lready = 0, rready = 0;

	do {
		if (!lready) lready = (int)fgets(lbuffer, MR_MAX_STRLEN, lfp);
		if (!rready) rready = (int)fgets(rbuffer, MR_MAX_STRLEN, rfp);

		if (lready && rready) {
			if (strncmp(lbuffer, rbuffer, MR_MAX_STRLEN) <= 0) {
				fputs(lbuffer, ofp);
				lready = 0;
			} else {
				fputs(rbuffer, ofp);
				rready = 0;
			}
		} else if (lready && !rready) {
			fputs(lbuffer, ofp);
			while ((lready = (int)fgets(lbuffer, MR_MAX_STRLEN, lfp)))
				fputs(lbuffer, ofp);
		} else if (!lready && rready) {
			fputs(rbuffer, ofp);
			while ((rready = (int)fgets(rbuffer, MR_MAX_STRLEN, rfp)))
				fputs(rbuffer, ofp);
		}
	} while (lready || rready);

	return 0;
}

static int merge_files( const char *lfile, const char *rfile, const char *ofile ) {
	FILE *lfp = NULL, *rfp = NULL, *ofp = NULL;
	int result = -1;

	lfp = fopen(lfile, "r");
	if (!lfp) goto mf_return;
	
	rfp = fopen(rfile, "r");
	if (!rfp) goto mf_return;
	
	ofp = fopen(ofile, "w+");
	if (!ofp) goto mf_return;

	merge_streams(lfp, rfp, ofp);

	result = 0;

mf_return:
	if (lfp) fclose(lfp);
	if (rfp) fclose(rfp);
	if (ofp) fclose(ofp);
	return result;
}

static int merge( const char *inputformat, const char *outputfile, const int njobs ) {
	char lfile[MR_MAX_STRLEN];
	char rfile[MR_MAX_STRLEN];
	char ofile[MR_MAX_STRLEN];
	int n, first_pass = 1;

	for (n = 1; n < njobs; n *= 2) {
		int i;
		for (i = 0; i < njobs; i += 2*n) {
			if (first_pass) {
				snprintf(lfile, MR_MAX_STRLEN, inputformat, i);
				snprintf(rfile, MR_MAX_STRLEN, inputformat, i + n);
			} else {
				snprintf(lfile, MR_MAX_STRLEN, "%s.%d", outputfile, i);
				snprintf(rfile, MR_MAX_STRLEN, "%s.%d", outputfile, i + n);
			}
			snprintf(ofile, MR_MAX_STRLEN, "%s.%d.merged", outputfile, i);

			if (merge_files(lfile, rfile, ofile) < 0) {
				debug(D_NOTICE, "could not merge %s and %s into %s: %s", lfile, rfile, ofile, strerror(errno));
				return EXIT_FAILURE;
			}
			if (first_pass) snprintf(lfile, MR_MAX_STRLEN, "%s.%d", outputfile, i);
			if (rename(ofile, lfile) < 0) { 
				debug(D_NOTICE, "could not move %s to %s: %s", ofile, lfile, strerror(errno));
				return EXIT_FAILURE;
			}
		}
		first_pass = 0;
	}

	if (rename(lfile, outputfile) < 0) { 
		debug(D_NOTICE, "could not move %s to %s: %s", lfile, outputfile, strerror(errno));
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}

static int mr_merge( struct mr_merge_config *cfg ) {
	char inputformat[MR_MAX_STRLEN];
	char outputfile[MR_MAX_STRLEN];
	int njobs;

	if (cfg->wid >= 0) {
		snprintf(inputformat, MR_MAX_STRLEN, "%s/map.output.%%d.%d", cfg->scratch_dir, cfg->wid);
		snprintf(outputfile, MR_MAX_STRLEN, "%s/reduce.input.%d", cfg->scratch_dir, cfg->wid);
		njobs = cfg->nmappers;
	} else {
		snprintf(inputformat, MR_MAX_STRLEN, "%s/reduce.output.%%d", cfg->scratch_dir);
		snprintf(outputfile, MR_MAX_STRLEN, "%s/merge.output", cfg->scratch_dir);
		njobs = cfg->nreducers;
	}

	return merge(inputformat, outputfile, njobs);
}

static void show_help( const char *cmd ) {
	printf("Use: %s [options] <scratchdir> <wid> <nmappers> <nreducers>\n", cmd);
	printf("where general options are:\n");
	printf(" -d <subsystem> Enable debugging for this subsystem.  (Try -d all to start.)\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

static void show_version( const char *cmd ) {       
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

int main( int argc, char *argv[] ) {
	const char *progname = "mr_merge";
	struct mr_merge_config cfg;
	char c;

	debug_config(progname);

	while ((c = getopt(argc, argv, "d:o:j:hv")) != (char)-1) {
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

	cfg.scratch_dir = argv[optind];
	cfg.wid		= (argv[optind + 1][0] == 'm') ? -1 : atoi(argv[optind + 1]);
	cfg.nmappers	= atoi(argv[optind + 2]);
	cfg.nreducers	= atoi(argv[optind + 3]);

	return mr_merge(&cfg); 
}

// vim: sw=8 sts=8 ts=8 ft=cpp
