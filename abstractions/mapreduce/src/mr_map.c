/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "mapreduce.h"

#include "debug.h"
#include "hash_table.h"
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

struct mr_map_config {
	const char     *scratch_dir;
	int		mid;
	int		nmappers;
	int		nreducers;
	struct list    *inputlist;
	struct list   **outputlists;
};

static int free_string_item( void *item, const void *arg ) {
	free(item);
	return 1;
}

static int print_string_item( void *item, const void *arg ) {
	fprintf((FILE *)arg, "%s\n", (char *)(item));
	return 1;
}

static int partition( struct mr_map_config *cfg ) {
	FILE *fp;
	char buffer[MR_MAX_STRLEN];
	int i, result = -1;
	
	cfg->inputlist = list_create();
	if (!cfg->inputlist) {
		debug(D_NOTICE, "\tcould not create inputlist");
		goto p_return;
	}
	
	fp = fopen(MR_INPUTLIST, "r");
	if (!fp) {
		debug(D_NOTICE, "\tcould not open inputlist: %s", MR_INPUTLIST);
		goto p_return;
	}

	i = 0;
	while ((fgets((char *)&buffer, MR_MAX_STRLEN, fp)) != NULL) {
		string_chomp(buffer);
		if (i == cfg->mid && !list_push_tail(cfg->inputlist, xstrdup(buffer))) {
			debug(D_NOTICE, "\tcould not append %s to inputlist", buffer);
			goto p_return;
		}
		i = (i + 1) % cfg->nmappers;
	}
	fclose(fp);

	snprintf(buffer, MR_MAX_STRLEN, "%s/map.input.%d", cfg->scratch_dir, cfg->mid);
	fp = fopen(buffer, "w");
	if (!fp) { 
		debug(D_NOTICE, "\tcould not open %s for writing", buffer);
		goto p_return;
	}
	if (!list_iterate(cfg->inputlist, print_string_item, fp)) {
		debug(D_NOTICE, "\tfailed to write inputlist");
		goto p_return;
	}
	fclose(fp);

	fp = 0;
	result = 0;

p_return:
	if (fp) fclose(fp);
	return result;
}

static int map_one( void *item, const void *arg ) {
	char *inputfile = (char *)item;
	struct mr_map_config *cfg = (struct mr_map_config *)arg;

	FILE *fp = NULL;
	char buffer[MR_MAX_STRLEN];
	int pos, rid, result = 0;
		
	snprintf(buffer, MR_MAX_STRLEN, "%s < %s", MR_MAPPER, inputfile);
	fp = popen(buffer, "r");
	if (!fp) {
		debug(D_NOTICE, "\tfailed to popen %s", buffer);
		goto mo_return;
	}

	while (fgets(buffer, MR_MAX_STRLEN, fp) != NULL) {
		string_chomp(buffer);

		pos = strpos(buffer, '\t');
		if (pos < 0) { 
			debug(D_NOTICE, "\tinvalid map output: %s", buffer);
			goto mo_return;
		}

		rid = hash_string(string_front(buffer, pos)) % cfg->nreducers;
		if (!list_push_tail(cfg->outputlists[rid], xstrdup(buffer))) {
			debug(D_NOTICE, "\tcould not append %s to outputlist %d", buffer, rid);
			goto mo_return;
		}
	}

mo_return:
	if (fp && pclose(fp) == 0) {
		result = 1;
	}
	return result;
}

static int map( struct mr_map_config *cfg ) {
	if (!list_iterate(cfg->inputlist, map_one, cfg)) {
		return -1;
	} else {
		return 0;
	}
}

static int mr_map( struct mr_map_config *cfg ) {
	FILE *fp = NULL;
	char outputfile[MR_MAX_STRLEN];
	int i, result = EXIT_FAILURE;

	debug(D_NOTICE, "0. Partition");
	if (partition(cfg) < 0) {
		goto mrm_return;
	}

	debug(D_NOTICE, "1. Allocating outputlists");
	cfg->outputlists = xxmalloc(sizeof(struct list*)*cfg->nreducers);
	for (i = 0; i < cfg->nreducers; i++) {
		cfg->outputlists[i] = list_create();
	}

	debug(D_NOTICE, "2. Map");
	if (map(cfg) < 0) {
		goto mrm_return;
	}
	
	debug(D_NOTICE, "3. Outputting outputlists");
	for (i = 0; i < cfg->nreducers; i++) {
		snprintf(outputfile, MR_MAX_STRLEN, "%s/map.output.%d.%d", cfg->scratch_dir, cfg->mid, i);

		fp = fopen(outputfile, "w");
		if (!fp) goto mrm_return;
		mergesort_list(cfg->outputlists[i], (cmp_op_t)strcmp);
		if (!list_iterate(cfg->outputlists[i], print_string_item, fp)) {
			debug(D_NOTICE, "failed to write outputlist %d to %s", i, outputfile);
			goto mrm_return;
		}
		fclose(fp);
	}

	fp = 0;
	result = EXIT_SUCCESS;

mrm_return:
	debug(D_NOTICE, "X. Cleanup");
	if (cfg->inputlist) {
		list_iterate(cfg->inputlist, free_string_item, NULL);
		list_delete(cfg->inputlist);
	}
	if (cfg->outputlists) {
		for (i = 0; i < cfg->nreducers; i++) {
			list_iterate(cfg->outputlists[i], free_string_item, NULL);
			list_delete(cfg->outputlists[i]);
		}
		free(cfg->outputlists);
	}
	if (fp) fclose(fp);
	return result;
}

static void show_help( const char *cmd ) {
	printf("Use: %s [options] <scratch_dir> <mid> <nmappers> <nreducers>\n", cmd);
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
	const char *progname = "mr_map";
	struct mr_map_config cfg;
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

	cfg.scratch_dir = argv[optind];
	cfg.mid		= atoi(argv[optind + 1]);
	cfg.nmappers	= atoi(argv[optind + 2]);
	cfg.nreducers	= atoi(argv[optind + 3]);
	cfg.inputlist	= NULL;
	cfg.outputlists = NULL;

	return mr_map(&cfg);
}

// vim: sw=8 sts=8 ts=8 ft=cpp
