/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "parrot_client.h"
#include "debug.h"
#include "cctools.h"

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

void show_help()
{
	printf("Use: parrot_unmount [OPTIONS] PATH\n");
	printf("Where options are:\n");
	printf("-d --debug <flags>  Enable debugging for this subsystem.\n");
	printf("-v --version        Show version number.\n");
	printf("-h --help           Help: Show these options.\n");
}

int main( int argc, char *argv[] )
{
	char c;

	static const struct option long_options[] = {
		{"help",  no_argument, 0, 'h'},
		{"version", no_argument, 0, 'v'},
		{"debug", required_argument, 0, 'd'},
		{0,0,0,0}
	};

	while((c=getopt_long(argc,argv,"d:vh", long_options, NULL)) > -1) {
		switch(c) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'h':
			show_help();
			return 0;
			break;
		case 'v':
			cctools_version_print(stdout,"parrot_unmount");
			return 0;
		default:
			show_help();
			return 1;
			break;
		}
	}

	if( (argc-optind)!=1 ) {
		show_help();
		return 1;
	}

	const char *path = argv[optind];

	int result = parrot_unmount(path);

	if(result<0) {
		fprintf(stderr,"parrot_unmount: couldn't unmount %s: %s\n",path,strerror(errno));
		return 1;
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
