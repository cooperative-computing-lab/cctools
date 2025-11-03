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

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static void show_help()
{
	printf("Use: parrot_mount [OPTIONS] PATH DEST RWX\n");
	printf("     parrot_mount [OPTIONS] --unmount PATH\n");
	printf("     parrot_mount [OPTIONS] --disable\n");
	printf("\n");
	printf("Where options are:\n");
	printf("   --unmount        Unmount a previously mounted path.\n");
	printf("   --disable        Disable any further mounting/unmounting in this parrot session.\n");
	printf("-d --debug <flags>  Enable debugging for this subsystem.\n");
	printf("-v --version        Show version number.\n");
	printf("-h --help           Help: Show these options.\n");
}

typedef enum {
	MODE_MOUNT,
	MODE_UNMOUNT,
	MODE_DISABLE
} mount_mode_t;

typedef enum {
	LONG_OPT_UNMOUNT=256,
	LONG_OPT_DISABLE,
} long_options_t;

int main( int argc, char *argv[] )
{
	int c;
	mount_mode_t mode = MODE_MOUNT;
	int expected_args = 3;

	static const struct option long_options[] = {
		{"help",  no_argument, 0, 'h'},
		{"version", no_argument, 0, 'v'},
		{"debug", required_argument, 0, 'd'},
		{"disable", no_argument, 0, LONG_OPT_DISABLE},
		{"unmount", no_argument, 0, LONG_OPT_UNMOUNT},
		{0,0,0,0}
	};
	
	while((c=getopt_long(argc,argv,"d:vh", long_options, NULL)) > -1) {
		switch(c) {
		case LONG_OPT_DISABLE:
			expected_args = 0;
			mode = MODE_DISABLE;
			break;
		case LONG_OPT_UNMOUNT:
			expected_args = 1;
			mode = MODE_UNMOUNT;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'h':
			show_help();
			return 0;
			break;
		case 'v':
			cctools_version_print(stdout,"parrot_mount");
			return 0;
		default:
			show_help();
			return 1;
			break;
		}
	}

	if( (argc-optind)!=expected_args ) {
		show_help();
		return 1;
	}

	if(mode==MODE_DISABLE) {
		int result = parrot_mount(0,0,0);
		if(result==0) {
			return 0;
		} else {
			fprintf(stderr,"parrot_mount: couldn't disable mounting: %s\n",strerror(errno));
			return 1;
		}
	}

	const char *relpath = argv[optind];
	const char *path;

	if(relpath[0]!='/') {
		const char * cwd = path_getcwd();
		path = string_format("%s/%s",cwd,relpath);
	} else {
		path = relpath;
	}

	if(mode==MODE_MOUNT) {
		const char *destination = argv[optind+1];
		const char *perms = argv[optind+2];
		int result = parrot_mount(path,destination,perms);
		if(result<0) {
			fprintf(stderr,"parrot_mount: couldn't mount %s as %s: %s\n",path,destination,strerror(errno));
			return 1;
		}
	} else {
		int result = parrot_unmount(path);
		if(result<0) {
			fprintf(stderr,"parrot_unmount: couldn't unmount %s: %s\n",path,strerror(errno));
			return 1;
		}
	}

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
