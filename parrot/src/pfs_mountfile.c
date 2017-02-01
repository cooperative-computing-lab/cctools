/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "pfs_mountfile.h"

#include "pfs_resolve.h"
#include "xxmalloc.h"
#include "debug.h"
#include "stringtools.h"

#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

int pfs_mountfile_parse_mode( const char * options ) {
	assert(options);
	unsigned int i;
	int mode = 0;
	for(i = 0; i < strlen(options); i++) {
		if(options[i] == 'r' || options[i] == 'R') {
			mode |= R_OK;
		} else if(options[i] == 'w' || options[i] == 'W') {
			mode |= W_OK;
		} else if (options[i] == 'x' || options[i] == 'X') {
			mode |= X_OK;
		} else {
			return -1;
		}
	}
	return mode;
}

void pfs_mountfile_parse_string(const char *str) {
	assert(str);
	char *e;
	str = xxstrdup(str);
	e = strchr(str, '=');
	if(!e) fatal("badly formed mount string: %s", str);
	*e = 0;
	e++;
	if (pfs_resolve_mount(str, e, "rwx") < 0) fatal("call to parrot_mount failed: %s", strerror(errno));
	free((char *) str);
}

void pfs_mountfile_parse_file(const char *mountfile) {
	FILE *file;
	char line[PFS_LINE_MAX];
	char prefix[PFS_LINE_MAX];
	char redirect[PFS_LINE_MAX];
	char options[PFS_LINE_MAX];
	int fields;
	int linenum=0;
	int mode;

	assert(mountfile);

	file = fopen(mountfile,"r");
	if(!file) fatal("couldn't open mountfile %s: %s\n",mountfile,strerror(errno));

	while(1) {
		if(!fgets(line,sizeof(line),file)) {
			if(errno==EINTR) {
				continue;
			} else {
				break;
			}
		}
		linenum++;
		if(line[0]=='#') continue;
		string_chomp(line);
		if(!line[0]) continue;
		fields = sscanf(line,"%s %s %s",prefix,redirect,options);

		if(fields==0) {
			continue;
		} else if(fields<2) {
			fatal("%s has an error on line %d\n",mountfile,linenum);
		} else if(fields==2) {
			mode = pfs_mountfile_parse_mode(redirect);
			if(mode < 0) {
				if (pfs_resolve_mount(prefix, redirect, "rwx") < 0) fatal("call to parrot_mount failed: %s", strerror(errno));
			} else {
				if (pfs_resolve_mount(prefix, prefix, redirect) < 0) fatal("call to parrot_mount failed: %s", strerror(errno));
			}
		} else {
			mode = pfs_mountfile_parse_mode(options);
			if(mode < 0) {
				fatal("%s has invalid options on line %d\n",mountfile,linenum);
			}
			if (pfs_resolve_mount(prefix, redirect, options) < 0) fatal("call to parrot_mount failed: %s", strerror(errno));
		}
	}

	fclose(file);
}

/* vim: set noexpandtab tabstop=4: */
