/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "delete_dir.h"
#include "stringtools.h"

#include <dirent.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>

int delete_dir(const char *dirname)
{
	char subdir[PATH_MAX];
	int result = 0;
	struct dirent *d;
	DIR *dir;

	dir = opendir(dirname);
	if(!dir) {
		if(errno == ENOTDIR) 
			return unlink(dirname);
		else if(errno == ENOENT) {
			/* 
			 ENOENT also signals a dangling symlink. So call unlink anyway to
			 remove any dangling symlinks. If not a dangling symlink, unlink
			 will fail and keep errno set to ENOENT. We always return success. 
			*/
			unlink(dirname); 			
			return 0;
		}	
		else 
			return -1;
	}

	while((d = readdir(dir))) {
		if(!strcmp(d->d_name, "."))
			continue;
		if(!strcmp(d->d_name, ".."))
			continue;
		sprintf(subdir, "%s/%s", dirname, d->d_name);
		result = delete_dir(subdir);
	}

	closedir(dir);

	if(rmdir(dirname) != 0) {
		result = -1;
	}

	return result;
}

int delete_dir_contents(const char *dirname) {
	char subdir[PATH_MAX];
	struct dirent *d;
	DIR *dir;

	int result = 0;

	dir = opendir(dirname);
	if (!dir) {
		return -1;	
	}
		
	while ((d = readdir(dir))) {
		if ((strcmp(d->d_name, ".") && strcmp(d->d_name, "..")) != 0) {
			sprintf(subdir, "%s/%s", dirname, d->d_name);
			if (delete_dir(subdir) != 0) {
				result = -1;
			}	
		}	
	}
		
	closedir(dir);
	return result;
}

/* vim: set noexpandtab tabstop=4: */
