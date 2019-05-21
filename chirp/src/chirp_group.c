/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_group.h"
#include "chirp_types.h"

#include "debug.h"
#include "stringtools.h"

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>


extern char chirp_transient_path[PATH_MAX];

char chirp_group_base_url[PATH_MAX];
int  chirp_group_cache_time = 900;

/*
Search for a given subject name in a group.
Return true if the member is found, false otherwise.
Works by downloading group files from a web server,
which are then cached for a configurable time, 15 minutes by default.
*/

int chirp_group_lookup(const char *group, const char *subject)
{
	char url[CHIRP_PATH_MAX];
	char cachedir[CHIRP_PATH_MAX];
	char cachepath[CHIRP_PATH_MAX];
	char line[CHIRP_PATH_MAX];
	struct stat info;

	if(chirp_group_base_url[0] == '\0')
		return 0;

	int fetch_group = 1;

	string_nformat(cachedir,  sizeof(cachedir),  "%s/.__groups", chirp_transient_path);
	string_nformat(cachepath, sizeof(cachepath), "%s/%s", cachedir, &group[6]);

	if(stat(cachepath, &info) == 0) {
		int age = time(0) - info.st_mtime;
		if(age < chirp_group_cache_time) {
			fetch_group = 0;
		}
	}

	if(fetch_group) {
		string_nformat(url, sizeof(url), "%s/%s", chirp_group_base_url, &group[6]);
		debug(D_DEBUG, "fetching group %s from %s", group, url);
		mkdir(cachedir, 0777);
		string_nformat(line, sizeof(line), "curl --silent --insecure --output '%s' %s", cachepath, url);
		if(system(line) != 0) {
			debug(D_NOTICE, "failed to fetch group using: %s", line);
			unlink(cachepath);
			return 0;
		}
	}

	FILE *file = fopen(cachepath, "r");
	if(!file)
		return 0;

	while(fgets(line, sizeof(line), file)) {
		string_chomp(line);

		// If it matches exactly, return.
		if(!strcmp(line, subject)) {
			fclose(file);
			return 1;
		}
		// If the group entry does not have an auth method,
		// and the subject is unix, look for equivalence after the colon.

		if(!strchr(line, ':') && !strncmp(subject, "unix:", 5)) {
			if(!strcmp(line, &subject[5])) {
				fclose(file);
				return 1;
			}
		}

	}

	fclose(file);
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
