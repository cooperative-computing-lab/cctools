/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_filesystem.h"
#include "chirp_audit.h"
#include "chirp_protocol.h"

#include "stringtools.h"
#include "debug.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>

static int audit_count = 0;

static int get_directory_owner(const char *path, char *owner)
{
	char aclpath[CHIRP_PATH_MAX];
	char tmp[CHIRP_LINE_MAX];
	char *r;
	CHIRP_FILE *file;
	int result;

	sprintf(aclpath, "%s/.__acl", path);

	file = cfs_fopen(aclpath, "r");
	if(!file)
		return -1;

	r = cfs_fgets(tmp, sizeof(tmp), file);
	if(!r)
		return -1;
	result = sscanf(tmp, "%[^ \t\n]", owner);

	cfs_fclose(file);

	if(result == 1) {
		return 0;
	} else {
		return -1;
	}
}

static int chirp_audit_recursive(const char *path, struct hash_table *table)
{
	struct chirp_dir *dir;
	struct chirp_dirent *d;
	char subpath[CHIRP_PATH_MAX];
	char owner[CHIRP_PATH_MAX];
	struct chirp_audit *entry;
	int result;

	result = get_directory_owner(path, owner);
	if(result < 0)
		strcpy(owner, "unknown");

	entry = hash_table_lookup(table, owner);
	if(!entry) {
		entry = malloc(sizeof(*entry));
		memset(entry, 0, sizeof(*entry));
		strcpy(entry->name, owner);
		hash_table_insert(table, owner, entry);
	}

	entry->ndirs++;

	dir = cfs->opendir(path);
	if(!dir) {
		debug(D_LOCAL, "audit: couldn't enter %s: %s", path, strerror(errno));
		return -1;
	}

	while((d = cfs->readdir(dir))) {
		if(!strcmp(d->name, "."))
			continue;
		if(!strcmp(d->name, ".."))
			continue;
		if(!strncmp(d->name, ".__", 3))
			continue;
		if(!strncmp(d->name, ".__", 3))
			continue;

		audit_count++;
		if((audit_count % 10000) == 0)
			debug(D_LOCAL, "audit: %d items", audit_count);

		sprintf(subpath, "%s/%s", path, d->name);

		if(S_ISDIR(d->info.cst_mode)) {
			chirp_audit_recursive(subpath, table);
		} else {
			entry->nfiles++;
			entry->nbytes += d->info.cst_size;
		}
	}

	cfs->closedir(dir);

	return 0;
}

struct hash_table *chirp_audit(const char *path)
{
	struct hash_table *table;
	time_t stop, start;
	int result;

	/*
	   An audit can be time consuming and resource intensive,
	   so run the audit at a low priority so as not to hurt
	   anyone else.
	 */

	nice(10);

	table = hash_table_create(0, 0);
	if(!table)
		return 0;

	audit_count = 0;

	start = time(0);
	debug(D_LOCAL, "audit: starting");
	result = chirp_audit_recursive(path, table);
	stop = time(0);
	if(stop == start)
		stop++;
	debug(D_LOCAL, "audit: completed %d items in %d seconds (%d items/sec)", audit_count, (int) (stop - start), audit_count / (int) (stop - start));

	if(result < 0) {
		chirp_audit_delete(table);
		return 0;
	} else {
		return table;
	}
}

void chirp_audit_delete(struct hash_table *table)
{
	char *key;
	struct chirp_audit *entry;

	hash_table_firstkey(table);
	while(hash_table_nextkey(table, &key, (void *) &entry)) {
		free(hash_table_remove(table, key));
	}

	hash_table_delete(table);
}

/* vim: set noexpandtab tabstop=8: */
