/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "makeflow_gc.h"
#include "makeflow_log.h"

#include "debug.h"
#include "set.h"
#include "timestamp.h"
#include "disk_info.h"
#include "stringtools.h"

#include <dirent.h>
#include <sys/types.h>
#include <limits.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

/*
XXX This module is implemented using Unix operations, and so
only make sense for batch modes that rely on the local disk.
An improved implementation should pass all filesystem operations
through the batch_job interface, which must be expanded to
include measuring available space and inodes consumed.
*/

/* XXX this should be configurable. */
#define	MAKEFLOW_MIN_SPACE 10*1024*1024	/* 10 MB */

static int makeflow_gc_collected = 0;

/* Count the number of items in a directory.  (expensive!) */

static int directory_inode_count(const char *dirname)
{
	DIR *dir;
	struct dirent *d;
	int inode_count = 0;

	dir = opendir(dirname);
	if(dir == NULL)
		return INT_MAX;

	while((d = readdir(dir)))
		inode_count++;
	closedir(dir);

	return inode_count;
}

/*
Return true if disk space falls below the fixed minimum. (inexpensive!)
XXX this value should be configurable.
*/

static int directory_low_disk( const char *path )
{
	UINT64_T avail, total;

	if(disk_info_get(path, &avail, &total) >= 0)
		return avail <= MAKEFLOW_MIN_SPACE;

	return 0;
}

/* Prepare the dag for garbage collection by identifying which files may or may not be gcd. */

void makeflow_gc_prepare( struct dag *d )
{
	/* Files to be collected:
	 * ((all_files \minus sink_files)) \union collect_list) \minus preserve_list) \minus source_files
	 */

	/* Parse GC_*_LIST and record which target files should be
	 * garbage collected. */
	char *collect_list  = dag_variable_lookup_global_string("GC_COLLECT_LIST", d);
	char *preserve_list = dag_variable_lookup_global_string("GC_PRESERVE_LIST", d);

	struct dag_file *f;
	char *filename;

	/* add all files, but sink_files */
	hash_table_firstkey(d->files);
	while((hash_table_nextkey(d->files, &filename, (void **) &f)))
		if(!dag_file_is_sink(f)) {
			set_insert(d->collect_table, f);
		}

	int i, argc;
	char **argv;

	/* add collect_list, for sink_files that should be removed */
	string_split_quotes(collect_list, &argc, &argv);
	for(i = 0; i < argc; i++) {
		f = dag_file_lookup_or_create(d, argv[i]);
		set_insert(d->collect_table, f);
		debug(D_MAKEFLOW_RUN, "Added %s to garbage collection list", f->filename);
	}
	free(argv);

	/* remove files from preserve_list */
	string_split_quotes(preserve_list, &argc, &argv);
	for(i = 0; i < argc; i++) {
		/* Must initialize to non-zero for hash_table functions to work properly. */
		f = dag_file_lookup_or_create(d, argv[i]);
		set_remove(d->collect_table, f);
		debug(D_MAKEFLOW_RUN, "Removed %s from garbage collection list", f->filename);
	}
	free(argv);

	/* remove source_files from collect_table */
	hash_table_firstkey(d->files);
	while((hash_table_nextkey(d->files, &filename, (void **) &f)))
		if(dag_file_is_source(f)) {
			set_remove(d->collect_table, f);
			debug(D_MAKEFLOW_RUN, "Removed %s from garbage collection list", f->filename);
		}

	/* Print reference counts of files to be collected */
	set_first_element(d->collect_table);
	while((f = set_next_element(d->collect_table)))
		debug(D_MAKEFLOW_RUN, "Added %s to garbage collection list (%d)", f->filename, f->ref_count);
}

/* Clean up one file and mark it as such in the dag. */

static int makeflow_gc_file( struct dag *d, const struct dag_file *f )
{
	struct stat buf;
	if(stat(f->filename, &buf) == 0 && unlink(f->filename)<0) {
		debug(D_NOTICE, "makeflow: unable to collect %s: %s", f->filename, strerror(errno));
		return 0;
	} else {
		debug(D_MAKEFLOW_RUN, "Garbage collected %s\n", f->filename);
		set_remove(d->collect_table, f);
		return 1;
	}
}

/* Collect available garbage, up to a limit of maxfiles. */

static void makeflow_gc_all( struct dag *d, int maxfiles )
{
	int collected = 0;
	struct dag_file *f;
	timestamp_t start_time, stop_time;

	/* This will walk the table of files to collect and will remove any
	 * that are below or equal to the threshold. */
	start_time = timestamp_get();
	set_first_element(d->collect_table);
	while((f = set_next_element(d->collect_table)) && collected < maxfiles) {
		if(f->ref_count < 1 && makeflow_gc_file(d, f))
			collected++;
	}

	stop_time = timestamp_get();

	/* Record total amount of files collected to Makeflowlog. */
	if(collected > 0) {
		makeflow_gc_collected += collected;
		makeflow_log_gc_event(d,collected,stop_time-start_time,makeflow_gc_collected);
	}
}

/* Collect garbage only if conditions warrant. */

void makeflow_gc( struct dag *d, makeflow_gc_method_t method, int count )
{
	switch (method) {
	case MAKEFLOW_GC_NONE:
		break;
	case MAKEFLOW_GC_REF_COUNT:
		debug(D_MAKEFLOW_RUN, "Performing incremental file (%d) garbage collection", count);
		makeflow_gc_all(d, count);
		break;
	case MAKEFLOW_GC_ON_DEMAND:
		if(directory_inode_count(".") >= count || directory_low_disk(".")) {
			debug(D_MAKEFLOW_RUN, "Performing on demand (%d) garbage collection", count);
			makeflow_gc_all(d, INT_MAX);
		}
		break;
	case MAKEFLOW_GC_FORCE:
		makeflow_gc_all(d,INT_MAX);
		break;
	}
}
