#ifndef VINE_WORKSPACE_H
#define VINE_WORKSPACE_H

#include "vine_cache.h"

/** @file vine_workspace.h

A workspace object describes the directories managed by a worker:
- workspace_dir - The top level path managed by the worker.
- cache_dir - Contains only files/directories that are sent by the
manager, or downloaded at the manager's direction.  These are managed
by the vine_cache object.
- temp_dir - a temporary directory of last resort if a tool needs some
space to work on items that neither belong in the cache or in a task sandbox.
Really anything using this directory is a hack and its behavior should be reconsidered.
- trash_dir - deleted files are moved here, and then unlinked via @ref trash_file.
This is done because (a) it may not be possible to unlink a
file outright if it is still in use as an executable,
and (b) the move of an entire directory can be done quickly and
atomically.  An attempt is made to deleted everything in this
directory on startup, shutdown, and whenever an individual file is trashed.
- task.%d - each executing task gets its own sandbox directory in the workspace as it runs
*/

/** Workspace object describing files used by the worker. */

struct vine_workspace {
	char *workspace_dir;
	char *cache_dir;
	char *trash_dir;
	char *temp_dir;
};

/** Create a new workspace directory.
@param manual_tempdir If not null, create the workspace inside this path.
Otherwise consult the system configuration for the location of the standard temp directory.
@return A new workspace object.
*/

struct vine_workspace * vine_workspace_create( const char *manual_tmpdir );

/** Check that the workspace can actually write and execute files.
(Some HPCs disallow execution out of temporary directories)
@param w A workspace object.
@return True if files can be written and executed, false otherwise.
*/

int vine_workspace_check( struct vine_workspace *w );

/** Prepare the workspace directories prior to working with a manager.
@param w A workspace object.
@return True if workspace prepared, false otherwise.
*/

int vine_workspace_prepare( struct vine_workspace *w );

/** Cleanup a workspace after working with a specific manager.
@param w A workspace object.
@return True if workspace cleand, false otherwise.
*/

int vine_workspace_cleanup( struct vine_workspace *w );

/** Delete a workspace object and underlying directory.
@param w A workspace object.
*/

void vine_workspace_delete( struct vine_workspace *w );

#endif
