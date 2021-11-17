
#include "random.h"
#include "create_dir.h"
#include "unlink_recursive.h"
#include "stringtools.h"
#include "debug.h"

#include <sys/stat.h>
#include <string.h>
#include <errno.h>

static char *trashdir = 0;

void trash_setup( const char *dir )
{
    if(trashdir) {
        debug(D_NOTICE, "Trash directory already setup to %s. Ignoring setup for %s.", trashdir, dir);
    }

	trashdir = strdup(dir);
	create_dir(trashdir,0700);
	random_init();
}

/*
A direct unlink of a file may fail, in particular if the file
is being executed by a process.  To avoid the problem of unlinkable
files, we instead move a file to a random name within the trash directory,
and then attempt to delete it there.  If that fails, it's ok b/c we will
get it later on the next attempt to empty the trash.
*/

#define TRASH_COOKIE_LENGTH 8

void trash_file( const char *filename )
{
	struct stat info;

	/* If the original file doesn't exist, then return success. */
	int result = stat(filename,&info);
	if(result<0 && errno==ENOENT) return;

	/* Generate a unique random name */
	char cookie[TRASH_COOKIE_LENGTH];
	string_cookie(cookie,TRASH_COOKIE_LENGTH);

	char *trashname = string_format("%s/%s",trashdir,cookie);
	debug(D_WQ,"trashing file %s to %s",filename,trashname);

	/* Move the file to the trash directory. */
	result = rename(filename,trashname);
	if(result!=0) {
		fatal("failed to move file (%s) to trash location (%s): %s",filename,trashname,strerror(errno));
	}

	free(trashname);
}

void trash_empty()
{
	int result = unlink_dir_contents(trashdir);
	if(result!=0) {
		debug(D_DEBUG,"warning: unable to delete all items in trash directory (%s), will try again later.",trashdir);
	}

	// recreate trash directory
	create_dir(trashdir,0700);
}

/* vim: set noexpandtab tabstop=4: */
