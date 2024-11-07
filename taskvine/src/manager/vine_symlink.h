/*
Copyright (C) 2024- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_SYMLINK_H
#define VINE_SYMLINK_H

/*
A vine symlink is a lightweight structure representing a symlink
to be added to the sandbox namespace of a task as it runs.
Note that a symlink is not treated as either an input file
or an output file in the vine infrastructure, because there are
no file contents to cache or move.  It is simply an addition
to the task namespace performed just prior to execution.
*/

struct vine_symlink {
	char *name;
	char *target;
};

struct vine_symlink * vine_symlink_create( const char *name, const char *target );
struct vine_symlink * vine_symlink_copy( struct vine_symlink *s );
void vine_symlink_delete( struct vine_symlink *s );

#endif

