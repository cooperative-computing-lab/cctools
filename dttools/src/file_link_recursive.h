#ifndef FILE_LINK_RECURSIVE_H
#define FILE_LINK_RECURSIVE_H

/** @file file_link_recursive.h Make deep links to directories. */

/** Make a hard link from source to target.
If source is a directory, do it recursively.
@param source The source path to link to.
@param target The target path to create.
@param allow_symlink If true, attempt symbolic links if hard links fail.
@return 1 on success, 0 on failure.
*/

int file_link_recursive( const char *source, const char *target, int allow_symlink, int symlink_dirs );

#endif
