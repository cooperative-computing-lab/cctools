/*
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef COPY_TREE_H
#define COPY_TREE_H

/* copy_dir copies the source dir into target. Like 'cp -r source target'.
 * @param source: the source directory, which must be an existing directory.
 * @param target: the target directory
 * @return zero on success, non-zero on failure.
 * If target does not exist, create the dir target and copy all the entries under the source dir into the target dir;
 * If target exists, create a sub-dir basename(source) under target, and copy all the entries under the source dir into target/basename(source).
 */
int copy_dir(const char *source, const char *target);

/* copy_symlink copies the symlink source to target.
 * @param source: the source, which must be an existing symlink.
 * @param target: the target, which must be non-exist.
 * @return zero on success, non-zero on failure.
 */
int copy_symlink(const char *source, const char *target);

/* copy_direntry copies the source dir or file into target. Like 'cp -r source target'.
 * @param source: the source directory/file, which must be an existing directory/file.
 * @param target: the target directory/file
 * @return zero on success, non-zero on failure.
 * If target does not exist, create the dir target and copy all the entries under the source dir into the target dir;
 * If target exists, create a sub-dir basename(source) under target, and copy all the entries under the source dir into target/basename(source).
 */
int copy_direntry(const char *source, const char *target);

/* Only copy regular files, directories, and symlinks. */
typedef enum {
	FILE_TYPE_REG,
	FILE_TYPE_LNK,
	FILE_TYPE_DIR,
	FILE_TYPE_UNSUPPORTED
} file_type;

/* check_file_type checks the file types and whether the copying of the file type is supported.
 * @param source: a file path.
 * @return a file_type value denoting the file type of source.
 */
file_type check_file_type(const char *source);

/* get_exist_ancestor_dir gets the closest existing ancestor dir.
 * @param s: s will be modified during the exeution of the function, and can not be in text segement.
 * If s = "a/b/c/d", and only d does not exist, returns "a/b/c".
 * If s is an absolute path, in the worst case, the return string should be "/".
 * If s is a relative path, and no any part of s exists, return an empty string.
 * The caller should free the result string.
 */
char *get_exist_ancestor_dir(const char *s);

/* is_subdir checks whether target is a (sub)directory of source.
 * is_subdir finds the closest existing ancestor directory of target, and check whether it is a (sub)directory of source.
 * source must exist, target must not exist.
 * return -1 if source can not be copied, return 0 if source can be copied.
 */
int is_subdir(const char *source, const char *target);

#endif

/* vim: set noexpandtab tabstop=8: */
