/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef GROW_H
#define GROW_H

#define GROW_LINE_MAX 4096
#define GROW_EPOCH 1199163600

#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "sha1.h"

/**
 * A grow_dirent is a node in a tree representing the
 * entire directory structure of a grow_filesystem.
 * Each node describes its name, metadata, checksum,
 * and children (if a directory)
*/
struct grow_dirent {
	char *name;
	char *linkname;
	unsigned mode;
	uint64_t size;
	uint64_t inode;
	time_t   mtime;
	char checksum[SHA1_DIGEST_ASCII_LENGTH];
	struct grow_dirent *children;
	struct grow_dirent *parent;
	struct grow_dirent *next;
};

/**
 * Parse the given file to generate an in-memory directory tree.
 * @param FILE A file stream open for reading.
 * @returns A pointer to the root of the directory tree, or NULL on error.
 */
struct grow_dirent *grow_from_file(FILE *file);

/**
 * Recursively free a directory tree.
 */
void grow_delete(struct grow_dirent *d);

/**
 * Resolve a path relative to a root directory.
 * @param path The slash-delimited path to resolve.
 * @param root The root from which to resolve.
 * @param link_count Set to 1 to follow symlinks, 0 otherwise.
 * @returns A pointer to the dirent referred to by the path, or NULL with errno set.
 */
struct grow_dirent *grow_lookup(const char *path, struct grow_dirent *root, int link_count);

/**
 * Given a dirent, fill in a stat buffer.
 * @param d The GROW dirent to read.
 * @param s The stat buffer to write to.
 */
void grow_dirent_to_stat(struct grow_dirent *d, struct stat *s);

#endif
