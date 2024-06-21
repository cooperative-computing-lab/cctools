/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


#include <string.h>
#include <errno.h>
#include <sys/types.h>

#include "grow.h"
#include "debug.h"
#include "xxmalloc.h"
#include "int_sizes.h"

static sha1_context_t grow_filesystem_checksum;

/*
Compare two path strings only up to the first slash.
For example, "foo" matches "foo/bar/baz".
Return one if they match, zero otherwise.
*/

static int compare_path_element( const char *a, const char *b ) {
	while(*a==*b) {
		if(!*a) return 1;
		if(*a=='/') return 1;
		a++;
		b++;
	}

	if(*a==0 && *b=='/') return 1;
	if(*b==0 && *a=='/') return 1;

	return 0;
}

static struct grow_dirent *grow_dirent_create_from_file( FILE *file, struct grow_dirent *parent ) {
	struct grow_dirent *d;
	struct grow_dirent *list=0;
	char line[GROW_LINE_MAX];
	char name[GROW_LINE_MAX];
	char linkname[GROW_LINE_MAX];
	char type;
	static INT64_T inode=2;

	while(fgets(line,sizeof(line),file)) {
		sha1_update(&grow_filesystem_checksum,(unsigned char*)line,strlen(line));
		sha1_update(&grow_filesystem_checksum,(unsigned char*)"\n",1);

		if(line[0]=='E') break;

		d = (struct grow_dirent *) xxmalloc(sizeof(*d));

		linkname[0] = 0;

		/* old large file format */
		int fields = sscanf(line,"%c %[^\t]\t%o %*d %" PRIu64 " %*d %*d %ld %*d %s %[^\n]",
						&type,
						name,
						&d->mode,
						&d->size,
						&d->mtime,
						d->checksum,
						linkname);

		if(fields<6) {
		  /* new more compact file format */
		fields = sscanf(line,"%c %[^\t]\t%u %" PRIu64 " %ld %s %[^\n]",
			&type,
			name,
			&d->mode,
			&d->size,
			&d->mtime,
			d->checksum,
			linkname);

			d->mtime += GROW_EPOCH;
		}

		d->inode = inode++;

		if(fields>=6) {
			d->name = xxstrdup(name);
			if(linkname[0]) {
				d->linkname = xxstrdup(linkname);
			} else {
				d->linkname = 0;
			}
			if(type=='D') {
				d->children = grow_dirent_create_from_file(file,d);
			} else {
				d->children = 0;
			}
			d->parent = parent;
			d->next = list;
			list = d;
		} else {
			debug(D_GROW,"directory listing is corrupted!");
			free(d);
			grow_delete(list);
			return 0;
		}
	}

	return list;
}

/*
Recursively create a grow directory structure by reading
descriptor lines from a stored file.
*/
struct grow_dirent *grow_from_file(FILE *file) {
	return grow_dirent_create_from_file(file, NULL);
}

/*
Recursively destroy a directory structure.
*/

void grow_delete(struct grow_dirent *d) {
	struct grow_dirent *n;

	while(d) {
		if(d->name) free(d->name);
		if(d->linkname) free(d->linkname);
		grow_delete(d->children);
		n = d->next;
		free(d);
		d = n;
	}
}

void grow_dirent_to_stat(struct grow_dirent *d, struct stat *s) {
	s->st_dev = 1;
	s->st_ino = d->inode;
	s->st_mode = d->mode;
	s->st_nlink = 1;
	s->st_uid = 0;
	s->st_gid = 0;
	s->st_rdev = 1;
	s->st_size = d->size;
	s->st_blksize = 65536;
	s->st_blocks = 1+d->size/512;
	s->st_atime = d->mtime;
	s->st_mtime = d->mtime;
	s->st_ctime = d->mtime;
}

/*
Recursively search for the grow_dirent named by path
in the filesystem given by root.  If link_count is zero,
then do not traverse symbolic links.  Otherwise, when
link_count reaches 100, ELOOP is returned.
*/


struct grow_dirent * grow_lookup(const char *path, struct grow_dirent *root, int link_count) {
	struct grow_dirent *d;

	if(!path) path = "\0";
	while(*path=='/') path++;

	if( S_ISLNK(root->mode) && ( link_count>0 || path[0] ) ) {
		if(link_count>100) {
			errno = ELOOP;
			return 0;
		}

		char *linkname = root->linkname;

		if(linkname[0]=='/') {
			while(root->parent) {
				root = root->parent;
			}
		} else {
			root = root->parent;
		}

		root = grow_lookup(linkname, root, link_count + 1);
		if(!root) {
			errno = ENOENT;
			return 0;
		}
	}

	if(!*path) return root;

	if(!S_ISDIR(root->mode)) {
		errno = ENOTDIR;
		return 0;
	}

	const char *subpath = strchr(path,'/');
	if(!subpath) subpath = "\0";

	if(compare_path_element(".",path)) {
		return grow_lookup(subpath, root, link_count);
	}

	if(compare_path_element("..",path)) {
		if(root->parent) {
			return grow_lookup(subpath, root->parent, link_count);
		} else {
			errno = ENOENT;
			return 0;
		}
	}

	for(d=root->children;d;d=d->next) {
		if(compare_path_element(d->name,path)) {
			return grow_lookup(subpath, d, link_count);
		}
	}

	errno = ENOENT;
	return 0;
}
