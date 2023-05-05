/*
Copyright (C) 2023- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_checksum.h"

#include "stringtools.h"
#include "string_array.h"
#include "sort_dir.h"
#include "md5.h"
#include "xxmalloc.h"

#include <debug.h>
#include <dirent.h>
#include <sys/stat.h>
#include <string.h>

/*
Compute the recursive hash of a directory by building up a string like this:

	filea:hash-of-filea
	fileb:hash-of-fileb
	dirc:hash-of-dirc

And then compute the hash of that string.

Returns an allocated string that must be freed.

XXX For consistency, this should sort the directory entries before hashing.
*/

static char *vine_checksum_dir( const char *path, ssize_t *totalsize )
{
	char *dirstring=xxstrdup("");
	char **entries;
	struct stat info;
	if(!sort_dir(path, &entries, strcmp)) return 0;
	int i;
	for(i=0; entries[i]; i++){

		if(!strcmp(entries[i],".")) continue;
		if(!strcmp(entries[i],"..")) continue;

		char *subpath = string_format("%s/%s",path,entries[i]);
		if(stat(subpath, &info)) return 0;

		char *mode = string_format("%o",info.st_mode);
		char *mtime = ctime(&info.st_mtime);
		char *meta = string_format("%s:%s",mode,mtime);
		char *subhash = vine_checksum_any(subpath,totalsize);
		char *line = string_format("%s:%s:%s:\n",entries[i],meta,subhash);

		dirstring = string_combine(dirstring,line);

		free(subpath);
		free(subhash);
		free(mode);
		free(meta);
		free(line);
	}

	sort_dir_free(entries);
	char *result = md5_of_string(dirstring);

	free(dirstring);

	return result;
}

static char *vine_checksum_file( const char *path )
{
	unsigned char digest[MD5_DIGEST_LENGTH];
	md5_file(path, digest);
	return xxstrdup(md5_to_string(digest));
}

static char *vine_checksum_symlink( const char *path, ssize_t linklength )
{
	char *linktext = xxmalloc(linklength+1);
	ssize_t actual = readlink(path,linktext,linklength);
	if(actual!=linklength) {
		free(linktext);
		return 0;
	} else {
		linktext[linklength] = 0;
		char *result = md5_of_string(linktext);
		free(linktext);
		return result;
	}
}
	
char *vine_checksum_any( const char *path, ssize_t *totalsize )
{
	struct stat info;

	if(lstat(path, &info)) return 0; 

	if(S_ISDIR(info.st_mode)) {
		return vine_checksum_dir(path,totalsize);
	} else if(S_ISREG(info.st_mode)) {
		*totalsize += info.st_size;
		return vine_checksum_file(path);
	} else if(S_ISLNK(info.st_mode)) {
		return vine_checksum_symlink(path,info.st_size);
	} else {
		debug(D_NOTICE,	"unexpected file type: %s is not a file, directory, or symlink.",path);
		return 0;
	}
}
