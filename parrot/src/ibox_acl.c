/*
Copyright (C) 2011- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ibox_acl.h"
#include "pfs_types.h"

#include "debug.h"
#include "stringtools.h"
#include "delete_dir.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

static void make_acl_name(const char *filename, int get_parent, char *aclname)
{
	char tmp[PFS_PATH_MAX];
	sprintf(tmp, "%s/%s", filename, IBOX_ACL_BASE_NAME);
	string_collapse_path(tmp, aclname, 1);
}

static int isdir(const char *path)
{
	struct stat info;
	if(stat(path, &info) < 0) {
		return 0;
	} else {
		return S_ISDIR(info.st_mode);
	}
}

/*
do_ibox_acl_get returns the acl flags associated with a subject and directory.
If the subject has rights there, they are returned and errno is undefined.
If the directory exists, but the subject has no rights, returns zero with errno=0.
If the rights cannot be obtained, returns zero with errno set appropriately.
*/

static int do_ibox_acl_get(const char *dirname, const char *subject, int *totalflags)
{
	FILE *aclfile;
	char aclsubject[PFS_LINE_MAX];
	int aclflags;

	errno = 0;
	*totalflags = 0;

	aclfile = ibox_acl_open(dirname);
	if(aclfile) {
		while(ibox_acl_read(aclfile, aclsubject, &aclflags)) {
			if(string_match(aclsubject, subject)) {
				*totalflags |= aclflags;
			}
		}
		ibox_acl_close(aclfile);
	}

	return 1;
}


int ibox_acl_check_dir(const char *dirname, const char *subject, int flags)
{
	int myflags;

	if(!do_ibox_acl_get(dirname, subject, &myflags)) {
		errno = EACCES;
		return 0;
	}

	if((flags & myflags) == flags) {
		return 1;
	} else {
		errno = EACCES;
		return 0;
	}
}

static int do_ibox_acl_check(const char *path, const char *subject, int flags, int follow_links)
{
	char dirname[PFS_PATH_MAX];

	if(!isdir(path))
		string_dirname(path, dirname);
	else
		strcpy(dirname, path);

	return ibox_acl_check_dir(dirname, subject, flags);
}

int ibox_acl_check(const char *filename, const char *subject, int flags)
{
	return do_ibox_acl_check(filename, subject, flags, 1);
}

FILE *ibox_acl_open(const char *dirname)
{
	char aclname[PFS_PATH_MAX];

	if(!isdir(dirname)) {
		if(errno == ENOTDIR)
			return NULL;
		errno = ENOENT;
		return 0;
	} else {
		make_acl_name(dirname, 0, aclname);
		return fopen(aclname, "r");
	}
}

int ibox_acl_read(FILE * aclfile, char *subject, int *flags)
{
	char acl[PFS_LINE_MAX];
	char tmp[PFS_LINE_MAX];

	while(fgets(acl, sizeof(acl), aclfile)) {
		if(sscanf(acl, "%[^ ] %[rwldpvax()]", subject, tmp) == 2) {
			*flags = ibox_acl_text_to_flags(tmp);
			return 1;
		} else {
			continue;
		}
	}

	return 0;
}

void ibox_acl_close(FILE * aclfile)
{
	fclose(aclfile);
}


const char *ibox_acl_flags_to_text(int flags)
{
	static char text[20];

	text[0] = 0;

	if(flags & IBOX_ACL_READ)
		strcat(text, "r");
	if(flags & IBOX_ACL_WRITE)
		strcat(text, "w");
	if(flags & IBOX_ACL_LIST)
		strcat(text, "l");
	if(flags & IBOX_ACL_DELETE)
		strcat(text, "d");
	if(flags & IBOX_ACL_PUT)
		strcat(text, "p");
	if(flags & IBOX_ACL_ADMIN)
		strcat(text, "a");
	if(flags & IBOX_ACL_EXECUTE)
		strcat(text, "x");
	if(flags & IBOX_ACL_RESERVE) {
		strcat(text, "v");
		strcat(text, "(");
		if(flags & IBOX_ACL_RESERVE_READ)
			strcat(text, "r");
		if(flags & IBOX_ACL_RESERVE_WRITE)
			strcat(text, "w");
		if(flags & IBOX_ACL_RESERVE_LIST)
			strcat(text, "l");
		if(flags & IBOX_ACL_RESERVE_DELETE)
			strcat(text, "d");
		if(flags & IBOX_ACL_RESERVE_PUT)
			strcat(text, "p");
		if(flags & IBOX_ACL_RESERVE_RESERVE)
			strcat(text, "v");
		if(flags & IBOX_ACL_RESERVE_ADMIN)
			strcat(text, "a");
		if(flags & IBOX_ACL_RESERVE_EXECUTE)
			strcat(text, "x");
		strcat(text, ")");
	}

	if(text[0] == 0) {
		strcpy(text, "n");
	}

	return text;
}

int ibox_acl_text_to_flags(const char *t)
{
	int flags = 0;

	while(*t) {
		if(*t == 'r')
			flags |= IBOX_ACL_READ;
		if(*t == 'w')
			flags |= IBOX_ACL_WRITE;
		if(*t == 'l')
			flags |= IBOX_ACL_LIST;
		if(*t == 'd')
			flags |= IBOX_ACL_DELETE;
		if(*t == 'p')
			flags |= IBOX_ACL_PUT;
		if(*t == 'a')
			flags |= IBOX_ACL_ADMIN;
		if(*t == 'x')
			flags |= IBOX_ACL_EXECUTE;
		if(*t == 'v') {
			flags |= IBOX_ACL_RESERVE;
			if(t[1] == '(') {
				t += 2;
				while(*t && *t != ')') {
					if(*t == 'r')
						flags |= IBOX_ACL_RESERVE_READ;
					if(*t == 'w')
						flags |= IBOX_ACL_RESERVE_WRITE;
					if(*t == 'l')
						flags |= IBOX_ACL_RESERVE_LIST;
					if(*t == 'd')
						flags |= IBOX_ACL_RESERVE_DELETE;
					if(*t == 'p')
						flags |= IBOX_ACL_RESERVE_PUT;
					if(*t == 'v')
						flags |= IBOX_ACL_RESERVE_RESERVE;
					if(*t == 'a')
						flags |= IBOX_ACL_RESERVE_ADMIN;
					if(*t == 'x')
						flags |= IBOX_ACL_RESERVE_EXECUTE;
					++t;
				}
			}
		}
		++t;
	}

	return flags;
}

int ibox_acl_from_access_flags(int flags)
{
	int acl = 0;
	if(flags & R_OK)
		acl |= IBOX_ACL_READ;
	if(flags & W_OK)
		acl |= IBOX_ACL_WRITE;
	if(flags & X_OK)
		acl |= IBOX_ACL_EXECUTE;
	if(flags & F_OK)
		acl |= IBOX_ACL_READ;
	if(acl == 0)
		acl |= IBOX_ACL_READ;
	return acl;
}

int ibox_acl_from_open_flags(int flags)
{
	int acl = 0;
	if(flags & O_WRONLY)
		acl |= IBOX_ACL_WRITE;
	if(flags & O_RDWR)
		acl |= IBOX_ACL_READ | IBOX_ACL_WRITE;
	if(flags & O_CREAT)
		acl |= IBOX_ACL_WRITE;
	if(flags & O_TRUNC)
		acl |= IBOX_ACL_WRITE;
	if(flags & O_APPEND)
		acl |= IBOX_ACL_WRITE;
	if(acl == 0)
		acl |= IBOX_ACL_READ;
	return acl;
}

int ibox_acl_init_copy(const char *path)
{
	char oldpath[PFS_LINE_MAX];
	char newpath[PFS_LINE_MAX];
	char subject[PFS_LINE_MAX];
	FILE *oldfile;
	FILE *newfile;
	int result = 0;
	int flags;

	sprintf(oldpath, "%s/..", path);
	sprintf(newpath, "%s/%s", path, IBOX_ACL_BASE_NAME);

	oldfile = ibox_acl_open(oldpath);
	if(oldfile) {
		newfile = fopen(newpath, "w");
		if(newfile) {
			while(ibox_acl_read(oldfile, subject, &flags)) {
				fprintf(newfile, "%s %s\n", subject, ibox_acl_flags_to_text(flags));
			}
			fclose(newfile);
			result = 1;
		}
		ibox_acl_close(oldfile);
	}

	return result;
}

/*
  Because each directory now contains an ACL,
  a simple rmdir will not work on a (perceived) empty directory.
  This function checks to see if the directory is empty,
  save for the ACL file, and deletes it if so.
  Otherwise, it determines an errno and returns with failure.
*/

int ibox_acl_rmdir(const char *path)
{
	void *dir;
	struct dirent *d;

	dir = opendir(path);
	if(dir) {
		while((d = readdir(dir))) {
			if(!strcmp(d->d_name, "."))
				continue;
			if(!strcmp(d->d_name, ".."))
				continue;
			if(!strcmp(d->d_name, IBOX_ACL_BASE_NAME))
				continue;
			closedir(dir);
			errno = ENOTEMPTY;
			return -1;
		}
		closedir(dir);
		return delete_dir(path);
	} else {
		errno = ENOENT;
		return -1;
	}
}
