/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "chirp_acl.h"
#include "chirp_protocol.h"

#include "username.h"
#include "stringtools.h"
#include "debug.h"
#include "delete_dir.h"
#include "chirp_group.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

/* Cygwin does not have 64-bit I/O, while Darwin has it by default. */

#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#define ftruncate64 ftruncate
#define truncate64 truncate
#define statfs64 statfs
#define fstatfs64 fstatfs
#endif

#if CCTOOLS_OPSYS_DARWIN
#define lchown chown
#endif

static int read_only_mode = 0;
static const char *default_acl=0;

extern const char *chirp_super_user;

static int do_stat( const char *filename, struct stat *buf )
{
	int result;
	do {
		result = stat(filename,buf);
	} while( result==-1 && errno==EINTR );
	return result;
}

void chirp_acl_force_readonly()
{
	read_only_mode = 1;
}

void chirp_acl_default( const char *d )
{
	default_acl = d;
}

static int is_a_directory( const char *filename )
{
	struct stat info;

	if(do_stat(filename,&info)==0) {
		if(S_ISDIR(info.st_mode)) {
			return 1;
		} else {
			return 0;
		}
	} else {
		return 0;
	}
}

static void make_acl_name( const char *filename, int get_parent, char *aclname )
{
	sprintf(aclname,"%s/%s",filename,CHIRP_ACL_BASE_NAME);
}

/*
do_chirp_acl_get returns the acl flags associated with a subject and directory.
If the subject has rights there, they are returned and errno is undefined.
If the directory exists, but the subject has no rights, returns zero with errno=0.
If the rights cannot be obtained, returns zero with errno set appropriately.
*/

static int do_chirp_acl_get( const char *filename, const char *dirname, const char *subject )
{
	FILE *aclfile;
	char aclsubject[CHIRP_LINE_MAX];
	int aclflags;
	int totalflags;

	errno = 0;
	totalflags = 0;
  
	aclfile = chirp_acl_open(dirname);
  	if(aclfile) {
		while(chirp_acl_read(aclfile,aclsubject,&aclflags)) {
			if(string_match(aclsubject,subject)) {
				totalflags |= aclflags;
			} else if(!strncmp(aclsubject,"group:",6)) {
				if(chirp_group_lookup(aclsubject,subject)) {
					totalflags |= aclflags;
				}
			}
		}
		chirp_acl_close(aclfile);
	} else {
		if(errno!=ENOENT) errno = EACCES;
	}

	if(read_only_mode) {
		totalflags &= CHIRP_ACL_READ|CHIRP_ACL_LIST;
	}

	return totalflags;
}


int chirp_acl_check_dir( const char *dirname, const char *subject, int flags )
{
	int myflags = do_chirp_acl_get(dirname,dirname,subject);

	/* The superuser can implicitly list and admin */

	if(chirp_super_user && !strcmp(subject,chirp_super_user)) {
		myflags |= CHIRP_ACL_LIST|CHIRP_ACL_ADMIN;
	}

	if( ( flags & myflags ) == flags ) {
		return 1;
	} else {
		errno = EACCES;
		return 0;
	}
}

static int do_chirp_acl_check( const char *filename, const char *subject, int flags, int follow_links )
{
	char linkname[CHIRP_PATH_MAX];
	char temp[CHIRP_PATH_MAX];
	char dirname[CHIRP_PATH_MAX];

	/*
	Symbolic links require special handling.
	If requested, follow the link and look for rights in that directory.
	*/

	if(follow_links && flags!=CHIRP_ACL_DELETE) {
		int length = readlink(filename,linkname,sizeof(linkname));
		if(length>0) {
			linkname[length] = 0;

			/* If the link is relative, construct a full path */

			if(linkname[0]!='/') {
				sprintf(temp,"%s/../%s",filename,linkname);
				string_collapse_path(temp,linkname,1);
			}

			/* Use the linkname now to look up the ACL */

			debug(D_DEBUG,"symlink %s points to %s",filename,linkname);
			filename = linkname;
		}
	}

	/*
	If the file being checked is an ACL file,
	then it may be written with the admin flag, but never deleted.
	*/

	if(!strcmp(string_back(filename,CHIRP_ACL_BASE_LENGTH),CHIRP_ACL_BASE_NAME)) {
		if(flags&CHIRP_ACL_DELETE) {
			errno = EACCES;
			return 0;
		}
		if(flags&CHIRP_ACL_WRITE) {
			flags &= ~CHIRP_ACL_WRITE;
			flags |= CHIRP_ACL_ADMIN;
		}
	}

	/* Now get the name of the directory containing the file */

	sprintf(temp,"%s/..",filename);
	string_collapse_path(temp,dirname,1);

	/* Perform the permissions check on that directory. */

	return chirp_acl_check_dir(dirname,subject,flags);
}

int chirp_acl_check( const char *filename, const char *subject, int flags )
{
	return do_chirp_acl_check(filename,subject,flags,1);
}

int chirp_acl_check_link( const char *filename, const char *subject, int flags )
{
	return do_chirp_acl_check(filename,subject,flags,0);
}

static int add_mode_bits( const char *path, mode_t mode )
{
	struct stat64 info;
	int result;
	result = stat64(path,&info);
	if(result<0) return result;
	return chmod(path,info.st_mode|mode);
}


static int remove_mode_bits( const char *path, mode_t mode )
{
	struct stat64 info;
	int result;
	result = stat64(path,&info);
	if(result<0) return result;
	return chmod(path,info.st_mode&(~mode));
}

static int add_mode_bits_all( const char *path, mode_t mode )
{
	char subpath[CHIRP_PATH_MAX];
	struct dirent *d;
	DIR *dir;
	struct stat64 info;
	int result;

	dir = opendir(path);
	if(!dir) return -1;

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;

		sprintf(subpath,"%s/%s",path,d->d_name);

		result = stat64(subpath,&info);
		if(result<0) continue;

		if(S_ISREG(info.st_mode)) {
			chmod(subpath,info.st_mode|mode);
		}
	}

	closedir(dir);

	return 0;
}

static int remove_mode_bits_all( const char *path, mode_t mode )
{
	char subpath[CHIRP_PATH_MAX];
	struct dirent *d;
	DIR *dir;
	struct stat64 info;
	int result;

	dir = opendir(path);
	if(!dir) return -1;

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;

		sprintf(subpath,"%s/%s",path,d->d_name);

		result = stat64(subpath,&info);
		if(result<0) continue;

		if(S_ISREG(info.st_mode)) {
			chmod(subpath,info.st_mode&(~mode));
		}
	}

	closedir(dir);

	return 0;
}

int chirp_acl_set( const char *dirname, const char *subject, int flags, int reset_acl )
{
	char aclname[CHIRP_PATH_MAX];
	char newaclname[CHIRP_PATH_MAX];
	char aclsubject[CHIRP_LINE_MAX];
	int aclflags;
	FILE *aclfile, *newaclfile;
	int result;
	int replaced_acl_entry=0;

	if(!is_a_directory(dirname)) {
		errno = ENOTDIR;
		return -1;
	}

	sprintf(aclname,"%s/%s",dirname,CHIRP_ACL_BASE_NAME);
	sprintf(newaclname,"%s/%s.%d",dirname,CHIRP_ACL_BASE_NAME,(int)getpid());

	if(reset_acl) {
		aclfile = fopen("/dev/null","r");
	} else {
		aclfile = fopen(aclname,"r");

		/* If the acl never existed, then we can simply create it. */
		if(!aclfile && errno==ENOENT) {
			if(default_acl) {
				aclfile = fopen(default_acl,"r");
			} else {
				aclfile = fopen("/dev/null","r");
			}
		}
	}

	if(!aclfile) {
		errno = EACCES;
		return -1;
	}

	replaced_acl_entry = 0;

	newaclfile = fopen(newaclname,"w");
	if(!newaclfile) {
		fclose(aclfile);
		errno = EACCES;
		return -1;
	}

	while(chirp_acl_read(aclfile,aclsubject,&aclflags)) {
		if(!strcmp(subject,aclsubject)) {
			aclflags = flags;
			replaced_acl_entry = 1;
		}
		if(aclflags!=0) {
			fprintf(newaclfile,"%s %s\n",aclsubject,chirp_acl_flags_to_text(aclflags));
		}
	}

	if(!replaced_acl_entry) {
		fprintf(newaclfile,"%s %s\n",subject,chirp_acl_flags_to_text(flags));
	}

	/* Need to force a write in order to get response from ferror */

	fflush(newaclfile);

	if(ferror(newaclfile)) {
		errno = EACCES;
		result = -1;
	} else {
		result = rename(newaclname,aclname);
		if(result<0) {
			unlink(newaclname);
			errno = EACCES;
			result = -1;
		}
	}

	fclose(aclfile);
	fclose(newaclfile);

	if(!strcmp(subject,"system:localuser")) {
		if(flags&CHIRP_ACL_READ) {
			add_mode_bits_all(dirname,0044);
		} else {
			remove_mode_bits_all(dirname,0044);
		}

		if(flags&CHIRP_ACL_LIST) {
			add_mode_bits(dirname,0055);
		} else {
			remove_mode_bits(dirname,0055);
		}

		if(flags&CHIRP_ACL_WRITE) {
			add_mode_bits(dirname,0022);
			add_mode_bits_all(dirname,0022);
		} else {
			remove_mode_bits(dirname,0022);
			remove_mode_bits_all(dirname,0022);
		}
	}

	return result;
}

FILE * chirp_acl_open( const char *dirname )
{
	char aclname[CHIRP_PATH_MAX];
	FILE *file;

	if(!is_a_directory(dirname)) {
		if (errno == ENOENT && default_acl) {
			file = fopen(default_acl,"r");
			return file;
		}
		errno = ENOENT;
		return 0;
	} else {
		make_acl_name(dirname,0,aclname);
		file = fopen(aclname,"r");
		if(!file && default_acl) file = fopen(default_acl,"r");
		return file;
	}
}

int chirp_acl_read( FILE *aclfile, char *subject, int *flags )
{
	char acl[CHIRP_LINE_MAX];
	char tmp[CHIRP_LINE_MAX];

	while(fgets(acl,sizeof(acl),aclfile)) {
		if(sscanf(acl,"%[^ ] %[rwldpvax()]",subject,tmp)==2) {
			*flags = chirp_acl_text_to_flags(tmp);
			return 1;
		} else {
			continue;
		}
	}

	return 0;
}

void chirp_acl_close( FILE *aclfile )
{
	fclose(aclfile);
}


const char * chirp_acl_flags_to_text( int flags )
{
	static char text[20];

	text[0] = 0;

	if(flags&CHIRP_ACL_READ)    strcat(text,"r");
	if(flags&CHIRP_ACL_WRITE)   strcat(text,"w");
	if(flags&CHIRP_ACL_LIST)    strcat(text,"l");
	if(flags&CHIRP_ACL_DELETE)  strcat(text,"d");
	if(flags&CHIRP_ACL_PUT)     strcat(text,"p");
	if(flags&CHIRP_ACL_ADMIN)   strcat(text,"a");
	if(flags&CHIRP_ACL_EXECUTE) strcat(text,"x");
	if(flags&CHIRP_ACL_RESERVE) {
		strcat(text,"v");
		strcat(text,"(");
		if(flags&CHIRP_ACL_RESERVE_READ)    strcat(text,"r");
		if(flags&CHIRP_ACL_RESERVE_WRITE)   strcat(text,"w");
		if(flags&CHIRP_ACL_RESERVE_LIST)    strcat(text,"l");
		if(flags&CHIRP_ACL_RESERVE_DELETE)  strcat(text,"d");
		if(flags&CHIRP_ACL_RESERVE_PUT)     strcat(text,"p");
		if(flags&CHIRP_ACL_RESERVE_RESERVE) strcat(text,"v");
		if(flags&CHIRP_ACL_RESERVE_ADMIN)   strcat(text,"a");
		if(flags&CHIRP_ACL_RESERVE_EXECUTE) strcat(text,"x");
		strcat(text,")");
	}

	if(text[0]==0) {
		strcpy(text,"n");
	}

	return text;
}

int chirp_acl_text_to_flags( const char *t )
{
	int flags=0;

	while(*t) {
		if( *t=='r' ) flags|=CHIRP_ACL_READ;
		if( *t=='w' ) flags|=CHIRP_ACL_WRITE;
		if( *t=='l' ) flags|=CHIRP_ACL_LIST;
		if( *t=='d' ) flags|=CHIRP_ACL_DELETE;
		if( *t=='p' ) flags|=CHIRP_ACL_PUT;
		if( *t=='a' ) flags|=CHIRP_ACL_ADMIN;
		if( *t=='x' ) flags|=CHIRP_ACL_EXECUTE;
		if( *t=='v' ) {
			flags |= CHIRP_ACL_RESERVE;
			if( t[1] == '(' ) {
				t += 2;
				while(*t && *t != ')'){
					if( *t=='r' ) flags|=CHIRP_ACL_RESERVE_READ;
					if( *t=='w' ) flags|=CHIRP_ACL_RESERVE_WRITE;
					if( *t=='l' ) flags|=CHIRP_ACL_RESERVE_LIST;
					if( *t=='d' ) flags|=CHIRP_ACL_RESERVE_DELETE;
					if( *t=='p' ) flags|=CHIRP_ACL_RESERVE_PUT;
					if( *t=='v' ) flags|=CHIRP_ACL_RESERVE_RESERVE;
					if( *t=='a' ) flags|=CHIRP_ACL_RESERVE_ADMIN;
					if( *t=='x' ) flags|=CHIRP_ACL_RESERVE_EXECUTE;
					++t;
				}
			}
		}
		++t;
	}

	return flags;
}

int chirp_acl_from_access_flags( int flags )
{
	int acl=0;
	if(flags&R_OK) acl|=CHIRP_ACL_READ;
	if(flags&W_OK) acl|=CHIRP_ACL_WRITE;
	if(flags&X_OK) acl|=CHIRP_ACL_EXECUTE;
	if(flags&F_OK) acl|=CHIRP_ACL_READ;
	if(acl==0)     acl|=CHIRP_ACL_READ;
	return acl;
}

int chirp_acl_from_open_flags( int flags )
{
	int acl = 0;
	if(flags&O_WRONLY) acl|=CHIRP_ACL_WRITE;
	if(flags&O_RDWR)   acl|=CHIRP_ACL_READ|CHIRP_ACL_WRITE;
	if(flags&O_CREAT)  acl|=CHIRP_ACL_WRITE;
	if(flags&O_TRUNC)  acl|=CHIRP_ACL_WRITE;
	if(flags&O_APPEND) acl|=CHIRP_ACL_WRITE;
	if(acl==0)         acl|=CHIRP_ACL_READ;
	return acl;
}

int chirp_acl_init_root( const char *path )
{
	char aclpath[CHIRP_PATH_MAX];
	char username[USERNAME_MAX];
	FILE *file;

	file = chirp_acl_open(path);
	if(file) {
		chirp_acl_close(file);
		return 1;
	}

	username_get(username);

	sprintf(aclpath,"%s/%s",path,CHIRP_ACL_BASE_NAME);
	file = fopen(aclpath,"w");
	if(file) {
		fprintf(file,"unix:%s %s\n",username,chirp_acl_flags_to_text(CHIRP_ACL_READ|CHIRP_ACL_WRITE|CHIRP_ACL_DELETE|CHIRP_ACL_LIST|CHIRP_ACL_ADMIN));
		fclose(file);
		return 1;
	} else {
		return 0;
	}
}

int chirp_acl_init_copy( const char *path )
{
	char oldpath[CHIRP_LINE_MAX];
	char newpath[CHIRP_LINE_MAX];
	char subject[CHIRP_LINE_MAX];
	FILE *oldfile;
	FILE *newfile;
	int result = 0;
	int flags;

	sprintf(oldpath,"%s/..",path);
	sprintf(newpath,"%s/%s",path,CHIRP_ACL_BASE_NAME);

	oldfile = chirp_acl_open(oldpath);
	if(oldfile) {
		newfile = fopen(newpath,"w");
		if(newfile) {
			while(chirp_acl_read(oldfile,subject,&flags)) {
				fprintf(newfile,"%s %s\n",subject,chirp_acl_flags_to_text(flags));
			}
			fclose(newfile);
			result = 1;
		}
		chirp_acl_close(oldfile);
	}

	return result;
}

int chirp_acl_init_reserve( const char *path, const char *subject )
{
	char dirname[CHIRP_PATH_MAX];
	char aclpath[CHIRP_PATH_MAX];
	FILE *file;
	int newflags = 0;

	string_dirname(path,dirname);

	int aclflags = do_chirp_acl_get(dirname,dirname,subject);

	if(aclflags&CHIRP_ACL_RESERVE_READ)
		newflags|=CHIRP_ACL_READ;
	if(aclflags&CHIRP_ACL_RESERVE_WRITE)
		newflags|=CHIRP_ACL_WRITE;
	if(aclflags&CHIRP_ACL_RESERVE_LIST)
		newflags|=CHIRP_ACL_LIST;
	if(aclflags&CHIRP_ACL_RESERVE_DELETE)
		newflags|=CHIRP_ACL_DELETE;
	if(aclflags&CHIRP_ACL_RESERVE_PUT)
		newflags|=CHIRP_ACL_PUT;
	if(aclflags&CHIRP_ACL_RESERVE_RESERVE)
		newflags|=CHIRP_ACL_RESERVE;
	if(aclflags&CHIRP_ACL_RESERVE_ADMIN)
		newflags|=CHIRP_ACL_ADMIN;
	if(aclflags&CHIRP_ACL_RESERVE_EXECUTE)
		newflags|=CHIRP_ACL_EXECUTE;

	/*
	compatibility note:
	If no sub-rights are associated with the v right,
	then give all of the ordinary subrights.
	*/

	if(newflags==0) newflags = CHIRP_ACL_READ|CHIRP_ACL_WRITE|CHIRP_ACL_LIST|CHIRP_ACL_DELETE|CHIRP_ACL_ADMIN;

	sprintf(aclpath,"%s/%s",path,CHIRP_ACL_BASE_NAME);
	file = fopen(aclpath,"w");
	if(file) {
		fprintf(file,"%s %s\n",subject,
			chirp_acl_flags_to_text(newflags));
		fclose(file);
		return 1;
	} else {
		return 0;
	}
}

/*
  Because each directory now contains an ACL,
  a simple rmdir will not work on a (perceived) empty directory.
  This function checks to see if the directory is empty,
  save for the ACL file, and deletes it if so.
  Otherwise, it determines an errno and returns with failure.
*/

int chirp_acl_rmdir( const char *path )
{
	DIR *dir;
	struct dirent *d;

	dir = opendir(path);
	if(dir) {
		while((d=readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;
			if(!strcmp(d->d_name,CHIRP_ACL_BASE_NAME)) continue;
			closedir(dir);
			errno = ENOTEMPTY;
			return -1;
		}
		closedir(dir);
		delete_dir(path);
		return 0;
	} else {
		errno = ENOENT;
		return -1;
	}
}
