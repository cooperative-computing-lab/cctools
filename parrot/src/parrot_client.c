/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "parrot_client.h"

#include "tracer.table.h"
#include "tracer.table64.h"
#include "int_sizes.h"

#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <stdio.h>

int parrot_whoami( const char *path, char *buf, int size )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_whoami,path,buf,size);
#else
	return syscall(SYSCALL64_parrot_whoami,path,buf,size);
#endif
}

int parrot_locate( const char *path, char *buf, int size )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_locate,path,buf,size);
#else
	return syscall(SYSCALL64_parrot_locate,path,buf,size);
#endif
}

int parrot_getacl( const char *path, char *buf, int size )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_getacl,path,buf,size);
#else
	return syscall(SYSCALL64_parrot_getacl,path,buf,size);
#endif
}

int parrot_setacl( const char *path, const char *subject, const char *rights )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_setacl,path,subject,rights);
#else
	return syscall(SYSCALL64_parrot_setacl,path,subject,rights);
#endif
}

int parrot_md5( const char *filename, unsigned char *digest )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_md5,filename,digest);
#else
	return syscall(SYSCALL64_parrot_md5,filename,digest);
#endif
}

int parrot_cp( const char *source, const char *dest )
{
#ifdef CCTOOLS_CPU_I386
		return syscall(SYSCALL32_parrot_copyfile,source,dest);
#else
		return syscall(SYSCALL64_parrot_copyfile,source,dest);
#endif
}

int parrot_mkalloc( const char *path, long long size, mode_t mode )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_mkalloc,path,&size,mode);
#else
	return syscall(SYSCALL64_parrot_mkalloc,path,&size,mode);
#endif
}

int parrot_lsalloc( const char *path, char *alloc_path, long long *total, long long *inuse )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_lsalloc,path,alloc_path,total,inuse);
#else
	return syscall(SYSCALL64_parrot_lsalloc,path,alloc_path,total,inuse);
#endif
}

int parrot_timeout( const char *time )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_timeout,time);
#else
	return syscall(SYSCALL64_parrot_timeout,time);
#endif
}

SEARCH *opensearch( const char *path, const char *pattern, int flags ) 
{
	int err;
	size_t buffer_size = 2048;
	char *buffer;

	do {
		buffer = malloc(buffer_size);

		if (buffer==NULL) {
			errno = ENOMEM;
			return NULL;
		}

		#ifdef CCTOOLS_CPU_I386
			err = syscall(SYSCALL32_search,path,pattern,flags,buffer,buffer_size);
		#else
			err = syscall(SYSCALL64_search,path,pattern,flags,buffer,buffer_size);
		#endif	

		buffer_size*=2;
	} while (err==-1 && errno==ERANGE);

	if (err==-1) return NULL;

	SEARCH *result = malloc(sizeof(SEARCH));
	result->entry = (struct searchent*) malloc(sizeof(struct searchent));
	result->data = buffer;
	result->i = 0;
	
	return result;
}

static char *readsearch_next(char *data, int *i) {
	data += *i;
	char *tail = strchr(data, ':');
	int length = (tail==NULL) ? strlen(data) : tail - data;

	if (length==0) return NULL;

	char *next = malloc(length + 1);
	strncpy(next, data, length);
	next[length] = '\0';
	*i += length + 1;

	return next;
}

static struct stat *readsearch_unpack_stat(char *stat_str) {
	if (stat_str==NULL) return NULL;
	
	struct stat *info = (struct stat*) malloc(sizeof(struct stat));
	sscanf(
		stat_str, 
		"%zd,%zd,%d,%zd,%d,%d,%zd,%zd,%zd,%zd,%zd,%zd,%zd", 
		&info->st_dev,
		&info->st_ino,
		&info->st_mode,
		&info->st_nlink,
		&info->st_uid,
		&info->st_gid,
		&info->st_rdev,
		&info->st_size,
		&info->st_atime,
		&info->st_mtime,
		&info->st_ctime,
		&info->st_blksize,
		&info->st_blocks
	);

	free(stat_str);
	return info;
}

struct searchent *readsearch(SEARCH *search) 
{
	int i = search->i;
	char *data = search->data;
	char *err_str = readsearch_next(data, &i);
	free(search->entry->path);
	free(search->entry->info);

	if (err_str==NULL) return NULL;

	char *path;
	struct stat *info;
	int err = atoi(err_str), errsource;

	if (err) {
		errsource = atoi(readsearch_next(data, &i));
		path = readsearch_next(data, &i);
		info = NULL;
	} else {
		errsource = 0;
		path = readsearch_next(data, &i);
		info = readsearch_unpack_stat(readsearch_next(data, &i));
	} 

	search->entry->path = path;
	search->entry->info = info;
	search->entry->errsource = errsource;
	search->entry->err = err;
	search->i = i;

	return search->entry;
}

int closesearch(SEARCH *search) {
	free(search->data);
	free(search->entry);
	free(search);
	return 0;
}
