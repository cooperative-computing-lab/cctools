/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "parrot_client.h"

#include "tracer.table.h"
#include "tracer.table64.h"
#include "int_sizes.h"

#include <stdlib.h>
#include <unistd.h>

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

int parrot_search( const char *path, const char *pattern, char *buffer, size_t len1, struct stat *stats, size_t len2 ) {
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_search,path,pattern,buffer,len1,stats,len2);
#else
	return syscall(SYSCALL64_parrot_search,path,pattern,buffer,len1,stats,len2);
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

int parrot_mkalloc( const char *path, INT64_T size, mode_t mode )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_mkalloc,path,&size,mode);
#else
	return syscall(SYSCALL64_parrot_mkalloc,path,&size,mode);
#endif
}

int parrot_lsalloc( const char *path, char *alloc_path, INT64_T *total, INT64_T *inuse )
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

