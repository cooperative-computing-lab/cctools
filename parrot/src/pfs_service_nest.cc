/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_NEST

#include "pfs_table.h"
#include "pfs_service.h"
#include "nest_speak.h"
#include "nest_error.h"

extern "C" {
#include "debug.h"
#include "stringtools.h"
}

#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <errno.h>

static int convert_error( NestReplyStatus r )
{
	switch(r) {
		case NEST_LOCAL_FILE_NOT_FOUND:
		case NEST_REMOTE_FILE_NOT_FOUND:
		case NEST_FILE_NOT_FOUND:
			return ENOENT;
		case NEST_ALREADY_EXISTS:
		case NEST_FILE_EXISTS:
			return EEXIST;
		case NEST_NOT_EMPTY:
			return ENOTEMPTY;
		case NEST_FILENAME_TOO_LONG:
			return EFBIG;
		case NEST_NO_CONNECTION:
		case NEST_PARTIAL_MESSAGE:
			return ECONNRESET;
		case NEST_INSUFFICIENT_SPACE:
		case NEST_DISK_FULL:
			return ENOSPC;
		case NEST_BAD_PARAMETERS:
		case NEST_UNKNOWN_REQUEST_TYPE:
			return EINVAL;
		case NEST_TOO_MANY_LINKS:
			return EMLINK;
		case NEST_NOT_YET_IMPLEMENTED:
			return ENOSYS;
		case NEST_NONPRIVELEDGED_USER:
		case NEST_UNKNOWN_USER:
		case NEST_NONPRIVELEDGED_SERVER:
		case NEST_INVALID_AUTHENTICATION:
		case NEST_COULD_NOT_AUTHENTICATE:
			return EACCES;
		case NEST_TEMPORARILY_UNAVAILABLE:
			return EBUSY;
		case NEST_STALE_FH:
			return ESTALE;
		case NEST_INTR:
			return EINTR;
		case NEST_LOT_NOT_ENOUGH:
		case NEST_LOT_LIMIT_REACHED:
			return ENOSPC;
		case NEST_NOT_DIRECTORY:
			return ENOTDIR;
		case NEST_UNKNOWN_ERROR:
		case NEST_UNKNOWN_DEBUG_VALUE:
		case NEST_OUT_OF_RESOURCES:
		case NEST_SYSTEM_FILE_MISSING:
		case NEST_USER_EXISTS:
		case NEST_USER_NOT_FOUND:
		case NEST_USER_LIMIT_REACHED:
		case NEST_GROUP_EXISTS:
		case NEST_GROUP_NOT_FOUND:
		case NEST_GROUP_LIMIT_REACHED:
		case NEST_GROUP_ACTIVE:
		case NEST_NO_QUOTA_ENFORCEMENT:
		case NEST_QUOTA_ENFORCEMENT_ERROR:
		case NEST_LOT_SCHEDULED:
		case NEST_LOT_NOT_FOUND:
		case NEST_LOT_NOT_EMPTY:
		case NEST_LOT_INVALID:
		case NEST_LOT_DISABLED:
		case NEST_CLASSAD_ERROR:
		case NEST_FLOCK:
		case NEST_SYSTEM_FILE_UPDATE_FAILED:
		case NEST_LOT_UPDATE_FAILED:
		case NEST_INCONSISTENT_STATE:
			return EIO;
			break;
	}

	return 0;
}

/* Remove trailing slashes from a path */

static void chomp_slashes( char *s )
{
	char *t = s;

	if(!s) return;

	while(*t) {
		t++;
	}

	t--;

	while(*t=='/' && t!=s ) {
		*t=0;
		t--;
	}
}

class pfs_file_nest : public pfs_file {
public:
	pfs_file_nest( pfs_name *n ) : pfs_file(n) {
	}

	int close() {
		return 0;
	}

	pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
		NestReplyStatus status;
		int actual;

		NestConnection fd = (int) pfs_service_connect_cache(&name);
		if(!fd) return -1;

		debug(D_NEST,"ReadBytes %s %d 0x%x",name.rest,offset,data,length);
		status = NestReadBytes(fd,name.rest,offset,(char*)data,length,actual);
		debug(D_NEST,"= %d %s",(status==NEST_SUCCESS) ? actual : 0,NestErrorString(status));
		int invalid = (status==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(&name,(void*)fd,invalid);

		if(status==NEST_SUCCESS) {
			return actual;
		} else {
			errno = convert_error(status);
			return -1;
		}
	}

	pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
		NestReplyStatus status;

		NestConnection fd = (int) pfs_service_connect_cache(&name);
		if(!fd) return -1;

		debug(D_NEST,"WriteBytes %s %d 0x%x %d",name.rest,offset,data,length);
		status = NestWriteBytes(fd,name.rest,offset,(char*)data,length,1);
		debug(D_NEST,"= %s",NestErrorString(status));
		int invalid = (status==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(&name,(void*)fd,invalid);
		if(status==NEST_SUCCESS) {
			return length;
		} else {
			errno = convert_error(status);
			return -1;
		}
	}

	int fstat( struct pfs_stat *buf ) {
		return name.service->stat(&name,buf);
	}

	int ftruncate( pfs_size_t length ) {
		return 0;
	}

	pfs_ssize_t get_size() {
		NestReplyStatus status;
		long size;

		NestConnection fd = (int) pfs_service_connect_cache(&name);
		if(!fd) return -1;

		debug(D_NEST,"Filesize %s",name.rest);
		status = NestFilesize(size,name.rest,fd);
		debug(D_NEST,"= %d %s",(status==NEST_SUCCESS) ? size : 0,NestErrorString(status));
		int invalid = (status==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(&name,(void*)fd,invalid);
		if(status==NEST_SUCCESS) {
			return (pfs_ssize_t)size;
		} else {
			errno = convert_error(status);
			return -1;
		}
	}
};

static void list_buffer_append( NestFileInfo *finfo, void *arg )
{
	pfs_dir *dir = (pfs_dir *) arg;
	dir->append(finfo->name);
}

class pfs_service_nest : public pfs_service {
public:

	void * connect( pfs_name *name ) {
		NestConnection fd;
		NestReplyStatus result;
		debug(D_NEST,"connecting to %s",name->host);
		result = NestOpenConnection(fd,name->host);
		if(result!=NEST_SUCCESS) result = NestOpenAnonymously(fd,name->host);

		if(result==NEST_SUCCESS) {
			return (void*)fd;
		} else {
			debug(D_NEST,"couldn't connect: %s",NestErrorString(result));
			errno = convert_error(result);
			return 0;
		}
	}

	void disconnect( pfs_name *name, void *cxn ) {
		NestConnection fd = (int)cxn;
		debug(D_NEST,"disconnecting from %s",name->host);
		NestCloseConnection(fd);
	}

	pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		NestReplyStatus result=NEST_SUCCESS;
		int rval = -1;

		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return 0;

		if(flags&O_TRUNC) {
			debug(D_NEST,"RemoveFile %s",name->rest);
			result = NestRemoveFile(name->rest,fd);
			debug(D_NEST,"= %s",NestErrorString(result));
			if(result==NEST_FILE_NOT_FOUND || result==NEST_REMOTE_FILE_NOT_FOUND || result==NEST_LOCAL_FILE_NOT_FOUND ) result = NEST_SUCCESS;
			if(result!=NEST_SUCCESS) {
				errno = convert_error(result);
				goto forgetit;
			}
		}

		if(flags&O_CREAT || flags&O_TRUNC) {
			debug(D_NEST,"WriteBytes %s 0 0 0 1",name->rest);
			result = NestWriteBytes(fd,name->rest,0,name->rest,0,1);
			debug(D_NEST,"= %s",NestErrorString(result));
			if(result==NEST_SUCCESS) {
				rval = 0;
			} else {
				errno = convert_error(result);
				rval = -1;
			}
		} else {
			debug(D_NEST,"ChangeDir %s",name->rest);
			result = NestChangeDir(name->rest,fd);
			debug(D_NEST,"= %s",NestErrorString(result));

			if(result==NEST_SUCCESS) {
				errno = EISDIR;
				rval = -1;
			} else {
				long size;
				debug(D_NEST,"Filesize %s",name->rest);
				result = NestFilesize(size,name->rest,fd);
				debug(D_NEST,"= %s",NestErrorString(result));

				if(result==NEST_SUCCESS) {
					rval = 0;
				} else {
					errno = convert_error(result);
					rval = -1;
				}
			}
		}

		forgetit:

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(rval==0) {
			return new pfs_file_nest(name);
		} else {
			return 0;
		}
	}

	pfs_dir * getdir( pfs_name *name ) {
		NestReplyStatus result;
		pfs_dir *dir=0;
		int invalid;

		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return 0;

		debug(D_NEST,"ChangeDir %s",name->rest);
		result = NestChangeDir( name->rest, fd );
		debug(D_NEST,"= %s",NestErrorString(result));
		if(result!=NEST_SUCCESS) goto failure;

		dir = new pfs_dir(name);

		debug(D_NEST,"ListFiles");
		result = NestListFiles( fd, list_buffer_append, dir );
		debug(D_NEST,"= %s",NestErrorString(result));
		if(result!=NEST_SUCCESS) goto failure;

		invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);
		return dir;

		failure:
		invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);
		if(dir) delete dir;
		errno = convert_error(result);
		return 0;
	}

	int chdir( pfs_name *name, char *newname ) {
		NestReplyStatus result;
		char *thedir;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;

		debug(D_NEST,"ChangeDir %s",name->rest);
		result = NestChangeDir( name->rest, fd );
		debug(D_NEST,"= %s",NestErrorString(result));

		if(result==NEST_SUCCESS) {
			debug(D_NEST,"GetPwd");
			result = NestGetPwd( &thedir, fd );
			debug(D_NEST,"= %s %s",thedir,NestErrorString(result));
			if(result==NEST_SUCCESS) {
				char *fmt;
				chomp_slashes(thedir);
				if(thedir[0]=='/') {
					fmt = "/%s/%s%s";
				} else {
					fmt = "/%s/%s/%s";
				}
				sprintf(newname,fmt,name->service_name,name->host,thedir);
				free(thedir);
			}
		}

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result!=NEST_SUCCESS) {
			errno = convert_error(result);
			return -1;
		} else {
			return 0;
		}
	}

	int stat( pfs_name *name, struct pfs_stat *buf ) {
		NestReplyStatus result;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;
		long size;

		debug(D_NEST,"ChangeDir %s",name->rest);
		result = NestChangeDir(name->rest,fd);
		debug(D_NEST,"= %s",NestErrorString(result));

		if(result==NEST_SUCCESS) {
			pfs_service_emulate_stat(name,buf);
			buf->st_size = 0;
			buf->st_mode &= ~(S_IFREG);
			buf->st_mode |= S_IFDIR;
		} else {
			debug(D_NEST,"Filesize %s",name->rest);
			result = NestFilesize(size,name->rest,fd);
			debug(D_NEST,"= %d %s",(result==NEST_SUCCESS) ? size : 0,NestErrorString(result));
			if(result==NEST_SUCCESS) {
				pfs_service_emulate_stat(name,buf);
				buf->st_size = size;
			}
		}

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result==NEST_SUCCESS) {
			return 0;
		} else {
			errno = convert_error(result);
			return -1;
		}
	}

	int lstat( pfs_name *name, struct pfs_stat *buf ) {
		return this->stat(name,buf);
	}

	int access( pfs_name *name, mode_t mode ) {
		struct pfs_stat buf;
		return this->stat(name,&buf);
	}

	int rename( pfs_name *name, pfs_name *newname ) {
		NestReplyStatus result;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;

		this->unlink(newname);

		debug(D_NEST,"RenameFile %s %s",name->rest,newname->rest);
		result = NestRenameFile(name->rest,newname->rest,fd);
		debug(D_NEST,"= %s",NestErrorString(result));

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result!=NEST_SUCCESS) {
			errno = convert_error(result);
			return -1;
		} else {
			return 0;
		}
	}

	int symlink( const char *linkname, pfs_name *newname ) {
		NestReplyStatus result;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;

		debug(D_NEST,"Link %s %s 1",linkname,newname->rest);
		result = NestLink(linkname,newname->rest,1,fd);
		debug(D_NEST,"= %s",NestErrorString(result));

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result!=NEST_SUCCESS) {
			errno = convert_error(result);
			return -1;
		} else {
			return 0;
		}
	}

	int link( pfs_name *name, pfs_name *newname ) {
		NestReplyStatus result;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;

		debug(D_NEST,"Link %s %s 0",name->rest,newname->rest);
		result = NestLink(name->rest,newname->rest,0,fd);
		debug(D_NEST,"= %s",NestErrorString(result));

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result!=NEST_SUCCESS) {
			errno = convert_error(result);
			return -1;
		} else {
			return 0;
		}
	}

	int unlink( pfs_name *name ) {
		NestReplyStatus result;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;

		debug(D_NEST,"RemoveFile %s",name->rest);
		result = NestRemoveFile(name->rest,fd);
		debug(D_NEST,"= %s",NestErrorString(result));

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result!=NEST_SUCCESS) {
			errno = convert_error(result);
			return -1;
		} else {
			return 0;
		}
	}

	int mkdir( pfs_name *name, mode_t mode ) {
		NestReplyStatus result;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;

		debug(D_NEST,"MkDir %s",name->rest);
		result = NestMkDir(name->rest,fd);
		debug(D_NEST,"= %s",NestErrorString(result));

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result!=NEST_SUCCESS) {
			errno = convert_error(result);
			return -1;
		} else {
			return 0;
		}
	}

	int rmdir( pfs_name *name ) {
		NestReplyStatus result;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;

		debug(D_NEST,"RmDir %s",name->rest);
		result = NestRmDir(name->rest,fd);
		debug(D_NEST,"= %s",NestErrorString(result));

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result!=NEST_SUCCESS) {
			errno = convert_error(result);
			return -1;
		} else {
			return 0;
		}
	}

	int statfs( pfs_name *name, struct pfs_statfs *buf ) {
		float avail, total;
		NestReplyStatus result;
		NestConnection fd = (int) pfs_service_connect_cache(name);
		if(!fd) return -1;

		debug(D_NEST,"AvailableSpace");
		result = NestAvailableSpace(avail,total,fd);
		debug(D_NEST,"= %s",NestErrorString(result));

		int invalid = (result==NEST_NO_CONNECTION);
		pfs_service_disconnect_cache(name,(void*)fd,invalid);

		if(result!=NEST_SUCCESS) {
			errno = convert_error(result);
			return -1;
		} else {
			memset(buf,0,sizeof(*buf));
			buf->f_bsize = pfs_service_get_block_size();
			buf->f_blocks = (__fsblkcnt_t)(total/buf->f_bsize);
			buf->f_bfree = (__fsblkcnt_t)(avail/buf->f_bsize);
			buf->f_bavail = (__fsblkcnt_t)(avail/buf->f_bsize);
			return 0;
		}
	}

	int truncate( pfs_name *name, pfs_off_t length ) {
		return 0;
	}

	virtual int is_seekable() {
		return 1;
	}

	virtual int tilde_is_special() {
		return 1;
	}
};

static pfs_service_nest pfs_service_nest_instance;
pfs_service *pfs_service_nest = &pfs_service_nest_instance;

#endif

/* vim: set noexpandtab tabstop=8: */
